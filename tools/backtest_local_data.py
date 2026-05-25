#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Replay local OKX trades/books through GlacierPulse's research-only A1 flow.

This is not a signal-trading backtester. It mirrors the current main.py research
runtime semantics:

- feed trades into MarketContext.apply_trade() and A1AbsorptionEngine.on_trade()
- feed books into MarketContext.apply_book_delta() and A1AbsorptionEngine.on_book_update()
- handle returned A1 research events like main.py does
- never instantiate IcebergTrader
- never simulate open/close orders
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import sys
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logger = None

TEXT_SUFFIXES = {".csv", ".jsonl", ".ndjson", ".json"}
CONTRACT_MULTIPLIERS = {"ETH-USDT-SWAP": 0.1}


@dataclass(order=True)
class ReplayEvent:
    ts: float
    seq: int
    kind: str = field(compare=False)
    payload: dict[str, Any] = field(compare=False)


@dataclass
class Stats:
    trades: int = 0
    books: int = 0
    research_events: int = 0
    a1_iceberg_events: int = 0
    spoofing_withdrawal_events: int = 0
    ignored_engine_returns: int = 0
    malformed_rows: int = 0
    first_ts: float = 0.0
    last_ts: float = 0.0

    def touch(self, ts: float) -> None:
        if ts <= 0:
            return
        self.first_ts = ts if self.first_ts <= 0 else min(self.first_ts, ts)
        self.last_ts = max(self.last_ts, ts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "trades": self.trades,
            "books": self.books,
            "research_events": self.research_events,
            "a1_iceberg_events": self.a1_iceberg_events,
            "spoofing_withdrawal_events": self.spoofing_withdrawal_events,
            "ignored_engine_returns": self.ignored_engine_returns,
            "malformed_rows": self.malformed_rows,
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
            "first_time_utc": fmt_utc(self.first_ts),
            "last_time_utc": fmt_utc(self.last_ts),
            "duration_sec": round(self.last_ts - self.first_ts, 6) if self.first_ts and self.last_ts else 0.0,
        }


class LocalA1ResearchRuntime:
    """Offline equivalent of main.py's current research-only callback pipeline."""

    def __init__(self, symbol: str, research_events_path: Path):
        from config import research_evaluator as research_config
        from src.context.market_context import MarketContext
        from src.detectors.iceberg_detector import IcebergDetector
        from src.strategy.a1_absorption.engine import A1AbsorptionEngine

        self.symbol = symbol
        self.research_config = research_config
        self.ctx = MarketContext()
        self.engine = A1AbsorptionEngine(market_context=self.ctx, iceberg_detector=IcebergDetector())
        self.research_events_path = research_events_path
        self.research_events_file = research_events_path.open("w", encoding="utf-8")

    def close(self) -> None:
        self.research_events_file.close()

    def on_trade_tick(self, trade_data: dict[str, Any], stats: Stats) -> None:
        """Mirror main.py on_trade_tick(), excluding trader breakeven checks."""
        current_price = float(trade_data["price"])
        self.ctx.apply_trade(trade_data)
        if hasattr(self.engine, "on_trade"):
            event = self.engine.on_trade(trade_data)
        else:
            event = self.engine.process_tick(trade_data)
        self.handle_research_event(event, current_price=current_price, source="trade", stats=stats)

    def on_book_update(self, book_data: dict[str, Any], stats: Stats) -> None:
        """Mirror main.py on_book_update(), excluding asyncio task scheduling."""
        self.ctx.apply_book_delta(book_data)
        if not hasattr(self.engine, "on_book_update"):
            return
        event = self.engine.on_book_update(book_data)
        current_price = float(getattr(self.ctx, "current_price", 0.0) or 0.0)
        self.handle_research_event(event, current_price=current_price, source="book", stats=stats)

    def handle_research_event(self, event: dict[str, Any] | None, current_price: float, source: str, stats: Stats) -> None:
        """Research-only equivalent of main.py handle_signal().

        The current live main.py returns immediately for ICEBERG_ABSORPTION events
        and keeps A1 single-event trading disabled. This method keeps the same
        semantics: record and log research events, but never dispatch execution.
        """
        if not event:
            return

        if event.get("event_type") == "ICEBERG_ABSORPTION" and bool(event.get("is_iceberg", False)):
            stats.research_events += 1
            stats.a1_iceberg_events += 1
            self._write_research_event(event, current_price=current_price, source=source)
            if bool(getattr(self.research_config, "V62_LOG_A1_ICEBERG_EVENT_ENABLED", True)):
                logger.info(
                    "[A1-ICEBERG-EVENT] id=%s direction=%s quality=%s price=%.2f zone=[%.2f, %.2f] hidden=%.0fU absorption=%.1f%% active=%.0fU conf=%.2f wait=%.1fms trades=%s source=%s",
                    event.get("event_id"),
                    event.get("direction", "BUY"),
                    event.get("phase1_quality", "LOW"),
                    float(event.get("trigger_price", current_price or 0.0)),
                    float(event.get("zone_lower", 0.0)),
                    float(event.get("zone_upper", 0.0)),
                    float(event.get("hidden_volume", 0.0)),
                    float(event.get("absorption_rate", 0.0)) * 100.0,
                    float(event.get("active_volume", 0.0)),
                    float(event.get("confidence", 0.0)),
                    float(event.get("wait_ms", 0.0)),
                    event.get("trade_count", 0),
                    source,
                )
            return

        if event.get("behavior") == "SPOOFING_WITHDRAWAL":
            stats.research_events += 1
            stats.spoofing_withdrawal_events += 1
            self._write_research_event(event, current_price=current_price, source=source)
            logger.warning(
                "[SPOOFING-WITHDRAWAL] source=%s hidden=%.0fU",
                source,
                abs(float(event.get("hidden_volume", 0.0))),
            )
            return

        # Keep visibility for unexpected returns, but do not treat them as trades.
        stats.ignored_engine_returns += 1
        self._write_research_event(event, current_price=current_price, source=source, ignored=True)

    def _write_research_event(
        self,
        event: Mapping[str, Any],
        current_price: float,
        source: str,
        ignored: bool = False,
    ) -> None:
        row = dict(event)
        row["replay_source"] = source
        row["replay_current_price"] = current_price
        row["replay_ignored"] = bool(ignored)
        row["runtime_mode"] = "local_research_replay"
        row["execution_enabled"] = False
        self.research_events_file.write(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str) + "\n")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay local OKX historical data through the A1 research-only engine path.")
    p.add_argument("--symbol", default="ETH-USDT-SWAP")
    p.add_argument("--trades-dir", type=Path)
    p.add_argument("--books-dir", type=Path)
    p.add_argument("--trades-file", type=Path)
    p.add_argument("--books-file", type=Path)
    p.add_argument("--run-name", default="local_a1_research_replay")
    p.add_argument("--out-dir", type=Path)
    p.add_argument("--contract-multiplier", type=float)
    p.add_argument("--sort-in-memory", action="store_true")
    p.add_argument("--allow-missing-books", action="store_true")
    p.add_argument("--max-events", type=int, default=0)
    p.add_argument("--progress-every", type=int, default=100_000)
    p.add_argument(
        "--use-shared-research-logs",
        action="store_true",
        help="Use config/default logs/research paths instead of isolating JSONL outputs under this run directory.",
    )
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    trades_path = args.trades_file or args.trades_dir
    books_path = args.books_file or args.books_dir
    if not trades_path:
        raise SystemExit("Missing --trades-dir or --trades-file")
    if not books_path and not args.allow_missing_books:
        raise SystemExit("Missing --books-dir/--books-file. Use --allow-missing-books only for parser smoke tests.")

    out_dir = args.out_dir or (ROOT / "reports" / "backtests" / args.run_name)
    out_dir.mkdir(parents=True, exist_ok=True)
    research_dir = out_dir / "research"
    research_dir.mkdir(parents=True, exist_ok=True)

    configure_runtime_environment(args=args, out_dir=out_dir, research_dir=research_dir)

    from src.config.runtime_profile_loader import load_runtime_profile
    from src.utils.log import get_logger

    load_runtime_profile()
    global logger
    logger = get_logger("BacktestLocalData")

    multiplier = args.contract_multiplier or CONTRACT_MULTIPLIERS.get(args.symbol, 1.0)
    research_events_path = out_dir / "research_events.jsonl"
    summary_path = out_dir / "summary.json"

    logger.info("[LOCAL-A1-RESEARCH-START] symbol=%s trades=%s books=%s out=%s", args.symbol, trades_path, books_path, out_dir)
    logger.info("[LOCAL-A1-RESEARCH-MODE] no_main_py=true no_websocket=true no_iceberg_trader=true no_execution=true")

    runtime = LocalA1ResearchRuntime(symbol=str(args.symbol), research_events_path=research_events_path)
    stats = Stats()
    start = time.perf_counter()
    try:
        events = build_events(
            Path(trades_path),
            Path(books_path) if books_path else None,
            str(args.symbol),
            float(multiplier),
            stats,
            bool(args.sort_in_memory),
        )
        for idx, event in enumerate(events, start=1):
            if args.max_events and idx > args.max_events:
                break
            stats.touch(event.ts)
            if event.kind == "trade":
                runtime.on_trade_tick(event.payload, stats)
            elif event.kind == "book":
                runtime.on_book_update(event.payload, stats)
            if args.progress_every and idx % args.progress_every == 0:
                logger.info(
                    "[LOCAL-A1-RESEARCH-PROGRESS] events=%d trades=%d books=%d research_events=%d a1_icebergs=%d last=%s",
                    idx,
                    stats.trades,
                    stats.books,
                    stats.research_events,
                    stats.a1_iceberg_events,
                    fmt_utc(stats.last_ts),
                )
    finally:
        runtime.close()

    elapsed = time.perf_counter() - start
    summary = {
        "run_name": args.run_name,
        "symbol": args.symbol,
        "contract_multiplier": multiplier,
        "trades_path": str(trades_path),
        "books_path": str(books_path) if books_path else "",
        "research_events_path": str(research_events_path),
        "research_jsonl_paths": {
            "phase1_candidates": os.getenv("PHASE1_CANDIDATE_RECORDER_JSONL_PATH", "logs/research/phase1_candidates.jsonl"),
            "a1_reaction_events": os.getenv("A1_REACTION_EVENT_RECORDER_JSONL_PATH", "logs/research/a1_reaction_events.jsonl"),
        },
        "elapsed_sec": round(elapsed, 6),
        "events_per_sec": round((stats.trades + stats.books) / elapsed, 3) if elapsed > 0 else 0.0,
        "stats": stats.to_dict(),
        "execution": {
            "mode": "research_only",
            "starts_main_py": False,
            "uses_websocket": False,
            "instantiates_iceberg_trader": False,
            "opens_or_closes_positions": False,
            "simulates_pnl": False,
        },
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[LOCAL-A1-RESEARCH-DONE] summary=%s research_events=%s", summary_path, research_events_path)
    return 0


def configure_runtime_environment(args: argparse.Namespace, out_dir: Path, research_dir: Path) -> None:
    # Safety first: local replay must never enable real execution.
    os.environ.setdefault("REAL_EXECUTION_ENABLED", "false")
    os.environ.setdefault("PHASE3_REAL_TRADING_ENABLED", "false")
    os.environ.setdefault("VIRTUAL_SHADOW_MODE", "false")
    os.environ.setdefault("A1_REACTION_TO_VIRTUAL_POSITION_ENABLED", "false")

    if args.use_shared_research_logs:
        return

    # Isolate research JSONL files per local run. This keeps offline runs from
    # mixing with live logs/research outputs.
    os.environ["PHASE1_CANDIDATE_RECORDER_WRITE_JSONL"] = "true"
    os.environ["PHASE1_CANDIDATE_RECORDER_JSONL_PATH"] = str(research_dir / "phase1_candidates.jsonl")
    os.environ["A1_REACTION_EVENT_RECORDER_WRITE_JSONL"] = "true"
    os.environ["A1_REACTION_EVENT_RECORDER_JSONL_PATH"] = str(research_dir / "a1_reaction_events.jsonl")
    os.environ["A1_DYNAMIC_PARAM_PREVIEW_JSON_PATH"] = str(out_dir / "runtime_state" / "a1_dynamic_params.json")


def build_events(trades_path: Path, books_path: Path | None, symbol: str, multiplier: float, stats: Stats, sort_in_memory: bool) -> Iterator[ReplayEvent]:
    trades = iter_trade_events(trades_path, symbol, multiplier, stats)
    books = iter_book_events(books_path, symbol, multiplier, stats) if books_path else iter(())
    if sort_in_memory:
        all_events = list(trades) + list(books)
        all_events.sort()
        yield from all_events
    else:
        yield from merge_sorted(trades, books)


def iter_trade_events(path: Path, symbol: str, multiplier: float, stats: Stats) -> Iterator[ReplayEvent]:
    seq = 0
    for row in iter_rows(path):
        try:
            trade = normalize_trade(row, symbol, multiplier)
            if not trade:
                continue
            seq += 1
            stats.trades += 1
            yield ReplayEvent(float(trade["ts"]), seq, "trade", trade)
        except Exception:
            stats.malformed_rows += 1


def iter_book_events(path: Path, symbol: str, multiplier: float, stats: Stats) -> Iterator[ReplayEvent]:
    seq = 0
    for row in iter_rows(path):
        try:
            book = normalize_book(row, symbol, multiplier)
            if not book:
                continue
            seq += 1
            stats.books += 1
            yield ReplayEvent(float(book["ts"]), 1_000_000_000 + seq, "book", book)
        except Exception:
            stats.malformed_rows += 1


def merge_sorted(a_iter: Iterator[ReplayEvent], b_iter: Iterator[ReplayEvent]) -> Iterator[ReplayEvent]:
    a = next(a_iter, None)
    b = next(b_iter, None)
    while a is not None or b is not None:
        if b is None or (a is not None and a <= b):
            yield a
            a = next(a_iter, None)
        else:
            yield b
            b = next(b_iter, None)


def iter_rows(path: Path) -> Iterator[dict[str, Any]]:
    if path.is_dir():
        for child in sorted(p for p in path.rglob("*") if p.is_file()):
            if child.suffix.lower() in TEXT_SUFFIXES or child.suffix.lower() in {".gz", ".zip"}:
                yield from iter_rows(child)
        return
    suffix = path.suffix.lower()
    if suffix == ".zip":
        with zipfile.ZipFile(path) as zf:
            for name in sorted(zf.namelist()):
                if Path(name).suffix.lower() not in TEXT_SUFFIXES:
                    continue
                with zf.open(name) as raw:
                    yield from iter_text_rows(io.TextIOWrapper(raw, encoding="utf-8", errors="replace"), Path(name).suffix.lower())
        return
    if suffix == ".gz":
        inner = path.with_suffix("").suffix.lower() or ".csv"
        with gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="") as f:
            yield from iter_text_rows(f, inner)
        return
    if suffix in TEXT_SUFFIXES:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            yield from iter_text_rows(f, suffix)


def iter_text_rows(f: io.TextIOBase, suffix: str) -> Iterator[dict[str, Any]]:
    if suffix == ".csv":
        sample = f.read(4096)
        f.seek(0)
        has_header = csv.Sniffer().has_header(sample) if sample.strip() else False
        if has_header:
            yield from csv.DictReader(f)
        else:
            for row in csv.reader(f):
                yield {str(i): value for i, value in enumerate(row)}
        return
    for line in f:
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        rows = obj if isinstance(obj, list) else obj.get("data") if isinstance(obj, dict) and isinstance(obj.get("data"), list) else [obj]
        for row in rows:
            if isinstance(row, dict):
                yield row


def normalize_trade(row: Mapping[str, Any], symbol: str, multiplier: float) -> dict[str, Any] | None:
    inst = get_any(row, "instId", "inst_id", "symbol", "instrument")
    if inst and str(inst) != symbol:
        return None
    price = to_float(get_any(row, "px", "price", "trade_price", "0"))
    raw_size = to_float(get_any(row, "sz", "size", "qty", "amount", "1"))
    side = str(get_any(row, "side", "2") or "").lower()
    ts = normalize_ts(get_any(row, "ts", "timestamp", "time", "created_time", "3"))
    if price <= 0 or raw_size <= 0 or side not in {"buy", "sell"} or ts <= 0:
        return None
    return {"price": price, "size": raw_size * multiplier, "side": side, "ts": ts, "recv_ts": ts, "raw_size": raw_size}


def normalize_book(row: Mapping[str, Any], symbol: str, multiplier: float) -> dict[str, Any] | None:
    inst = get_any(row, "instId", "inst_id", "symbol", "instrument")
    if inst and str(inst) != symbol:
        return None
    ts = normalize_ts(get_any(row, "ts", "timestamp", "time", "created_time", "0"))
    bids = parse_levels(get_any(row, "bids", "bid", "bid_levels", "1"), multiplier)
    asks = parse_levels(get_any(row, "asks", "ask", "ask_levels", "2"), multiplier)
    if ts <= 0 or (not bids and not asks):
        return None
    return {"bids": bids, "asks": asks, "ts": ts, "recv_ts": ts}


def parse_levels(value: Any, multiplier: float) -> list[list[float]]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return []
    levels = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, Mapping):
                price, qty = to_float(get_any(item, "px", "price", "p")), to_float(get_any(item, "sz", "size", "qty", "q"))
            elif isinstance(item, Sequence) and not isinstance(item, (str, bytes)) and len(item) >= 2:
                price, qty = to_float(item[0]), to_float(item[1])
            else:
                continue
            if price > 0:
                levels.append([price, qty * multiplier])
    return levels


def get_any(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in row:
            return row[name]
        for key, value in row.items():
            if str(key).lower() == name.lower():
                return value
    return None


def to_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def normalize_ts(value: Any) -> float:
    raw = to_float(value)
    if raw > 10_000_000_000_000_000:
        return raw / 1_000_000_000.0
    if raw > 10_000_000_000_000:
        return raw / 1_000_000.0
    if raw > 10_000_000_000:
        return raw / 1_000.0
    return raw


def fmt_utc(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(float(ts))) if ts and ts > 0 else ""


if __name__ == "__main__":
    raise SystemExit(main())
