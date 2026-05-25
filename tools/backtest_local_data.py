#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Replay local OKX trades/books through GlacierPulse without starting main.py."""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import sys
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config.runtime_profile_loader import load_runtime_profile

load_runtime_profile()

from src.context.market_context import MarketContext
from src.detectors.iceberg_detector import IcebergDetector
from src.strategy.a1_absorption.engine import A1AbsorptionEngine
from src.utils.log import get_logger

logger = get_logger("BacktestLocalData")

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
    signals: int = 0
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
            "signals": self.signals,
            "malformed_rows": self.malformed_rows,
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
            "first_time_utc": fmt_utc(self.first_ts),
            "last_time_utc": fmt_utc(self.last_ts),
            "duration_sec": round(self.last_ts - self.first_ts, 6) if self.first_ts and self.last_ts else 0.0,
        }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay local OKX historical data through the A1 research engine.")
    p.add_argument("--symbol", default="ETH-USDT-SWAP")
    p.add_argument("--trades-dir", type=Path)
    p.add_argument("--books-dir", type=Path)
    p.add_argument("--trades-file", type=Path)
    p.add_argument("--books-file", type=Path)
    p.add_argument("--run-name", default="local_data_backtest")
    p.add_argument("--out-dir", type=Path)
    p.add_argument("--contract-multiplier", type=float)
    p.add_argument("--sort-in-memory", action="store_true")
    p.add_argument("--allow-missing-books", action="store_true")
    p.add_argument("--max-events", type=int, default=0)
    p.add_argument("--progress-every", type=int, default=100_000)
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    trades_path = args.trades_file or args.trades_dir
    books_path = args.books_file or args.books_dir
    if not trades_path:
        raise SystemExit("Missing --trades-dir or --trades-file")
    if not books_path and not args.allow_missing_books:
        raise SystemExit("Missing --books-dir/--books-file. Use --allow-missing-books only for smoke tests.")

    multiplier = args.contract_multiplier or CONTRACT_MULTIPLIERS.get(args.symbol, 1.0)
    out_dir = args.out_dir or (ROOT / "reports" / "backtests" / args.run_name)
    out_dir.mkdir(parents=True, exist_ok=True)
    signals_path = out_dir / "signals.jsonl"
    summary_path = out_dir / "summary.json"

    logger.info("[LOCAL-BACKTEST-START] symbol=%s trades=%s books=%s out=%s", args.symbol, trades_path, books_path, out_dir)
    ctx = MarketContext()
    engine = A1AbsorptionEngine(market_context=ctx, iceberg_detector=IcebergDetector())
    stats = Stats()
    start = time.perf_counter()

    events = build_events(Path(trades_path), Path(books_path) if books_path else None, args.symbol, multiplier, stats, args.sort_in_memory)
    with signals_path.open("w", encoding="utf-8") as f:
        for idx, event in enumerate(events, start=1):
            if args.max_events and idx > args.max_events:
                break
            stats.touch(event.ts)
            signal = replay_one(ctx, engine, event)
            if signal:
                stats.signals += 1
                row = dict(signal)
                row["replay_event_kind"] = event.kind
                row["replay_event_ts"] = event.ts
                row["replay_event_time_utc"] = fmt_utc(event.ts)
                f.write(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str) + "\n")
            if args.progress_every and idx % args.progress_every == 0:
                logger.info("[LOCAL-BACKTEST-PROGRESS] events=%d trades=%d books=%d signals=%d last=%s", idx, stats.trades, stats.books, stats.signals, fmt_utc(stats.last_ts))

    elapsed = time.perf_counter() - start
    summary = {
        "run_name": args.run_name,
        "symbol": args.symbol,
        "contract_multiplier": multiplier,
        "trades_path": str(trades_path),
        "books_path": str(books_path) if books_path else "",
        "signals_path": str(signals_path),
        "elapsed_sec": round(elapsed, 6),
        "events_per_sec": round((stats.trades + stats.books) / elapsed, 3) if elapsed > 0 else 0.0,
        "stats": stats.to_dict(),
        "execution": "research-only; no main.py, no websocket, no IcebergTrader",
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("[LOCAL-BACKTEST-DONE] summary=%s signals=%s", summary_path, signals_path)
    return 0


def build_events(trades_path: Path, books_path: Path | None, symbol: str, multiplier: float, stats: Stats, sort_in_memory: bool) -> Iterator[ReplayEvent]:
    trades = iter_trade_events(trades_path, symbol, multiplier, stats)
    books = iter_book_events(books_path, symbol, multiplier, stats) if books_path else iter(())
    if sort_in_memory:
        all_events = list(trades) + list(books)
        all_events.sort()
        yield from all_events
    else:
        yield from merge_sorted(trades, books)


def replay_one(ctx: MarketContext, engine: A1AbsorptionEngine, event: ReplayEvent) -> dict[str, Any] | None:
    if event.kind == "trade":
        ctx.apply_trade(event.payload)
        return engine.on_trade(event.payload) if hasattr(engine, "on_trade") else engine.process_tick(event.payload)
    if event.kind == "book":
        ctx.apply_book_delta(event.payload)
        return engine.on_book_update(event.payload) if hasattr(engine, "on_book_update") else None
    return None


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
