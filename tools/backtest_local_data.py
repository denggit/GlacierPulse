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

The local book replay path includes a cleaning layer to make historical files
behave closer to the live OKX `books` channel used by main.py:

- default depth limit: 400 levels per side
- default book event cadence: 100ms buckets
- historical snapshots can be converted into deltas before entering MarketContext
- contract quantities are converted into ETH amounts using the same multiplier
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import re
import sys
import tarfile
import time
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logger = None

TEXT_SUFFIXES = {".csv", ".jsonl", ".ndjson", ".json"}
JSON_LINE_SUFFIXES = {".jsonl", ".ndjson"}
JSON_SUFFIXES = {".json", ".jsonl", ".ndjson"}
CONTRACT_MULTIPLIERS = {"ETH-USDT-SWAP": 0.1}


@dataclass(order=True)
class ReplayEvent:
    ts: float
    seq: int
    kind: str = field(compare=False)
    payload: dict[str, Any] = field(compare=False)


@dataclass
class BookCleaningOptions:
    event_mode: str = "auto"
    depth_limit: int = 400
    bucket_ms: float = 100.0
    snapshot_infer_min_levels: int = 100


@dataclass
class TimeFilter:
    start_ts: float | None = None
    end_ts: float | None = None

    def includes(self, ts: float) -> bool:
        if self.start_ts is not None and ts < self.start_ts:
            return False
        if self.end_ts is not None and ts >= self.end_ts:
            return False
        return True

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "start_time_utc": fmt_utc(self.start_ts or 0.0),
            "end_time_utc": fmt_utc(self.end_ts or 0.0),
            "end_is_exclusive": True,
        }


@dataclass
class Stats:
    trades: int = 0
    books: int = 0
    raw_book_rows: int = 0
    filtered_trades: int = 0
    filtered_books: int = 0
    filtered_raw_book_rows: int = 0
    book_bucket_coalesces: int = 0
    book_snapshot_rebuilds: int = 0
    book_zero_delete_levels: int = 0
    research_events: int = 0
    a1_iceberg_events: int = 0
    spoofing_withdrawal_events: int = 0
    ignored_engine_returns: int = 0
    malformed_rows: int = 0
    parsed_files: int = 0
    skipped_files: int = 0
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
            "raw_book_rows": self.raw_book_rows,
            "filtered_trades": self.filtered_trades,
            "filtered_books": self.filtered_books,
            "filtered_raw_book_rows": self.filtered_raw_book_rows,
            "book_bucket_coalesces": self.book_bucket_coalesces,
            "book_snapshot_rebuilds": self.book_snapshot_rebuilds,
            "book_zero_delete_levels": self.book_zero_delete_levels,
            "research_events": self.research_events,
            "a1_iceberg_events": self.a1_iceberg_events,
            "spoofing_withdrawal_events": self.spoofing_withdrawal_events,
            "ignored_engine_returns": self.ignored_engine_returns,
            "malformed_rows": self.malformed_rows,
            "parsed_files": self.parsed_files,
            "skipped_files": self.skipped_files,
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
        """Research-only equivalent of main.py handle_signal()."""
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


class BookEventCleaner:
    """Normalize historical book rows to live-like OKX `books` update events."""

    def __init__(self, options: BookCleaningOptions, stats: Stats):
        self.options = options
        self.stats = stats
        self.pending_bucket: int | None = None
        self.pending_book: dict[str, Any] | None = None
        self.prev_snapshot_bids: dict[float, float] = {}
        self.prev_snapshot_asks: dict[float, float] = {}

    def push(self, raw_book: dict[str, Any]) -> list[dict[str, Any]]:
        if self.options.bucket_ms <= 0:
            cleaned = self._clean_for_replay(raw_book)
            return [cleaned] if cleaned else []

        bucket = int(float(raw_book["ts"]) * 1000.0 // float(self.options.bucket_ms))
        if self.pending_book is None:
            self.pending_bucket = bucket
            self.pending_book = raw_book
            return []

        if bucket == self.pending_bucket:
            self.pending_book = self._coalesce_raw_books(self.pending_book, raw_book)
            self.stats.book_bucket_coalesces += 1
            return []

        cleaned = self._clean_for_replay(self.pending_book)
        self.pending_bucket = bucket
        self.pending_book = raw_book
        return [cleaned] if cleaned else []

    def flush(self) -> list[dict[str, Any]]:
        if self.pending_book is None:
            return []
        cleaned = self._clean_for_replay(self.pending_book)
        self.pending_book = None
        self.pending_bucket = None
        return [cleaned] if cleaned else []

    def _coalesce_raw_books(self, base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        base_mode = base.get("_mode", "delta")
        incoming_mode = incoming.get("_mode", "delta")
        if incoming_mode == "snapshot":
            return incoming
        if base_mode == "snapshot":
            return self._apply_delta_to_snapshot(base, incoming)

        bids = merge_level_updates(base.get("bids", []), incoming.get("bids", []), side="bids", depth_limit=0)
        asks = merge_level_updates(base.get("asks", []), incoming.get("asks", []), side="asks", depth_limit=0)
        inferred_mode = "snapshot" if len(bids) + len(asks) >= self.options.snapshot_infer_min_levels else "delta"
        return {
            "bids": bids,
            "asks": asks,
            "ts": incoming["ts"],
            "recv_ts": incoming.get("recv_ts", incoming["ts"]),
            "_mode": inferred_mode,
        }

    def _apply_delta_to_snapshot(self, snapshot: dict[str, Any], delta: dict[str, Any]) -> dict[str, Any]:
        bids = {float(price): float(size) for price, size in snapshot.get("bids", []) if float(size) > 0}
        asks = {float(price): float(size) for price, size in snapshot.get("asks", []) if float(size) > 0}
        apply_level_updates_to_state(bids, delta.get("bids", []))
        apply_level_updates_to_state(asks, delta.get("asks", []))
        return {
            "bids": sort_levels_from_state(bids, side="bids", depth_limit=self.options.depth_limit),
            "asks": sort_levels_from_state(asks, side="asks", depth_limit=self.options.depth_limit),
            "ts": delta["ts"],
            "recv_ts": delta.get("recv_ts", delta["ts"]),
            "_mode": "snapshot",
        }

    def _clean_for_replay(self, raw_book: dict[str, Any]) -> dict[str, Any] | None:
        mode = raw_book.get("_mode", "delta")
        if mode == "snapshot":
            self.stats.book_snapshot_rebuilds += 1
            return self._snapshot_to_live_delta(raw_book)

        bids = normalize_levels_for_replay(raw_book.get("bids", []), side="bids", depth_limit=0)
        asks = normalize_levels_for_replay(raw_book.get("asks", []), side="asks", depth_limit=0)
        if not bids and not asks:
            return None
        return {"bids": bids, "asks": asks, "ts": raw_book["ts"], "recv_ts": raw_book.get("recv_ts", raw_book["ts"])}

    def _snapshot_to_live_delta(self, snapshot: dict[str, Any]) -> dict[str, Any] | None:
        new_bids = levels_to_state(snapshot.get("bids", []), side="bids", depth_limit=self.options.depth_limit)
        new_asks = levels_to_state(snapshot.get("asks", []), side="asks", depth_limit=self.options.depth_limit)

        bid_delta, bid_deletes = diff_states(self.prev_snapshot_bids, new_bids, side="bids")
        ask_delta, ask_deletes = diff_states(self.prev_snapshot_asks, new_asks, side="asks")
        self.stats.book_zero_delete_levels += bid_deletes + ask_deletes

        self.prev_snapshot_bids = new_bids
        self.prev_snapshot_asks = new_asks
        if not bid_delta and not ask_delta:
            return None
        return {"bids": bid_delta, "asks": ask_delta, "ts": snapshot["ts"], "recv_ts": snapshot.get("recv_ts", snapshot["ts"])}


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
    p.add_argument("--start-date", help="Replay lower bound as YYYY-MM-DD at 00:00:00 UTC.")
    p.add_argument("--end-date", help="Replay upper bound as inclusive YYYY-MM-DD; internally next day 00:00:00 UTC.")
    p.add_argument("--start-time", help="Replay lower bound as ISO8601 UTC timestamp. Overrides --start-date.")
    p.add_argument("--end-time", help="Replay upper bound as ISO8601 UTC timestamp. Overrides --end-date.")
    p.add_argument("--sort-in-memory", action="store_true")
    p.add_argument("--allow-missing-books", action="store_true")
    p.add_argument("--max-events", type=int, default=0)
    p.add_argument("--progress-every", type=int, default=100_000)
    p.add_argument("--books-event-mode", choices=["auto", "delta", "snapshot"], default="auto", help="How historical book rows should be interpreted before replay. Default: auto.")
    p.add_argument("--books-depth-limit", type=int, default=400, help="Depth limit per side for snapshot-style historical books. Default: 400.")
    p.add_argument("--books-bucket-ms", type=float, default=100.0, help="Coalesce book updates into this many milliseconds to mimic OKX live books cadence. Use 0 to disable. Default: 100.")
    p.add_argument("--books-snapshot-infer-min-levels", type=int, default=100, help="In auto mode, treat a coalesced book event as snapshot once it contains at least this many levels. Default: 100.")
    p.add_argument(
        "--use-shared-research-logs",
        action="store_true",
        help="Use config/default logs/research paths instead of isolating JSONL outputs under this run directory.",
    )
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    time_filter = parse_time_filter(args)
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
    book_cleaning = BookCleaningOptions(
        event_mode=str(args.books_event_mode),
        depth_limit=max(1, int(args.books_depth_limit)),
        bucket_ms=float(args.books_bucket_ms),
        snapshot_infer_min_levels=max(1, int(args.books_snapshot_infer_min_levels)),
    )
    research_events_path = out_dir / "research_events.jsonl"
    summary_path = out_dir / "summary.json"

    logger.info("[LOCAL-A1-RESEARCH-START] symbol=%s trades=%s books=%s out=%s", args.symbol, trades_path, books_path, out_dir)
    logger.info("[LOCAL-A1-RESEARCH-MODE] no_main_py=true no_websocket=true no_iceberg_trader=true no_execution=true")
    logger.info("[LOCAL-BOOK-CLEANING] %s", asdict(book_cleaning))
    logger.info("[LOCAL-TIME-FILTER] %s", time_filter.to_dict())

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
            book_cleaning,
            time_filter,
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
                    "[LOCAL-A1-RESEARCH-PROGRESS] events=%d trades=%d books=%d raw_book_rows=%d research_events=%d a1_icebergs=%d last=%s",
                    idx,
                    stats.trades,
                    stats.books,
                    stats.raw_book_rows,
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
        "book_cleaning": asdict(book_cleaning),
        "time_filter": time_filter.to_dict(),
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
    os.environ.setdefault("REAL_EXECUTION_ENABLED", "false")
    os.environ.setdefault("PHASE3_REAL_TRADING_ENABLED", "false")
    os.environ.setdefault("VIRTUAL_SHADOW_MODE", "false")
    os.environ.setdefault("A1_REACTION_TO_VIRTUAL_POSITION_ENABLED", "false")

    if args.use_shared_research_logs:
        return

    os.environ["PHASE1_CANDIDATE_RECORDER_WRITE_JSONL"] = "true"
    os.environ["PHASE1_CANDIDATE_RECORDER_JSONL_PATH"] = str(research_dir / "phase1_candidates.jsonl")
    os.environ["A1_REACTION_EVENT_RECORDER_WRITE_JSONL"] = "true"
    os.environ["A1_REACTION_EVENT_RECORDER_JSONL_PATH"] = str(research_dir / "a1_reaction_events.jsonl")
    os.environ["A1_DYNAMIC_PARAM_PREVIEW_JSON_PATH"] = str(out_dir / "runtime_state" / "a1_dynamic_params.json")


def parse_time_filter(args: argparse.Namespace) -> TimeFilter:
    start_ts = parse_datetime_to_ts(args.start_time) if args.start_time else None
    end_ts = parse_datetime_to_ts(args.end_time) if args.end_time else None
    if start_ts is None and args.start_date:
        start_ts = date_to_start_ts(args.start_date)
    if end_ts is None and args.end_date:
        end_ts = date_to_next_day_start_ts(args.end_date)
    if start_ts is not None and end_ts is not None and start_ts >= end_ts:
        raise SystemExit(
            f"Invalid time range: start ({fmt_utc(start_ts)}) must be before exclusive end ({fmt_utc(end_ts)})"
        )
    return TimeFilter(start_ts=start_ts, end_ts=end_ts)


def parse_datetime_to_ts(text: str) -> float:
    value = str(text).strip()
    if not value:
        raise SystemExit("Invalid empty timestamp")
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"Invalid ISO8601 timestamp: {text}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return float(dt.timestamp())


def date_to_start_ts(date_text: str) -> float:
    return parse_date_utc(date_text).timestamp()


def date_to_next_day_start_ts(date_text: str) -> float:
    return (parse_date_utc(date_text) + timedelta(days=1)).timestamp()


def parse_date_utc(date_text: str) -> datetime:
    try:
        date_value = datetime.strptime(str(date_text).strip(), "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"Invalid date, expected YYYY-MM-DD: {date_text}") from exc
    return date_value.replace(tzinfo=timezone.utc)


def build_events(
    trades_path: Path,
    books_path: Path | None,
    symbol: str,
    multiplier: float,
    stats: Stats,
    sort_in_memory: bool,
    book_cleaning: BookCleaningOptions,
    time_filter: TimeFilter,
) -> Iterator[ReplayEvent]:
    trades = iter_trade_events(trades_path, symbol, multiplier, stats, time_filter)
    books = iter_book_events(books_path, symbol, multiplier, stats, book_cleaning, time_filter) if books_path else iter(())
    if sort_in_memory:
        all_events = list(trades) + list(books)
        all_events.sort()
        yield from all_events
    else:
        yield from merge_sorted(trades, books)


def iter_trade_events(path: Path, symbol: str, multiplier: float, stats: Stats, time_filter: TimeFilter) -> Iterator[ReplayEvent]:
    seq = 0
    for row in iter_rows(path, stats, time_filter):
        try:
            trade = normalize_trade(row, symbol, multiplier)
            if not trade:
                continue
            ts = float(trade["ts"])
            if not time_filter.includes(ts):
                stats.filtered_trades += 1
                continue
            seq += 1
            stats.trades += 1
            yield ReplayEvent(ts, seq, "trade", trade)
        except Exception:
            stats.malformed_rows += 1


def iter_book_events(
    path: Path,
    symbol: str,
    multiplier: float,
    stats: Stats,
    options: BookCleaningOptions,
    time_filter: TimeFilter,
) -> Iterator[ReplayEvent]:
    seq = 0
    cleaner = BookEventCleaner(options=options, stats=stats)
    for row in iter_rows(path, stats, time_filter):
        try:
            stats.raw_book_rows += 1
            raw_book = normalize_book(row, symbol, multiplier, options)
            if not raw_book:
                continue
            if not time_filter.includes(float(raw_book["ts"])):
                stats.filtered_books += 1
                stats.filtered_raw_book_rows += 1
                continue
            for book in cleaner.push(raw_book):
                seq += 1
                stats.books += 1
                yield ReplayEvent(float(book["ts"]), 1_000_000_000 + seq, "book", book)
        except Exception:
            stats.malformed_rows += 1
    for book in cleaner.flush():
        seq += 1
        stats.books += 1
        yield ReplayEvent(float(book["ts"]), 1_000_000_000 + seq, "book", book)


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


def iter_rows(path: Path, stats: Stats, time_filter: TimeFilter) -> Iterator[dict[str, Any]]:
    if path.is_dir():
        for child in sorted(p for p in path.rglob("*") if p.is_file()):
            if should_skip_path(child):
                continue
            yield from iter_rows(child, stats, time_filter)
        return
    if file_outside_time_filter(path, time_filter):
        log_file_skip(str(path), "outside_time_filter_filename_date", stats)
        return
    suffix = path.suffix.lower()
    if is_tar_archive_path(path):
        try:
            with tarfile.open(path, "r:*") as tf:
                for member in sorted((m for m in tf.getmembers() if m.isfile()), key=lambda m: m.name):
                    inner = Path(member.name)
                    if should_skip_path(inner):
                        continue
                    source = f"{path}::{member.name}"
                    if file_outside_time_filter(inner, time_filter):
                        log_file_skip(source, "outside_time_filter_filename_date", stats)
                        continue
                    try:
                        raw = tf.extractfile(member)
                        if raw is None:
                            log_file_skip(source, "empty tar member", stats)
                            continue
                        with raw:
                            text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
                            yield from iter_text_rows(text, inner.suffix.lower(), source, stats)
                    except Exception as exc:
                        log_file_skip(source, str(exc), stats)
        except Exception as exc:
            log_file_skip(str(path), str(exc), stats)
        return
    if suffix == ".zip":
        try:
            with zipfile.ZipFile(path) as zf:
                for name in sorted(zf.namelist()):
                    inner = Path(name)
                    if name.endswith("/") or should_skip_path(inner):
                        continue
                    source = f"{path}::{name}"
                    if file_outside_time_filter(inner, time_filter):
                        log_file_skip(source, "outside_time_filter_filename_date", stats)
                        continue
                    try:
                        with zf.open(name) as raw:
                            text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
                            yield from iter_text_rows(text, inner.suffix.lower(), source, stats)
                    except Exception as exc:
                        log_file_skip(source, str(exc), stats)
        except Exception as exc:
            log_file_skip(str(path), str(exc), stats)
        return
    if suffix == ".gz":
        inner = path.with_suffix("").suffix.lower()
        try:
            with gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="") as f:
                yield from iter_text_rows(f, inner, str(path), stats)
        except Exception as exc:
            log_file_skip(str(path), str(exc), stats)
        return
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            yield from iter_text_rows(f, suffix, str(path), stats)
    except Exception as exc:
        log_file_skip(str(path), str(exc), stats)


def iter_text_rows(f: io.TextIOBase, suffix: str, source: str, stats: Stats) -> Iterator[dict[str, Any]]:
    sample = f.read(4096)
    try:
        f.seek(0)
    except Exception as exc:
        log_file_skip(source, f"stream is not seekable after sniffing: {exc}", stats)
        return
    detected_format = detect_text_format(suffix, sample)
    if not detected_format:
        log_file_skip(source, "empty or unsupported text content", stats)
        return

    log_file_detected(source, detected_format, stats)
    if detected_format == "csv":
        yield from iter_csv_rows(f, sample, source, stats)
    elif detected_format == "json":
        yield from iter_json_rows(f, source, stats)
    elif detected_format == "jsonl":
        yield from iter_jsonl_rows(f, source, stats)


def iter_csv_rows(f: io.TextIOBase, sample: str, source: str, stats: Stats) -> Iterator[dict[str, Any]]:
    try:
        has_header = csv.Sniffer().has_header(sample) if sample.strip() else False
    except csv.Error:
        has_header = False
    try:
        if has_header:
            yield from csv.DictReader(f)
        else:
            for row in csv.reader(f):
                if not row:
                    continue
                yield {str(i): value for i, value in enumerate(row)}
    except csv.Error as exc:
        stats.malformed_rows += 1
        log_file_skip(source, f"csv_error:{exc}", stats)


def iter_json_rows(f: io.TextIOBase, source: str, stats: Stats) -> Iterator[dict[str, Any]]:
    text = f.read()
    if not text.strip():
        return
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        stats.malformed_rows += 1
        log_file_skip(source, f"json_decode_error:{exc}", stats)
        return
    yield from expand_json_rows(obj)


def iter_jsonl_rows(f: io.TextIOBase, source: str, stats: Stats) -> Iterator[dict[str, Any]]:
    warned = False
    for line_no, line in enumerate(f, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            stats.malformed_rows += 1
            if not warned:
                log_warning(
                    "[LOCAL-REPLAY-MALFORMED-ROW] path=%s line=%d reason=json_decode_error:%s",
                    source,
                    line_no,
                    exc,
                )
                warned = True
            continue
        yield from expand_json_rows(obj)


def expand_json_rows(obj: Any) -> Iterator[dict[str, Any]]:
    rows = obj if isinstance(obj, list) else obj.get("data") if isinstance(obj, dict) and isinstance(obj.get("data"), list) else [obj]
    for row in rows:
        if isinstance(row, dict):
            yield row


def detect_text_format(suffix: str, sample: str) -> str:
    stripped = sample.lstrip()
    if not stripped:
        return ""
    suffix = suffix.lower()
    first = stripped[0]
    looks_json = first in {"{", "["}
    if suffix == ".csv":
        return "csv"
    if suffix == ".json" and looks_json:
        return "json"
    if suffix in JSON_LINE_SUFFIXES and looks_json:
        return "jsonl"
    if suffix in JSON_SUFFIXES and not looks_json:
        return "csv"
    if looks_json:
        return "json" if first == "[" else "jsonl"
    return "csv"


def should_skip_path(path: Path) -> bool:
    return any(part.startswith(".") for part in path.parts)


def file_outside_time_filter(path: Path, time_filter: TimeFilter) -> bool:
    file_date = extract_date_from_name(path.name)
    if file_date is None:
        return False
    day_start = datetime(file_date.year, file_date.month, file_date.day, tzinfo=timezone.utc).timestamp()
    day_end = day_start + 86400.0
    if time_filter.start_ts is not None and day_end <= time_filter.start_ts:
        return True
    if time_filter.end_ts is not None and day_start >= time_filter.end_ts:
        return True
    return False


def extract_date_from_name(name: str) -> datetime | None:
    match = re.search(r"(20\d{2})-(\d{2})-(\d{2})", name)
    if not match:
        return None
    try:
        return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def is_tar_archive_path(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz"))


def log_file_detected(source: str, detected_format: str, stats: Stats) -> None:
    stats.parsed_files += 1
    log_info("[LOCAL-REPLAY-FILE] path=%s detected_format=%s skipped=false", source, detected_format)


def log_file_skip(source: str, reason: str, stats: Stats) -> None:
    stats.skipped_files += 1
    if reason == "outside_time_filter_filename_date":
        log_info("[LOCAL-REPLAY-FILE-SKIP] path=%s reason=%s", source, reason)
    else:
        log_warning("[LOCAL-REPLAY-FILE-SKIP] path=%s reason=%s", source, reason)


def log_info(message: str, *args: Any) -> None:
    if logger:
        logger.info(message, *args)
    else:
        print(message % args if args else message)


def log_warning(message: str, *args: Any) -> None:
    if logger:
        logger.warning(message, *args)
    else:
        print(message % args if args else message, file=sys.stderr)


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


def normalize_book(row: Mapping[str, Any], symbol: str, multiplier: float, options: BookCleaningOptions) -> dict[str, Any] | None:
    inst = get_any(row, "instId", "inst_id", "symbol", "instrument")
    if inst and str(inst) != symbol:
        return None
    ts = normalize_ts(get_any(row, "ts", "timestamp", "time", "created_time", "0"))
    bids = parse_levels(get_any(row, "bids", "bid", "bid_levels", "1"), multiplier)
    asks = parse_levels(get_any(row, "asks", "ask", "ask_levels", "2"), multiplier)

    # Some historical exports store one level per CSV row instead of nested bids/asks arrays.
    if not bids and not asks:
        side = str(get_any(row, "side", "book_side", "3") or "").lower()
        price = to_float(get_any(row, "px", "price", "p", "4"))
        raw_size = to_float(get_any(row, "sz", "size", "qty", "quantity", "amount", "5"))
        if price > 0 and side in {"bid", "bids", "buy"}:
            bids = [[price, raw_size * multiplier]]
        elif price > 0 and side in {"ask", "asks", "sell"}:
            asks = [[price, raw_size * multiplier]]

    if ts <= 0 or (not bids and not asks):
        return None

    action = str(get_any(row, "action", "type", "event_type", "op") or "").lower()
    if options.event_mode != "auto":
        mode = options.event_mode
    elif action in {"snapshot", "full", "partial"}:
        mode = "snapshot"
    elif action in {"update", "delta", "incremental"}:
        mode = "delta"
    else:
        mode = "snapshot" if len(bids) + len(asks) >= options.snapshot_infer_min_levels else "delta"

    if mode == "snapshot":
        bids = normalize_levels_for_replay(bids, side="bids", depth_limit=options.depth_limit)
        asks = normalize_levels_for_replay(asks, side="asks", depth_limit=options.depth_limit)
    else:
        bids = normalize_levels_for_replay(bids, side="bids", depth_limit=0)
        asks = normalize_levels_for_replay(asks, side="asks", depth_limit=0)

    return {"bids": bids, "asks": asks, "ts": ts, "recv_ts": ts, "_mode": mode}


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


def merge_level_updates(base_levels: Sequence[Sequence[float]], incoming_levels: Sequence[Sequence[float]], side: str, depth_limit: int) -> list[list[float]]:
    state: dict[float, float] = {}
    apply_level_updates_to_state(state, base_levels)
    apply_level_updates_to_state(state, incoming_levels, keep_zero=True)
    return sort_levels_from_state(state, side=side, depth_limit=depth_limit, keep_zero=True)


def apply_level_updates_to_state(state: dict[float, float], levels: Sequence[Sequence[float]], keep_zero: bool = False) -> None:
    for item in levels:
        if len(item) < 2:
            continue
        price = float(item[0])
        size = float(item[1])
        if size == 0 and not keep_zero:
            state.pop(price, None)
        else:
            state[price] = size


def normalize_levels_for_replay(levels: Sequence[Sequence[float]], side: str, depth_limit: int) -> list[list[float]]:
    state = {float(price): float(size) for price, size in levels if float(price) > 0}
    return sort_levels_from_state(state, side=side, depth_limit=depth_limit, keep_zero=True)


def levels_to_state(levels: Sequence[Sequence[float]], side: str, depth_limit: int) -> dict[float, float]:
    normalized = normalize_levels_for_replay(levels, side=side, depth_limit=depth_limit)
    return {float(price): float(size) for price, size in normalized if float(size) > 0}


def sort_levels_from_state(state: Mapping[float, float], side: str, depth_limit: int, keep_zero: bool = False) -> list[list[float]]:
    reverse = side == "bids"
    items = [(float(price), float(size)) for price, size in state.items() if keep_zero or float(size) > 0]
    items.sort(key=lambda x: x[0], reverse=reverse)
    if depth_limit and depth_limit > 0:
        items = items[:depth_limit]
    return [[price, size] for price, size in items]


def diff_states(old: Mapping[float, float], new: Mapping[float, float], side: str) -> tuple[list[list[float]], int]:
    delta: list[list[float]] = []
    delete_count = 0
    for price, size in new.items():
        if old.get(price) != size:
            delta.append([float(price), float(size)])
    for price in old.keys() - new.keys():
        delta.append([float(price), 0.0])
        delete_count += 1
    delta.sort(key=lambda x: x[0], reverse=(side == "bids"))
    return delta, delete_count


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
