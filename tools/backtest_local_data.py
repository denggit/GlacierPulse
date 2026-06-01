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
import heapq
import io
import json
import os
import re
import sys
import tarfile
import time
import zipfile
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logger = None

TEXT_SUFFIXES = {".csv", ".jsonl", ".ndjson", ".json"}
JSON_LINE_SUFFIXES = {".jsonl", ".ndjson"}
JSON_SUFFIXES = {".json", ".jsonl", ".ndjson"}
CONTRACT_MULTIPLIERS = {"ETH-USDT-SWAP": 0.1}
LOCAL_REPLAY_LOG_OVERRIDE_ENV_KEYS = (
    "V62_INTEGRATION_HEARTBEAT_ENABLED",
    "V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED",
    "V62_LOG_COMPONENT_STATUS_ON_START",
    "V62_LOG_CONFIG_SNAPSHOT_ON_START",
)
ZONE_V2_INTERNAL_PROFILE_FIELDS = {
    "book_profile_start",
    "book_profile_min",
    "book_profile_end",
    "trade_notional_by_bucket",
    "_zone_v2_profile_keys",
    "_zone_v2_scan_lower",
    "_zone_v2_scan_upper",
}


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


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
    skipped_ignored_engine_return_writes: int = 0
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
            "skipped_ignored_engine_return_writes": self.skipped_ignored_engine_return_writes,
            "malformed_rows": self.malformed_rows,
            "parsed_files": self.parsed_files,
            "skipped_files": self.skipped_files,
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
            "first_time_utc": fmt_utc(self.first_ts),
            "last_time_utc": fmt_utc(self.last_ts),
            "duration_sec": round(self.last_ts - self.first_ts, 6) if self.first_ts and self.last_ts else 0.0,
        }


@dataclass
class ReplayProfiler:
    total_sec: float = 0.0
    event_loop_sec: float = 0.0
    trade_tick_sec: float = 0.0
    book_update_sec: float = 0.0
    book_cleaner_push_sec: float = 0.0
    book_cleaner_finalize_sec: float = 0.0
    book_cleaner_diff_sec: float = 0.0
    write_research_event_sec: float = 0.0
    report_generation_sec: float = 0.0
    bucket_push_count: int = 0
    finalize_bucket_count: int = 0
    empty_bucket_count: int = 0
    finalized_bid_levels_total: int = 0
    finalized_ask_levels_total: int = 0
    finalized_bid_levels_max: int = 0
    finalized_ask_levels_max: int = 0
    bid_delta_levels_total: int = 0
    ask_delta_levels_total: int = 0

    def timing_summary(self) -> dict[str, float]:
        return {
            "total_sec": round(self.total_sec, 6),
            "event_loop_sec": round(self.event_loop_sec, 6),
            "trade_tick_sec": round(self.trade_tick_sec, 6),
            "book_update_sec": round(self.book_update_sec, 6),
            "book_cleaner_push_sec": round(self.book_cleaner_push_sec, 6),
            "book_cleaner_finalize_sec": round(self.book_cleaner_finalize_sec, 6),
            "book_cleaner_diff_sec": round(self.book_cleaner_diff_sec, 6),
            "write_research_event_sec": round(self.write_research_event_sec, 6),
            "report_generation_sec": round(self.report_generation_sec, 6),
        }

    def rates_summary(self, stats: Stats) -> dict[str, float]:
        event_sec = self.event_loop_sec if self.event_loop_sec > 0 else self.total_sec
        return {
            "events_per_sec": round((stats.trades + stats.books) / event_sec, 3) if event_sec > 0 else 0.0,
            "trades_per_sec": round(stats.trades / event_sec, 3) if event_sec > 0 else 0.0,
            "books_per_sec": round(stats.books / event_sec, 3) if event_sec > 0 else 0.0,
            "raw_book_rows_per_sec": round(stats.raw_book_rows / event_sec, 3) if event_sec > 0 else 0.0,
            "research_events_per_sec": round(stats.research_events / event_sec, 3) if event_sec > 0 else 0.0,
        }

    def book_cleaning_summary(self, stats: Stats) -> dict[str, Any]:
        finalized = self.finalize_bucket_count
        return {
            "bucket_push_count": self.bucket_push_count,
            "finalize_bucket_count": finalized,
            "empty_bucket_count": self.empty_bucket_count,
            "bucket_coalesces": stats.book_bucket_coalesces,
            "snapshot_rebuilds": stats.book_snapshot_rebuilds,
            "finalized_bid_levels_total": self.finalized_bid_levels_total,
            "finalized_ask_levels_total": self.finalized_ask_levels_total,
            "finalized_bid_levels_max": self.finalized_bid_levels_max,
            "finalized_ask_levels_max": self.finalized_ask_levels_max,
            "avg_bid_levels_per_finalized_book": round(self.finalized_bid_levels_total / finalized, 6) if finalized else 0.0,
            "avg_ask_levels_per_finalized_book": round(self.finalized_ask_levels_total / finalized, 6) if finalized else 0.0,
            "bid_delta_levels_total": self.bid_delta_levels_total,
            "ask_delta_levels_total": self.ask_delta_levels_total,
            "zero_delete_levels_total": stats.book_zero_delete_levels,
        }


class LocalA1ResearchRuntime:
    """Offline equivalent of main.py's current research-only callback pipeline."""

    def __init__(
        self,
        symbol: str,
        research_events_path: Path,
        write_ignored_engine_returns: bool = False,
        profiler: ReplayProfiler | None = None,
    ):
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
        self.write_ignored_engine_returns = bool(write_ignored_engine_returns)
        self.profiler = profiler

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
        if not getattr(self, "write_ignored_engine_returns", False):
            stats.skipped_ignored_engine_return_writes += 1
            return
        self._write_research_event(event, current_price=current_price, source=source, ignored=True)

    def _write_research_event(
        self,
        event: Mapping[str, Any],
        current_price: float,
        source: str,
        ignored: bool = False,
    ) -> None:
        start = time.perf_counter()
        try:
            row = dict(event)
            row["replay_source"] = source
            row["replay_current_price"] = current_price
            row["replay_ignored"] = bool(ignored)
            row["runtime_mode"] = "local_research_replay"
            row["execution_enabled"] = False
            if not _truthy(getattr(self.research_config, "ZONE_BOUNDARY_V2_WRITE_PROFILE_MAPS", False)):
                for field_name in ZONE_V2_INTERNAL_PROFILE_FIELDS:
                    row.pop(field_name, None)
            else:
                for field_name in ("_zone_v2_profile_keys", "_zone_v2_scan_lower", "_zone_v2_scan_upper"):
                    row.pop(field_name, None)
            self.research_events_file.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        finally:
            profiler = getattr(self, "profiler", None)
            if profiler is not None:
                profiler.write_research_event_sec += time.perf_counter() - start


class BookEventCleaner:
    """Normalize historical book rows to live-like OKX `books` update events."""

    def __init__(self, options: BookCleaningOptions, stats: Stats, profiler: ReplayProfiler | None = None):
        self.options = options
        self.stats = stats
        self.profiler = profiler
        self.pending_bucket: int | None = None
        self.bucket_bids_state: dict[float, float] = {}
        self.bucket_asks_state: dict[float, float] = {}
        self.bucket_ts: float = 0.0
        self.bucket_recv_ts: float = 0.0
        self.bucket_mode: str = "delta"
        self.prev_sent_bids_state: dict[float, float] = {}
        self.prev_sent_asks_state: dict[float, float] = {}

    def push(self, raw_book: dict[str, Any]) -> list[dict[str, Any]]:
        profiler = self.profiler
        if profiler is not None:
            profiler.bucket_push_count += 1
        if self.options.bucket_ms <= 0:
            cleaned = self._clean_immediate(raw_book)
            return [cleaned] if cleaned else []

        bucket = int(float(raw_book["ts"]) * 1000.0 // float(self.options.bucket_ms))
        if self.pending_bucket is None:
            self._start_bucket(bucket, raw_book)
            return []

        if bucket == self.pending_bucket:
            self._apply_raw_book_to_bucket(raw_book)
            self.stats.book_bucket_coalesces += 1
            return []

        cleaned = self._finalize_bucket()
        self._start_bucket(bucket, raw_book)
        return [cleaned] if cleaned else []

    def flush(self) -> list[dict[str, Any]]:
        if self.pending_bucket is None:
            return []
        cleaned = self._finalize_bucket()
        self.pending_bucket = None
        self.bucket_bids_state = {}
        self.bucket_asks_state = {}
        self.bucket_ts = 0.0
        self.bucket_recv_ts = 0.0
        self.bucket_mode = "delta"
        return [cleaned] if cleaned else []

    def _start_bucket(self, bucket: int, raw_book: dict[str, Any]) -> None:
        self.pending_bucket = bucket
        if raw_book.get("_mode", "delta") == "snapshot":
            self.bucket_bids_state = {}
            self.bucket_asks_state = {}
        else:
            self.bucket_bids_state = dict(self.prev_sent_bids_state)
            self.bucket_asks_state = dict(self.prev_sent_asks_state)
        self.bucket_ts = float(raw_book["ts"])
        self.bucket_recv_ts = float(raw_book.get("recv_ts", raw_book["ts"]))
        self.bucket_mode = str(raw_book.get("_mode", "delta"))
        self._apply_raw_book_to_bucket(raw_book)

    def _apply_raw_book_to_bucket(self, raw_book: dict[str, Any]) -> None:
        mode = raw_book.get("_mode", "delta")
        if mode == "snapshot":
            self.stats.book_snapshot_rebuilds += 1
            self.bucket_bids_state = levels_to_state(raw_book.get("bids", []), side="bids", depth_limit=self.options.depth_limit)
            self.bucket_asks_state = levels_to_state(raw_book.get("asks", []), side="asks", depth_limit=self.options.depth_limit)
            self.bucket_mode = "snapshot"
        else:
            apply_level_updates_to_state(self.bucket_bids_state, raw_book.get("bids", []))
            apply_level_updates_to_state(self.bucket_asks_state, raw_book.get("asks", []))
        self.bucket_ts = float(raw_book["ts"])
        self.bucket_recv_ts = float(raw_book.get("recv_ts", raw_book["ts"]))

    def _finalize_bucket(self) -> dict[str, Any] | None:
        profiler = self.profiler
        finalize_start = time.perf_counter()
        try:
            current_bids = trim_state_to_depth(
                self.bucket_bids_state,
                side="bids",
                depth_limit=self.options.depth_limit,
            )
            current_asks = trim_state_to_depth(
                self.bucket_asks_state,
                side="asks",
                depth_limit=self.options.depth_limit,
            )

            diff_start = time.perf_counter()
            bid_delta, bid_deletes = diff_states(self.prev_sent_bids_state, current_bids, side="bids")
            ask_delta, ask_deletes = diff_states(self.prev_sent_asks_state, current_asks, side="asks")
            if profiler is not None:
                profiler.book_cleaner_diff_sec += time.perf_counter() - diff_start
                profiler.finalize_bucket_count += 1
                bid_levels = len(current_bids)
                ask_levels = len(current_asks)
                profiler.finalized_bid_levels_total += bid_levels
                profiler.finalized_ask_levels_total += ask_levels
                profiler.finalized_bid_levels_max = max(profiler.finalized_bid_levels_max, bid_levels)
                profiler.finalized_ask_levels_max = max(profiler.finalized_ask_levels_max, ask_levels)
                profiler.bid_delta_levels_total += len(bid_delta)
                profiler.ask_delta_levels_total += len(ask_delta)
            self.stats.book_zero_delete_levels += bid_deletes + ask_deletes

            self.prev_sent_bids_state = current_bids
            self.prev_sent_asks_state = current_asks
            if not bid_delta and not ask_delta:
                if profiler is not None:
                    profiler.empty_bucket_count += 1
                return None
            return {"bids": bid_delta, "asks": ask_delta, "ts": self.bucket_ts, "recv_ts": self.bucket_recv_ts}
        finally:
            if profiler is not None:
                profiler.book_cleaner_finalize_sec += time.perf_counter() - finalize_start

    def _clean_immediate(self, raw_book: dict[str, Any]) -> dict[str, Any] | None:
        if raw_book.get("_mode", "delta") == "snapshot":
            self._start_bucket(0, raw_book)
            cleaned = self._finalize_bucket()
            self.pending_bucket = None
            self.bucket_bids_state = {}
            self.bucket_asks_state = {}
            return cleaned

        bids = normalize_levels_for_replay(raw_book.get("bids", []), side="bids", depth_limit=0)
        asks = normalize_levels_for_replay(raw_book.get("asks", []), side="asks", depth_limit=0)
        self.pending_bucket = None
        self.bucket_bids_state = {}
        self.bucket_asks_state = {}
        apply_level_updates_to_state(self.prev_sent_bids_state, bids)
        apply_level_updates_to_state(self.prev_sent_asks_state, asks)
        if not bids and not asks:
            return None
        return {"bids": bids, "asks": asks, "ts": raw_book["ts"], "recv_ts": raw_book.get("recv_ts", raw_book["ts"])}


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
    p.add_argument("--enable-runtime-events-cache", choices=["true", "false"], default="true")
    p.add_argument("--runtime-events-cache-root", type=Path, default=ROOT / "data" / "derived" / "runtime_events")
    p.add_argument("--runtime-events-bucket-sec", type=float, default=1.0)
    p.add_argument("--runtime-events-rolling-sec", type=float, default=3.0)
    p.add_argument("--runtime-events-cache-overwrite", choices=["true", "false"], default="false")
    p.add_argument("--runtime-events-cache-days", help="Optional comma-separated UTC dates, YYYY-MM-DD.")
    p.add_argument("--disable-runtime-3a", action="store_true", help="Disable V7.3 runtime A2/A3 report generation for this run.")
    p.add_argument("--start-date", help="Replay lower bound as YYYY-MM-DD at 00:00:00 UTC.")
    p.add_argument("--end-date", help="Replay upper bound as inclusive YYYY-MM-DD; internally next day 00:00:00 UTC.")
    p.add_argument("--start-time", help="Replay lower bound as ISO8601 UTC timestamp. Timezone-less values are UTC.")
    p.add_argument("--end-time", help="Replay upper bound as ISO8601 UTC timestamp. Timezone-less values are UTC.")
    p.add_argument("--sort-in-memory", action="store_true")
    p.add_argument("--allow-missing-books", action="store_true")
    p.add_argument("--max-events", type=int, default=0)
    p.add_argument("--progress-every", type=int, default=100_000)
    p.add_argument("--books-event-mode", choices=["auto", "delta", "snapshot"], default="auto", help="How historical book rows should be interpreted before replay. Default: auto.")
    p.add_argument("--books-depth-limit", type=int, default=400, help="Depth limit per side for snapshot-style historical books. Default: 400.")
    p.add_argument("--books-bucket-ms", type=float, default=100.0, help="Coalesce book updates into this many milliseconds to mimic OKX live books cadence. Use 0 to disable. Default: 100.")
    p.add_argument("--books-snapshot-infer-min-levels", type=int, default=100, help="In auto mode, treat a coalesced book event as snapshot once it contains at least this many levels. Default: 100.")
    p.add_argument("--generate-reports", action="store_true", help="Generate unified research reports after replay completes.")
    p.add_argument("--write-ignored-engine-returns", action="store_true", help="Write ignored A1 engine debug returns to research_events.jsonl. Default: count only.")
    p.add_argument("--kline", type=Path, help="1m kline CSV used by unified research reports.")
    p.add_argument("--report-run-name", help="Report run name under reports/. Default: backtests/<run_name>/research_reports.")
    p.add_argument("--report-min-sample", type=int, default=30)
    p.add_argument("--report-timezone", default="Asia/Shanghai", help="Timezone passed only to research report generation and kline parsing.")
    p.add_argument(
        "--use-shared-research-logs",
        action="store_true",
        help="Use config/default logs/research paths instead of isolating JSONL outputs under this run directory.",
    )
    args = p.parse_args(argv)
    if args.generate_reports and not args.kline:
        raise SystemExit("--generate-reports requires --kline")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    time_filter = parse_time_window(args)
    trades_input = args.trades_file or args.trades_dir
    books_input = args.books_file or args.books_dir
    if not trades_input:
        raise SystemExit("Missing --trades-dir or --trades-file")
    if not books_input and not args.allow_missing_books:
        raise SystemExit("Missing --books-dir/--books-file. Use --allow-missing-books only for parser smoke tests.")

    date_set = utc_date_set_for_filter(time_filter)
    trades_files = [Path(args.trades_file)] if args.trades_file else select_files_by_utc_dates(Path(args.trades_dir), date_set, "trades")
    books_files = (
        [Path(args.books_file)]
        if args.books_file
        else select_files_by_utc_dates(Path(args.books_dir), date_set, "books")
        if books_input
        else []
    )
    selection_mode = "explicit_file" if args.trades_file and (args.books_file or not books_input) else "utc_filename_date"

    out_dir = args.out_dir or (ROOT / "reports" / "backtests" / args.run_name)
    out_dir.mkdir(parents=True, exist_ok=True)
    research_dir = out_dir / "research"
    research_dir.mkdir(parents=True, exist_ok=True)

    configure_runtime_environment(args=args, out_dir=out_dir, research_dir=research_dir)

    from src.config.runtime_profile_loader import load_runtime_profile
    from src.research.runtime_three_a.runtime_event_builder import (
        RuntimeEventAccumulator,
        RuntimeEventCacheManager,
    )
    from src.utils.log import get_logger

    load_runtime_profile()
    global logger
    logger = get_logger("BacktestLocalData")

    multiplier = resolve_contract_multiplier_for_replay(str(args.symbol), args.contract_multiplier)
    runtime_cache_enabled = parse_bool_arg(args.enable_runtime_events_cache)
    runtime_cache_manager: RuntimeEventCacheManager | None = None
    runtime_accumulator: RuntimeEventAccumulator | None = None
    runtime_writers: dict[str, Any] = {}
    selected_days = parse_runtime_cache_days(args.runtime_events_cache_days) or infer_utc_days_from_paths(trades_files)
    actual_runtime_days: set[str] = set()
    cache_hit_days: set[str] = set()
    cache_miss_days: set[str] = set()
    generated_days: set[str] = set()
    runtime_events_used_by_zone_truth = False
    if runtime_cache_enabled:
        runtime_cache_manager = RuntimeEventCacheManager(
            cache_root=Path(args.runtime_events_cache_root),
            symbol=str(args.symbol),
            bucket_sec=float(args.runtime_events_bucket_sec),
            rolling_sec=float(args.runtime_events_rolling_sec),
            contract_multiplier=float(multiplier),
            notional_mode="price_x_size_x_contract_multiplier",
            overwrite=parse_bool_arg(args.runtime_events_cache_overwrite),
        )
        for day in selected_days:
            if runtime_cache_manager.has_valid_day(day):
                cache_hit_days.add(day)
            else:
                cache_miss_days.add(day)
        if not selected_days or cache_miss_days:
            runtime_accumulator = RuntimeEventAccumulator(
                symbol=str(args.symbol),
                bucket_sec=float(args.runtime_events_bucket_sec),
                rolling_sec=float(args.runtime_events_rolling_sec),
                contract_multiplier=float(multiplier),
                notional_mode="price_x_size_x_contract_multiplier",
            )
    book_cleaning = BookCleaningOptions(
        event_mode=str(args.books_event_mode),
        depth_limit=max(1, int(args.books_depth_limit)),
        bucket_ms=float(args.books_bucket_ms),
        snapshot_infer_min_levels=max(1, int(args.books_snapshot_infer_min_levels)),
    )
    research_events_path = out_dir / "research_events.jsonl"
    summary_path = out_dir / "summary.json"

    logger.info("[LOCAL-A1-RESEARCH-START] symbol=%s trades=%s books=%s out=%s", args.symbol, trades_input, books_input, out_dir)
    logger.info("[LOCAL-A1-RESEARCH-MODE] no_main_py=true no_websocket=true no_iceberg_trader=true no_execution=true")
    logger.info("[LOCAL-BOOK-CLEANING] %s", asdict(book_cleaning))
    logger.info("[LOCAL-TIME-FILTER] %s", time_filter.to_dict())
    logger.info("[LOCAL-SELECTED-FILES] trades=%s books=%s mode=%s", [str(path) for path in trades_files], [str(path) for path in books_files], selection_mode)

    stats = Stats()
    profiler = ReplayProfiler()
    total_start = time.perf_counter()
    runtime = LocalA1ResearchRuntime(
        symbol=str(args.symbol),
        research_events_path=research_events_path,
        write_ignored_engine_returns=bool(args.write_ignored_engine_returns),
        profiler=profiler,
    )
    warmup = {
        "book_warmup_rows": 0,
        "book_bootstrap_bid_levels": 0,
        "book_bootstrap_ask_levels": 0,
        "book_warmup_start_utc": "",
        "book_warmup_end_utc": fmt_utc(time_filter.start_ts or 0.0),
        "book_warmup_last_row_utc": "",
    }
    if books_files and time_filter.start_ts is not None:
        warmup = warmup_books_state(
            runtime=runtime,
            book_files=books_files,
            symbol=str(args.symbol),
            multiplier=float(multiplier),
            options=book_cleaning,
            start_ts=float(time_filter.start_ts),
        )
        if warmup["book_bootstrap_bid_levels"] or warmup["book_bootstrap_ask_levels"]:
            bootstrap_book = {
                "bids": sort_levels_from_state(runtime.ctx.bids, side="bids", depth_limit=book_cleaning.depth_limit),
                "asks": sort_levels_from_state(runtime.ctx.asks, side="asks", depth_limit=book_cleaning.depth_limit),
                "ts": float(time_filter.start_ts),
                "recv_ts": float(time_filter.start_ts),
            }
            stats.books += 1
            runtime.on_book_update(bootstrap_book, stats)
            stats.touch(float(time_filter.start_ts))
            logger.info(
                "[LOCAL-BOOK-WARMUP] rows=%d bootstrap_bids=%d bootstrap_asks=%d ts=%s",
                warmup["book_warmup_rows"],
                warmup["book_bootstrap_bid_levels"],
                warmup["book_bootstrap_ask_levels"],
                fmt_utc(float(time_filter.start_ts)),
            )

    event_loop_start = time.perf_counter()
    replay_success = False
    try:
        events = build_events(
            trades_files,
            books_files,
            str(args.symbol),
            float(multiplier),
            stats,
            bool(args.sort_in_memory),
            book_cleaning,
            time_filter,
            profiler,
        )
        for idx, event in enumerate(events, start=1):
            if args.max_events and idx > args.max_events:
                break
            stats.touch(event.ts)
            if event.kind == "trade":
                tick_start = time.perf_counter()
                if runtime_cache_enabled and runtime_cache_manager is not None:
                    handle_runtime_cache_trade(
                        trade=event.payload,
                        event_ts=event.ts,
                        manager=runtime_cache_manager,
                        accumulator=runtime_accumulator,
                        writers=runtime_writers,
                        selected_days=selected_days,
                        actual_days=actual_runtime_days,
                        cache_hit_days=cache_hit_days,
                        cache_miss_days=cache_miss_days,
                        generated_days=generated_days,
                    )
                runtime.on_trade_tick(event.payload, stats)
                profiler.trade_tick_sec += time.perf_counter() - tick_start
            elif event.kind == "book":
                book_start = time.perf_counter()
                runtime.on_book_update(event.payload, stats)
                profiler.book_update_sec += time.perf_counter() - book_start
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
        replay_success = True
    finally:
        if runtime_cache_enabled and runtime_cache_manager is not None:
            if replay_success:
                finalize_runtime_cache_writers(
                    manager=runtime_cache_manager,
                    accumulator=runtime_accumulator,
                    writers=runtime_writers,
                    cache_miss_days=cache_miss_days,
                    generated_days=generated_days,
                )
            else:
                for writer in runtime_writers.values():
                    writer.cleanup()
                runtime_writers.clear()
        runtime.close()

    elapsed = time.perf_counter() - event_loop_start
    profiler.event_loop_sec = elapsed
    report_generation = build_report_generation_summary(
        enabled=bool(args.generate_reports),
        status="skipped",
        report_run_name=resolve_report_run_name(args),
        kline_path=args.kline,
        phase1_candidates_path=research_dir / "phase1_candidates.jsonl",
        a1_reactions_path=research_dir / "a1_reaction_events.jsonl",
        created_empty_research_inputs=[],
    )

    report_exit_code = 0
    if args.generate_reports:
        report_start = time.perf_counter()
        runtime_events_source = None
        if runtime_cache_enabled and runtime_cache_manager is not None:
            effective_days = sorted(actual_runtime_days or set(selected_days))
            runtime_events_source = runtime_cache_manager.selected_source(effective_days)
            runtime_events_used_by_zone_truth = bool(runtime_events_source is not None and not args.disable_runtime_3a)
            runtime_cache_manager.write_run_ref(
                out_dir,
                effective_days,
                sorted(cache_hit_days & set(effective_days)),
                sorted(cache_miss_days & set(effective_days)),
                sorted(generated_days & set(effective_days)),
                runtime_events_used_by_zone_truth=runtime_events_used_by_zone_truth,
            )
        report_generation, report_exit_code = generate_replay_research_reports(
            args=args,
            research_dir=research_dir,
            report_generation=report_generation,
            runtime_events_path=runtime_cache_manager.cache_dir() if runtime_events_used_by_zone_truth and runtime_cache_manager is not None else None,
        )
        if runtime_cache_enabled and not runtime_events_used_by_zone_truth and not args.disable_runtime_3a:
            patch_zone_truth_runtime_status(resolve_report_run_name(args), "SKIPPED_NO_RUNTIME_EVENTS_CACHE")
        profiler.report_generation_sec = time.perf_counter() - report_start
    elif runtime_cache_enabled and runtime_cache_manager is not None:
        effective_days = sorted(actual_runtime_days or set(selected_days))
        runtime_cache_manager.write_run_ref(
            out_dir,
            effective_days,
            sorted(cache_hit_days & set(effective_days)),
            sorted(cache_miss_days & set(effective_days)),
            sorted(generated_days & set(effective_days)),
            runtime_events_used_by_zone_truth=False,
        )
    profiler.total_sec = time.perf_counter() - total_start

    summary = {
        "run_name": args.run_name,
        "symbol": args.symbol,
        "contract_multiplier": multiplier,
        "trades_path": str(trades_input),
        "books_path": str(books_input) if books_input else "",
        "time_window": {
            "start_utc": fmt_utc(time_filter.start_ts or 0.0),
            "end_utc": fmt_utc(time_filter.end_ts or 0.0),
            "time_assumption": "UTC",
        },
        "selected_files": {
            "trades": [str(path) for path in trades_files],
            "books": [str(path) for path in books_files],
            "selection_mode": selection_mode,
        },
        "runtime_mode": "local_research_replay",
        "execution_enabled": False,
        "book_cleaning": asdict(book_cleaning),
        "book_warmup": warmup,
        "research_events_path": str(research_events_path),
        "research_jsonl_paths": {
            "phase1_candidates": os.getenv("PHASE1_CANDIDATE_RECORDER_JSONL_PATH", "logs/research/phase1_candidates.jsonl"),
            "a1_reaction_events": os.getenv("A1_REACTION_EVENT_RECORDER_JSONL_PATH", "logs/research/a1_reaction_events.jsonl"),
        },
        "local_replay_log_overrides": {
            key: os.getenv(key, "")
            for key in LOCAL_REPLAY_LOG_OVERRIDE_ENV_KEYS
        },
        "report_generation": report_generation,
        "runtime_events_cache": {
            "enabled": runtime_cache_enabled,
            "cache_dir": str(runtime_cache_manager.cache_dir()) if runtime_cache_manager is not None else "",
            "selected_days": sorted(actual_runtime_days or set(selected_days)),
            "cache_hit_days": sorted(cache_hit_days & set(actual_runtime_days or selected_days)),
            "cache_miss_days": sorted(cache_miss_days & set(actual_runtime_days or selected_days)),
            "generated_days": sorted(generated_days & set(actual_runtime_days or selected_days)),
            "reused_days": sorted(cache_hit_days & set(actual_runtime_days or selected_days)),
            "estimated_cache_size_mb": estimated_runtime_cache_size_mb(runtime_cache_manager.cache_dir()) if runtime_cache_manager is not None else 0.0,
            "runtime_events_used_by_zone_truth": runtime_events_used_by_zone_truth,
        },
        "replay_timing": profiler.timing_summary(),
        "replay_rates": profiler.rates_summary(stats),
        "book_cleaning_profile": profiler.book_cleaning_summary(stats),
        "write_ignored_engine_returns": bool(args.write_ignored_engine_returns),
        "skipped_ignored_engine_return_writes": stats.skipped_ignored_engine_return_writes,
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
    return int(report_exit_code)


def resolve_report_run_name(args: argparse.Namespace) -> str:
    if getattr(args, "report_run_name", None):
        return str(args.report_run_name)
    return f"backtests/{args.run_name}/research_reports"


def build_report_generation_summary(
    *,
    enabled: bool,
    status: str,
    report_run_name: str,
    kline_path: Path | None,
    phase1_candidates_path: Path,
    a1_reactions_path: Path,
    created_empty_research_inputs: list[str],
) -> dict[str, Any]:
    report_dir = Path("reports") / report_run_name
    return {
        "enabled": bool(enabled),
        "status": status,
        "run_name": report_run_name,
        "report_dir": str(report_dir),
        "zip_path": str(Path("reports") / f"{report_run_name}.zip"),
        "kline_path": str(kline_path) if kline_path else "",
        "phase1_candidates_path": str(phase1_candidates_path),
        "a1_reactions_path": str(a1_reactions_path),
        "created_empty_research_inputs": list(created_empty_research_inputs),
    }


def parse_bool_arg(value: Any) -> bool:
    return str(value).strip().lower() not in {"0", "false", "no", "off", ""}


def resolve_contract_multiplier_for_replay(symbol: str, value: float | None) -> float:
    if value is None and "-SWAP" in str(symbol).upper():
        raise SystemExit("--contract-multiplier is required for SWAP symbols to avoid incorrect notional.")
    return float(1.0 if value is None else value)


def parse_runtime_cache_days(value: str | None) -> list[str]:
    if not value:
        return []
    out: list[str] = []
    for part in str(value).split(","):
        day = part.strip()
        if not day:
            continue
        parse_date_utc(day)
        out.append(day)
    return sorted(dict.fromkeys(out))


def utc_day(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), timezone.utc).date().isoformat()


def infer_utc_days_from_paths(paths: Sequence[Path]) -> list[str]:
    days: set[str] = set()
    for path in paths:
        for match in re.finditer(r"\d{4}-\d{2}-\d{2}|\d{8}", path.name):
            token = match.group(0)
            if len(token) == 8:
                token = f"{token[:4]}-{token[4:6]}-{token[6:8]}"
            try:
                parse_date_utc(token)
            except SystemExit:
                continue
            days.add(token)
    return sorted(days)


def handle_runtime_cache_trade(
    *,
    trade: Mapping[str, Any],
    event_ts: float,
    manager: Any,
    accumulator: Any,
    writers: dict[str, Any],
    selected_days: list[str],
    actual_days: set[str],
    cache_hit_days: set[str],
    cache_miss_days: set[str],
    generated_days: set[str],
) -> None:
    day = utc_day(event_ts)
    actual_days.add(day)
    if day not in cache_hit_days and day not in cache_miss_days:
        if manager.has_valid_day(day):
            cache_hit_days.add(day)
        else:
            cache_miss_days.add(day)
    if accumulator is None:
        return
    raw_size = trade.get("raw_size", trade.get("size"))
    events = accumulator.update_trade(
        ts=float(event_ts),
        price=float(trade.get("price") or 0.0),
        side=str(trade.get("side") or ""),
        size=to_float(raw_size),
    )
    for event in events:
        write_runtime_cache_event(event, manager, writers, cache_miss_days, generated_days)


def write_runtime_cache_event(
    event: Mapping[str, Any],
    manager: Any,
    writers: dict[str, Any],
    cache_miss_days: set[str],
    generated_days: set[str],
) -> None:
    day = utc_day(float(event.get("ts") or 0.0))
    if day not in cache_miss_days:
        return
    writer = writers.get(day)
    if writer is None:
        writer = manager.begin_day_writer(day)
        writers[day] = writer
    writer.write(event)
    generated_days.add(day)


def finalize_runtime_cache_writers(
    *,
    manager: Any,
    accumulator: Any,
    writers: dict[str, Any],
    cache_miss_days: set[str],
    generated_days: set[str],
) -> None:
    try:
        if accumulator is not None:
            for event in accumulator.flush():
                write_runtime_cache_event(event, manager, writers, cache_miss_days, generated_days)
        for day, writer in list(writers.items()):
            stats = writer.commit()
            manager.finalize_day(day, stats)
    except Exception:
        for writer in writers.values():
            writer.cleanup()
        raise
    finally:
        writers.clear()


def patch_zone_truth_runtime_status(report_run_name: str, status: str) -> None:
    path = ROOT / "reports" / report_run_name / "zone_truth" / "zone_truth_3a_rt_summary.json"
    if not path.exists():
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    payload["runtime_3a_status"] = status
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def estimated_runtime_cache_size_mb(path: Path) -> float:
    if not path.exists():
        return 0.0
    total = sum(child.stat().st_size for child in path.rglob("*.jsonl") if child.is_file())
    return round(total / (1024.0 * 1024.0), 6)


def generate_replay_research_reports(
    *,
    args: argparse.Namespace,
    research_dir: Path,
    report_generation: dict[str, Any],
    runtime_events_path: Path | None = None,
) -> tuple[dict[str, Any], int]:
    from tools.generate_research_reports import main as generate_research_reports_main

    phase1_candidates_path = research_dir / "phase1_candidates.jsonl"
    a1_reactions_path = research_dir / "a1_reaction_events.jsonl"
    created_empty = ensure_research_report_inputs_exist([phase1_candidates_path, a1_reactions_path])
    report_run_name = resolve_report_run_name(args)
    argv = [
        "--run-name",
        report_run_name,
        "--phase1-candidates",
        str(phase1_candidates_path),
        "--a1-reactions",
        str(a1_reactions_path),
        "--kline",
        str(args.kline),
        "--timezone",
        str(args.report_timezone),
        "--min-sample",
        str(args.report_min_sample),
        "--snapshot",
        "--zip",
    ]
    if runtime_events_path is not None:
        argv.extend(["--runtime-events", str(runtime_events_path)])
    if args.disable_runtime_3a:
        argv.extend(["--enable-3a-rt-backtest", "false"])
    exit_code = int(generate_research_reports_main(argv) or 0)
    report_generation.update(
        build_report_generation_summary(
            enabled=True,
            status="success" if exit_code == 0 else "failed",
            report_run_name=report_run_name,
            kline_path=args.kline,
            phase1_candidates_path=phase1_candidates_path,
            a1_reactions_path=a1_reactions_path,
            created_empty_research_inputs=created_empty,
        )
    )
    return report_generation, exit_code


def ensure_research_report_inputs_exist(paths: Sequence[Path]) -> list[str]:
    created_empty: list[str] = []
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            continue
        path.touch()
        created_empty.append(str(path))
    return created_empty


def configure_runtime_environment(args: argparse.Namespace, out_dir: Path, research_dir: Path) -> None:
    os.environ.setdefault("REAL_EXECUTION_ENABLED", "false")
    os.environ.setdefault("PHASE3_REAL_TRADING_ENABLED", "false")
    os.environ.setdefault("VIRTUAL_SHADOW_MODE", "false")
    os.environ.setdefault("A1_REACTION_TO_VIRTUAL_POSITION_ENABLED", "false")
    os.environ.setdefault("V62_INTEGRATION_HEARTBEAT_ENABLED", "false")
    os.environ.setdefault("V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED", "false")
    os.environ.setdefault("V62_LOG_COMPONENT_STATUS_ON_START", "false")
    os.environ.setdefault("V62_LOG_CONFIG_SNAPSHOT_ON_START", "false")

    if args.use_shared_research_logs:
        return

    os.environ["PHASE1_CANDIDATE_RECORDER_WRITE_JSONL"] = "true"
    os.environ["PHASE1_CANDIDATE_RECORDER_JSONL_PATH"] = str(research_dir / "phase1_candidates.jsonl")
    os.environ["A1_REACTION_EVENT_RECORDER_WRITE_JSONL"] = "true"
    os.environ["A1_REACTION_EVENT_RECORDER_JSONL_PATH"] = str(research_dir / "a1_reaction_events.jsonl")
    os.environ["A1_DYNAMIC_PARAM_PREVIEW_JSON_PATH"] = str(out_dir / "runtime_state" / "a1_dynamic_params.json")


def parse_time_window(args: argparse.Namespace) -> TimeFilter:
    start_ts = parse_datetime_to_ts(args.start_time) if args.start_time else None
    end_ts = parse_datetime_to_ts(args.end_time) if args.end_time else None
    if start_ts is None and args.start_date:
        start_ts = date_to_start_ts(args.start_date)
    if end_ts is None and args.end_date:
        end_ts = date_to_next_day_start_ts(args.end_date)
    if start_ts is not None and end_ts is not None and start_ts >= end_ts:
        raise SystemExit(f"Invalid time range: start ({fmt_utc(start_ts)}) must be before exclusive end ({fmt_utc(end_ts)})")
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


def utc_date_set_for_filter(time_filter: TimeFilter) -> set[str]:
    if time_filter.start_ts is None or time_filter.end_ts is None:
        return set()
    if time_filter.start_ts >= time_filter.end_ts:
        raise SystemExit(f"Invalid time range: start ({fmt_utc(time_filter.start_ts)}) must be before exclusive end ({fmt_utc(time_filter.end_ts)})")
    start_date = datetime.fromtimestamp(float(time_filter.start_ts), timezone.utc).date()
    end_probe_ts = max(float(time_filter.start_ts), float(time_filter.end_ts) - 0.000001)
    end_date = datetime.fromtimestamp(end_probe_ts, timezone.utc).date()
    values = set()
    current = start_date
    while current <= end_date:
        values.add(current.isoformat())
        current += timedelta(days=1)
    return values


def select_files_by_utc_dates(input_path: Path, date_set: set[str], kind: str) -> list[Path]:
    candidates = discover_supported_files(input_path)
    if not date_set:
        return candidates

    selected: list[Path] = []
    missing_dates: list[str] = []
    for date_text in sorted(date_set):
        matches = [path for path in candidates if date_text in path.name]
        if not matches:
            missing_dates.append(date_text)
        selected.extend(matches)
    if missing_dates:
        raise SystemExit(f"Missing {kind} files for UTC date(s): {', '.join(missing_dates)} in {input_path}")

    deduped = sorted(dict.fromkeys(selected))
    log_info("[LOCAL-FILE-SELECTION] kind=%s mode=utc_filename_date dates=%s files=%s", kind, sorted(date_set), [str(path) for path in deduped])
    return deduped


def discover_supported_files(input_path: Path) -> list[Path]:
    path = Path(input_path)
    if path.is_file():
        return [path] if is_supported_data_path(path) else []
    if not path.is_dir():
        raise SystemExit(f"Input path does not exist: {input_path}")
    return sorted(child for child in path.rglob("*") if child.is_file() and not should_skip_path(child) and is_supported_data_path(child))


def is_supported_data_path(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith((".csv", ".zip", ".gz", ".tar", ".tar.gz", ".tgz", ".json", ".jsonl", ".ndjson"))


def extract_row_ts_only(row: Mapping[str, Any]) -> float:
    return normalize_ts(get_any(row, "ts", "timestamp", "time", "created_time", "0"))


def row_matches_symbol(row: Mapping[str, Any], symbol: str) -> bool:
    inst = get_any(row, "instId", "inst_id", "symbol", "instrument")
    return True if inst in (None, "") else str(inst) == str(symbol)


def warmup_books_state(
    runtime: LocalA1ResearchRuntime,
    book_files: Sequence[Path],
    symbol: str,
    multiplier: float,
    options: BookCleaningOptions,
    start_ts: float,
) -> dict[str, Any]:
    warmup_stats = Stats()
    cleaner = BookEventCleaner(options=options, stats=warmup_stats)
    warmup_rows = 0
    first_warmup_ts = 0.0
    last_warmup_ts = 0.0
    raw_books = merge_many_raw_books(
        iter_normalized_book_rows_from_file(path, symbol, multiplier, warmup_stats, options, TimeFilter(end_ts=start_ts))
        for path in book_files
    )
    for raw_book in raw_books:
        raw_ts = float(raw_book["ts"])
        if raw_ts >= start_ts:
            continue
        first_warmup_ts = raw_ts if first_warmup_ts <= 0 else min(first_warmup_ts, raw_ts)
        last_warmup_ts = max(last_warmup_ts, raw_ts)
        warmup_rows += 1
        for book in cleaner.push(raw_book):
            runtime.ctx.apply_book_delta(book)
    for book in cleaner.flush():
        runtime.ctx.apply_book_delta(book)
    bids = sort_levels_from_state(runtime.ctx.bids, side="bids", depth_limit=options.depth_limit)
    asks = sort_levels_from_state(runtime.ctx.asks, side="asks", depth_limit=options.depth_limit)
    return {
        "book_warmup_rows": warmup_rows,
        "book_bootstrap_bid_levels": len(bids),
        "book_bootstrap_ask_levels": len(asks),
        "book_warmup_start_utc": fmt_utc(first_warmup_ts),
        "book_warmup_end_utc": fmt_utc(start_ts),
        "book_warmup_last_row_utc": fmt_utc(last_warmup_ts),
    }


def build_events(
    trade_files: Sequence[Path],
    book_files: Sequence[Path],
    symbol: str,
    multiplier: float,
    stats: Stats,
    sort_in_memory: bool,
    book_cleaning: BookCleaningOptions,
    time_filter: TimeFilter,
    profiler: ReplayProfiler | None = None,
) -> Iterator[ReplayEvent]:
    trades = iter_trade_events(trade_files, symbol, multiplier, stats, time_filter)
    books = iter_book_events(book_files, symbol, multiplier, stats, book_cleaning, time_filter, profiler=profiler) if book_files else iter(())
    if sort_in_memory:
        all_events = list(trades) + list(books)
        all_events.sort()
        yield from all_events
    else:
        yield from merge_sorted(trades, books)


def iter_trade_events(
    paths: Sequence[Path],
    symbol: str,
    multiplier: float,
    stats: Stats,
    time_filter: TimeFilter,
    assume_sorted: bool = True,
) -> Iterator[ReplayEvent]:
    yield from merge_many_sorted(
        iter_trade_events_from_file(path, symbol, multiplier, stats, time_filter, seq_offset=idx * 100_000_000, assume_sorted=assume_sorted)
        for idx, path in enumerate(paths)
    )


def iter_trade_events_from_file(
    path: Path,
    symbol: str,
    multiplier: float,
    stats: Stats,
    time_filter: TimeFilter,
    seq_offset: int = 0,
    assume_sorted: bool = True,
) -> Iterator[ReplayEvent]:
    seq = seq_offset
    for row in iter_rows(path, stats, time_filter):
        try:
            ts = extract_row_ts_only(row)
            if ts <= 0:
                stats.malformed_rows += 1
                continue
            if time_filter.start_ts is not None and ts < time_filter.start_ts:
                stats.filtered_trades += 1
                continue
            if time_filter.end_ts is not None and ts >= time_filter.end_ts:
                stats.filtered_trades += 1
                if assume_sorted:
                    break
                continue
            if not row_matches_symbol(row, symbol):
                continue
            trade = normalize_trade(row, symbol, multiplier)
            if not trade:
                continue
            seq += 1
            stats.trades += 1
            yield ReplayEvent(ts, seq, "trade", trade)
        except Exception:
            stats.malformed_rows += 1


def iter_book_events(
    paths: Sequence[Path],
    symbol: str,
    multiplier: float,
    stats: Stats,
    options: BookCleaningOptions,
    time_filter: TimeFilter,
    assume_sorted: bool = True,
    profiler: ReplayProfiler | None = None,
) -> Iterator[ReplayEvent]:
    seq = 0
    cleaner = BookEventCleaner(options=options, stats=stats, profiler=profiler)
    raw_books = merge_many_raw_books(
        iter_normalized_book_rows_from_file(path, symbol, multiplier, stats, options, time_filter, assume_sorted=assume_sorted)
        for path in paths
    )
    for raw_book in raw_books:
        try:
            stats.raw_book_rows += 1
            push_start = time.perf_counter()
            cleaned_books = cleaner.push(raw_book)
            if profiler is not None:
                profiler.book_cleaner_push_sec += time.perf_counter() - push_start
            for book in cleaned_books:
                seq += 1
                stats.books += 1
                yield ReplayEvent(float(book["ts"]), 1_000_000_000 + seq, "book", book)
        except Exception:
            stats.malformed_rows += 1
    push_start = time.perf_counter()
    flushed_books = cleaner.flush()
    if profiler is not None:
        profiler.book_cleaner_push_sec += time.perf_counter() - push_start
    for book in flushed_books:
        seq += 1
        stats.books += 1
        yield ReplayEvent(float(book["ts"]), 1_000_000_000 + seq, "book", book)


def iter_normalized_book_rows_from_file(
    path: Path,
    symbol: str,
    multiplier: float,
    stats: Stats,
    options: BookCleaningOptions,
    time_filter: TimeFilter,
    assume_sorted: bool = True,
) -> Iterator[dict[str, Any]]:
    for row in iter_rows(path, stats, time_filter):
        try:
            ts = extract_row_ts_only(row)
            if ts <= 0:
                stats.malformed_rows += 1
                continue
            if time_filter.start_ts is not None and ts < time_filter.start_ts:
                stats.filtered_books += 1
                stats.filtered_raw_book_rows += 1
                continue
            if time_filter.end_ts is not None and ts >= time_filter.end_ts:
                stats.filtered_books += 1
                stats.filtered_raw_book_rows += 1
                if assume_sorted:
                    break
                continue
            if not row_matches_symbol(row, symbol):
                continue
            raw_book = normalize_book(row, symbol, multiplier, options)
            if not raw_book:
                continue
            yield raw_book
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


def merge_many_sorted(iterators: Sequence[Iterator[ReplayEvent]]) -> Iterator[ReplayEvent]:
    heap: list[tuple[float, int, int, ReplayEvent, Iterator[ReplayEvent]]] = []
    counter = 0
    for iterator in iterators:
        event = next(iterator, None)
        if event is None:
            continue
        heapq.heappush(heap, (event.ts, event.seq, counter, event, iterator))
        counter += 1
    while heap:
        _, _, _, event, iterator = heapq.heappop(heap)
        yield event
        next_event = next(iterator, None)
        if next_event is not None:
            heapq.heappush(heap, (next_event.ts, next_event.seq, counter, next_event, iterator))
            counter += 1


def merge_many_raw_books(iterators: Sequence[Iterator[dict[str, Any]]]) -> Iterator[dict[str, Any]]:
    heap: list[tuple[float, int, dict[str, Any], Iterator[dict[str, Any]]]] = []
    counter = 0
    for iterator in iterators:
        row = next(iterator, None)
        if row is None:
            continue
        heapq.heappush(heap, (float(row["ts"]), counter, row, iterator))
        counter += 1
    while heap:
        _, _, row, iterator = heapq.heappop(heap)
        yield row
        next_row = next(iterator, None)
        if next_row is not None:
            heapq.heappush(heap, (float(next_row["ts"]), counter, next_row, iterator))
            counter += 1


def iter_rows(path: Path, stats: Stats, time_filter: TimeFilter) -> Iterator[dict[str, Any]]:
    if path.is_dir():
        for child in sorted(p for p in path.rglob("*") if p.is_file()):
            if should_skip_path(child):
                continue
            yield from iter_rows(child, stats, time_filter)
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


def is_tar_archive_path(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz"))


def log_file_detected(source: str, detected_format: str, stats: Stats) -> None:
    stats.parsed_files += 1
    log_info("[LOCAL-REPLAY-FILE] path=%s detected_format=%s skipped=false", source, detected_format)


def log_file_skip(source: str, reason: str, stats: Stats) -> None:
    stats.skipped_files += 1
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


def trim_state_to_depth(state: Mapping[float, float], *, side: str, depth_limit: int) -> dict[float, float]:
    if depth_limit <= 0 or len(state) <= depth_limit:
        return {float(price): float(size) for price, size in state.items() if float(size) > 0}

    valid_items = [(float(price), float(size)) for price, size in state.items() if float(size) > 0]
    if len(valid_items) <= depth_limit:
        return dict(valid_items)
    if side == "bids":
        return dict(heapq.nlargest(depth_limit, valid_items, key=lambda item: item[0]))
    return dict(heapq.nsmallest(depth_limit, valid_items, key=lambda item: item[0]))


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
