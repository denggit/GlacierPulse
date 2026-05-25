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
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
class RequestedWindow:
    time_filter: TimeFilter
    timezone_name: str
    date_mode: str
    requested_start_local: str
    requested_end_local: str
    requested_start_utc: str
    requested_end_utc: str


@dataclass
class FileCoverage:
    path: Path
    first_ts: float
    last_ts: float
    row_count: int = 0
    malformed_ts_count: int = 0

    def overlaps(self, time_filter: TimeFilter, tolerance_sec: float = 0.0) -> bool:
        if time_filter.start_ts is not None and self.last_ts + tolerance_sec < time_filter.start_ts:
            return False
        if time_filter.end_ts is not None and self.first_ts - tolerance_sec >= time_filter.end_ts:
            return False
        return True


@dataclass
class CoverageSelection:
    selected: list[FileCoverage]
    all_coverage: list[FileCoverage]

    @property
    def first_ts(self) -> float:
        values = [item.first_ts for item in self.selected if item.first_ts > 0]
        return min(values) if values else 0.0

    @property
    def last_ts(self) -> float:
        values = [item.last_ts for item in self.selected if item.last_ts > 0]
        return max(values) if values else 0.0

    @property
    def paths(self) -> list[Path]:
        return [item.path for item in self.selected]

    @property
    def row_count(self) -> int:
        return sum(item.row_count for item in self.selected)

    @property
    def malformed_ts_count(self) -> int:
        return sum(item.malformed_ts_count for item in self.selected)


@dataclass
class CoverageCacheStats:
    hits: int = 0
    misses: int = 0


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
        self.bucket_bids_state: dict[float, float] = {}
        self.bucket_asks_state: dict[float, float] = {}
        self.bucket_ts: float = 0.0
        self.bucket_recv_ts: float = 0.0
        self.bucket_mode: str = "delta"
        self.prev_sent_bids_state: dict[float, float] = {}
        self.prev_sent_asks_state: dict[float, float] = {}

    def push(self, raw_book: dict[str, Any]) -> list[dict[str, Any]]:
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
        current_bids = levels_to_state(
            sort_levels_from_state(self.bucket_bids_state, side="bids", depth_limit=self.options.depth_limit),
            side="bids",
            depth_limit=self.options.depth_limit,
        )
        current_asks = levels_to_state(
            sort_levels_from_state(self.bucket_asks_state, side="asks", depth_limit=self.options.depth_limit),
            side="asks",
            depth_limit=self.options.depth_limit,
        )

        bid_delta, bid_deletes = diff_states(self.prev_sent_bids_state, current_bids, side="bids")
        ask_delta, ask_deletes = diff_states(self.prev_sent_asks_state, current_asks, side="asks")
        self.stats.book_zero_delete_levels += bid_deletes + ask_deletes

        self.prev_sent_bids_state = current_bids
        self.prev_sent_asks_state = current_asks
        if not bid_delta and not ask_delta:
            return None
        return {"bids": bid_delta, "asks": ask_delta, "ts": self.bucket_ts, "recv_ts": self.bucket_recv_ts}

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
    p.add_argument("--timezone", default="Asia/Shanghai", help="Timezone for local dates and timezone-less ISO timestamps. Default: Asia/Shanghai.")
    p.add_argument("--date-mode", choices=["local", "utc"], default="local", help="Interpret --start-date/--end-date as local natural days or UTC days. Default: local.")
    p.add_argument("--start-date", help="Replay lower bound as YYYY-MM-DD. In local mode this is 00:00:00 in --timezone.")
    p.add_argument("--end-date", help="Replay upper bound as inclusive YYYY-MM-DD; internally next local/UTC day 00:00:00.")
    p.add_argument("--start-time", help="Replay lower bound as ISO8601 timestamp. Overrides --start-date. Timezone-less values use --timezone.")
    p.add_argument("--end-time", help="Replay upper bound as ISO8601 timestamp. Overrides --end-date. Timezone-less values use --timezone.")
    p.add_argument("--auto-discover-files", dest="auto_discover_files", action="store_true", default=True)
    p.add_argument("--no-auto-discover-files", dest="auto_discover_files", action="store_false")
    p.add_argument("--require-full-coverage", dest="require_full_coverage", action="store_true", default=True)
    p.add_argument("--allow-partial-coverage", dest="allow_partial_coverage", action="store_true")
    p.add_argument("--coverage-boundary-tolerance-sec", type=float, default=60.0, help="Tolerance used only for coverage boundary checks. Default: 60.")
    p.add_argument("--coverage-cache-enabled", dest="coverage_cache_enabled", action="store_true", default=True)
    p.add_argument("--no-coverage-cache-enabled", dest="coverage_cache_enabled", action="store_false")
    p.add_argument("--coverage-cache-path", type=Path, default=ROOT / "data" / "okx" / "coverage_index" / "backtest_local_data_coverage.json")
    p.add_argument("--filename-date-hint-enabled", dest="filename_date_hint_enabled", action="store_true", default=True)
    p.add_argument("--no-filename-date-hint-enabled", dest="filename_date_hint_enabled", action="store_false")
    p.add_argument("--filename-date-hint-padding-days", type=int, default=2)
    p.add_argument("--full-directory-coverage-scan", action="store_true", default=False)
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
    requested_window = parse_requested_window(args)
    requested_filter = requested_window.time_filter
    trades_input = args.trades_file or args.trades_dir
    books_input = args.books_file or args.books_dir
    if not trades_input:
        raise SystemExit("Missing --trades-dir or --trades-file")
    if not books_input and not args.allow_missing_books:
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
    coverage_cache_stats = CoverageCacheStats()
    coverage_cache = load_coverage_cache(args.coverage_cache_path) if args.coverage_cache_enabled else {}
    filename_hint_summary: dict[str, Any] = {
        "enabled": bool(args.filename_date_hint_enabled),
        "padding_days": max(0, int(args.filename_date_hint_padding_days)),
        "full_directory_coverage_scan": bool(args.full_directory_coverage_scan),
        "trades_candidates_before": 0,
        "trades_candidates_after": 0,
        "books_candidates_before": 0,
        "books_candidates_after": 0,
    }

    trades_selection = select_files_for_replay(
        trades_input,
        kind="trade",
        symbol=str(args.symbol),
        multiplier=float(multiplier),
        book_options=book_cleaning,
        requested_filter=requested_filter,
        auto_discover=bool(args.auto_discover_files),
        coverage_tolerance_sec=float(args.coverage_boundary_tolerance_sec),
        cache=coverage_cache,
        cache_enabled=bool(args.coverage_cache_enabled),
        cache_stats=coverage_cache_stats,
        timezone_name=str(args.timezone),
        filename_date_hint_enabled=bool(args.filename_date_hint_enabled),
        filename_date_hint_padding_days=max(0, int(args.filename_date_hint_padding_days)),
        full_directory_coverage_scan=bool(args.full_directory_coverage_scan),
        filename_hint_summary=filename_hint_summary,
    )
    books_selection = (
        select_files_for_replay(
            books_input,
            kind="book",
            symbol=str(args.symbol),
            multiplier=float(multiplier),
            book_options=book_cleaning,
            requested_filter=requested_filter,
            auto_discover=bool(args.auto_discover_files),
            coverage_tolerance_sec=float(args.coverage_boundary_tolerance_sec),
            cache=coverage_cache,
            cache_enabled=bool(args.coverage_cache_enabled),
            cache_stats=coverage_cache_stats,
            timezone_name=str(args.timezone),
            filename_date_hint_enabled=bool(args.filename_date_hint_enabled),
            filename_date_hint_padding_days=max(0, int(args.filename_date_hint_padding_days)),
            full_directory_coverage_scan=bool(args.full_directory_coverage_scan),
            filename_hint_summary=filename_hint_summary,
        )
        if books_input
        else CoverageSelection(selected=[], all_coverage=[])
    )
    if args.coverage_cache_enabled:
        save_coverage_cache(args.coverage_cache_path, coverage_cache)
    effective_filter, time_alignment = resolve_effective_time_filter(
        requested_window=requested_window,
        trades_selection=trades_selection,
        books_selection=books_selection,
        require_full_coverage=bool(args.require_full_coverage and not args.allow_partial_coverage),
        allow_missing_books=bool(args.allow_missing_books and not books_input),
        coverage_tolerance_sec=float(args.coverage_boundary_tolerance_sec),
        coverage_cache_enabled=bool(args.coverage_cache_enabled),
        coverage_cache_path=Path(args.coverage_cache_path),
        coverage_cache_stats=coverage_cache_stats,
    )

    logger.info("[LOCAL-A1-RESEARCH-START] symbol=%s trades=%s books=%s out=%s", args.symbol, trades_input, books_input, out_dir)
    logger.info("[LOCAL-A1-RESEARCH-MODE] no_main_py=true no_websocket=true no_iceberg_trader=true no_execution=true")
    logger.info("[LOCAL-BOOK-CLEANING] %s", asdict(book_cleaning))
    logger.info("[LOCAL-TIME-FILTER] %s", effective_filter.to_dict())
    logger.info("[LOCAL-TIME-ALIGNMENT] %s", time_alignment)
    for warning in time_alignment["warnings"]:
        logger.warning("[LOCAL-TIME-ALIGNMENT-WARNING] %s", warning)

    runtime = LocalA1ResearchRuntime(symbol=str(args.symbol), research_events_path=research_events_path)
    stats = Stats()
    if books_selection.paths and effective_filter.start_ts is not None:
        warmup = warmup_books_state(
            runtime=runtime,
            book_files=books_selection.paths,
            symbol=str(args.symbol),
            multiplier=float(multiplier),
            options=book_cleaning,
            start_ts=float(effective_filter.start_ts),
        )
        time_alignment.update(warmup)
        if warmup["book_bootstrap_bid_levels"] or warmup["book_bootstrap_ask_levels"]:
            bootstrap_book = {
                "bids": sort_levels_from_state(runtime.ctx.bids, side="bids", depth_limit=book_cleaning.depth_limit),
                "asks": sort_levels_from_state(runtime.ctx.asks, side="asks", depth_limit=book_cleaning.depth_limit),
                "ts": float(effective_filter.start_ts),
                "recv_ts": float(effective_filter.start_ts),
            }
            stats.books += 1
            runtime.on_book_update(bootstrap_book, stats)
            stats.touch(float(effective_filter.start_ts))
            logger.info(
                "[LOCAL-BOOK-WARMUP] rows=%d bootstrap_bids=%d bootstrap_asks=%d ts=%s",
                warmup["book_warmup_rows"],
                warmup["book_bootstrap_bid_levels"],
                warmup["book_bootstrap_ask_levels"],
                fmt_utc(float(effective_filter.start_ts)),
            )
    else:
        time_alignment.update(
            {
                "book_warmup_rows": 0,
                "book_bootstrap_bid_levels": 0,
                "book_bootstrap_ask_levels": 0,
                "book_warmup_start_utc": "",
                "book_warmup_end_utc": fmt_utc(effective_filter.start_ts or 0.0),
                "book_warmup_last_row_utc": "",
            }
        )

    start = time.perf_counter()
    try:
        events = build_events(
            trades_selection.paths,
            books_selection.paths,
            str(args.symbol),
            float(multiplier),
            stats,
            bool(args.sort_in_memory),
            book_cleaning,
            effective_filter,
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
        "trades_path": str(trades_input),
        "books_path": str(books_input) if books_input else "",
        "book_cleaning": asdict(book_cleaning),
        "time_filter": effective_filter.to_dict(),
        "time_alignment": time_alignment,
        "filename_date_hint": filename_hint_summary,
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


def parse_requested_window(args: argparse.Namespace) -> RequestedWindow:
    tz = load_timezone(str(args.timezone))
    start_ts = parse_datetime_to_ts(args.start_time, tz) if args.start_time else None
    end_ts = parse_datetime_to_ts(args.end_time, tz) if args.end_time else None
    if start_ts is None and args.start_date:
        start_ts = date_to_start_ts(args.start_date, tz, str(args.date_mode))
    if end_ts is None and args.end_date:
        end_ts = date_to_next_day_start_ts(args.end_date, tz, str(args.date_mode))
    if start_ts is not None and end_ts is not None and start_ts >= end_ts:
        raise SystemExit(
            f"Invalid time range: start ({fmt_utc(start_ts)}) must be before exclusive end ({fmt_utc(end_ts)})"
        )
    return RequestedWindow(
        time_filter=TimeFilter(start_ts=start_ts, end_ts=end_ts),
        timezone_name=str(args.timezone),
        date_mode=str(args.date_mode),
        requested_start_local=fmt_local(start_ts or 0.0, tz),
        requested_end_local=fmt_local(end_ts or 0.0, tz),
        requested_start_utc=fmt_utc(start_ts or 0.0),
        requested_end_utc=fmt_utc(end_ts or 0.0),
    )


def load_timezone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise SystemExit(f"Invalid timezone: {timezone_name}") from exc


def parse_datetime_to_ts(text: str, default_tz: ZoneInfo) -> float:
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
        dt = dt.replace(tzinfo=default_tz)
    else:
        dt = dt.astimezone(timezone.utc)
    return float(dt.timestamp())


def date_to_start_ts(date_text: str, tz: ZoneInfo, date_mode: str) -> float:
    return parse_date_start(date_text, tz, date_mode).timestamp()


def date_to_next_day_start_ts(date_text: str, tz: ZoneInfo, date_mode: str) -> float:
    return (parse_date_start(date_text, tz, date_mode) + timedelta(days=1)).timestamp()


def parse_date_start(date_text: str, tz: ZoneInfo, date_mode: str) -> datetime:
    try:
        date_value = datetime.strptime(str(date_text).strip(), "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"Invalid date, expected YYYY-MM-DD: {date_text}") from exc
    return date_value.replace(tzinfo=timezone.utc if date_mode == "utc" else tz)


def resolve_effective_time_filter(
    requested_window: RequestedWindow,
    trades_selection: CoverageSelection,
    books_selection: CoverageSelection,
    require_full_coverage: bool,
    allow_missing_books: bool = False,
    coverage_tolerance_sec: float = 0.0,
    coverage_cache_enabled: bool = False,
    coverage_cache_path: Path | None = None,
    coverage_cache_stats: CoverageCacheStats | None = None,
) -> tuple[TimeFilter, dict[str, Any]]:
    requested = requested_window.time_filter
    missing: list[dict[str, Any]] = []
    trades_missing = missing_for_selection(trades_selection, requested, coverage_tolerance_sec)
    books_missing: list[tuple[float, float]] = []
    if not allow_missing_books:
        books_missing = missing_for_selection(books_selection, requested, coverage_tolerance_sec)
    for start_ts, end_ts in trades_missing:
        missing.append({"side": "trades", "start_utc": fmt_utc(start_ts), "end_utc": fmt_utc(end_ts)})
    for start_ts, end_ts in books_missing:
        missing.append({"side": "books", "start_utc": fmt_utc(start_ts), "end_utc": fmt_utc(end_ts)})

    full_coverage = not missing
    warnings: list[str] = []
    if require_full_coverage and not full_coverage:
        raise SystemExit(format_coverage_error(requested_window, trades_selection, books_selection, missing, coverage_tolerance_sec))

    effective_start = requested.start_ts
    effective_end = requested.end_ts
    if not full_coverage and not require_full_coverage:
        effective_filter = partial_effective_filter(
            requested,
            trades_selection,
            books_selection,
            coverage_tolerance_sec,
            allow_missing_books=allow_missing_books,
        )
        effective_start = effective_filter.start_ts
        effective_end = effective_filter.end_ts
        if effective_start is None or effective_end is None:
            raise SystemExit(format_coverage_error(requested_window, trades_selection, books_selection, missing, coverage_tolerance_sec))
        effective_missing = missing_for_selection(trades_selection, effective_filter, coverage_tolerance_sec)
        if not allow_missing_books:
            effective_missing += missing_for_selection(books_selection, effective_filter, coverage_tolerance_sec)
        if effective_start >= effective_end or effective_missing:
            internal_missing = [
                {"side": "partial", "start_utc": fmt_utc(start_ts), "end_utc": fmt_utc(end_ts)}
                for start_ts, end_ts in effective_missing
            ]
            raise SystemExit(
                format_coverage_error(
                    requested_window,
                    trades_selection,
                    books_selection,
                    internal_missing or missing,
                    coverage_tolerance_sec,
                )
            )
        if requested.start_ts is not None and effective_start > requested.start_ts:
            warnings.append(
                "partial coverage trimmed head: "
                f"{fmt_utc(requested.start_ts)} to {fmt_utc(effective_start)}"
            )
        if requested.end_ts is not None and effective_end < requested.end_ts:
            warnings.append(
                "partial coverage trimmed tail: "
                f"{fmt_utc(effective_end)} to {fmt_utc(requested.end_ts)}"
            )
        warnings.append(f"partial coverage effective window: {fmt_utc(effective_start)} to {fmt_utc(effective_end)}")

    tz = load_timezone(requested_window.timezone_name)
    alignment = {
        "timezone": requested_window.timezone_name,
        "date_mode": requested_window.date_mode,
        "requested_start_local": requested_window.requested_start_local,
        "requested_end_local": requested_window.requested_end_local,
        "requested_start_utc": requested_window.requested_start_utc,
        "requested_end_utc": requested_window.requested_end_utc,
        "effective_start_utc": fmt_utc(effective_start or 0.0),
        "effective_end_utc": fmt_utc(effective_end or 0.0),
        "effective_start_local": fmt_local(effective_start or 0.0, tz),
        "effective_end_local": fmt_local(effective_end or 0.0, tz),
        "trades_selected_files": [str(path) for path in trades_selection.paths],
        "books_selected_files": [str(path) for path in books_selection.paths],
        "trades_first_ts": trades_selection.first_ts,
        "trades_last_ts": trades_selection.last_ts,
        "books_first_ts": books_selection.first_ts,
        "books_last_ts": books_selection.last_ts,
        "trades_first_utc": fmt_utc(trades_selection.first_ts),
        "trades_last_utc": fmt_utc(trades_selection.last_ts),
        "books_first_utc": fmt_utc(books_selection.first_ts),
        "books_last_utc": fmt_utc(books_selection.last_ts),
        "coverage_boundary_tolerance_sec": float(coverage_tolerance_sec),
        "trades_coverage_rows": trades_selection.row_count,
        "books_coverage_rows": books_selection.row_count,
        "trades_malformed_ts_count": trades_selection.malformed_ts_count,
        "books_malformed_ts_count": books_selection.malformed_ts_count,
        "coverage_cache_enabled": bool(coverage_cache_enabled),
        "coverage_cache_path": str(coverage_cache_path) if coverage_cache_path else "",
        "coverage_cache_hits": coverage_cache_stats.hits if coverage_cache_stats else 0,
        "coverage_cache_misses": coverage_cache_stats.misses if coverage_cache_stats else 0,
        "full_coverage": full_coverage,
        "partial_coverage_used": bool(not full_coverage and not require_full_coverage),
        "warnings": warnings,
    }
    return TimeFilter(start_ts=effective_start, end_ts=effective_end), alignment


def coverage_intervals(coverage: Sequence[FileCoverage], tolerance_sec: float = 0.0) -> list[tuple[float, float]]:
    tolerance = max(0.0, float(tolerance_sec))
    intervals = sorted(
        (item.first_ts, item.last_ts)
        for item in coverage
        if item.first_ts > 0 and item.last_ts > 0
    )
    if not intervals:
        return []
    merged: list[tuple[float, float]] = []
    cur_start, cur_end = intervals[0]
    for start_ts, end_ts in intervals[1:]:
        if start_ts <= cur_end or start_ts - cur_end <= tolerance:
            cur_end = max(cur_end, end_ts)
        else:
            merged.append((cur_start, cur_end))
            cur_start, cur_end = start_ts, end_ts
    merged.append((cur_start, cur_end))
    return merged


def missing_intervals(
    time_filter: TimeFilter,
    intervals: Sequence[tuple[float, float]],
    tolerance_sec: float = 0.0,
) -> list[tuple[float, float]]:
    if time_filter.start_ts is None or time_filter.end_ts is None:
        return []
    tolerance = max(0.0, float(tolerance_sec))
    missing: list[tuple[float, float]] = []
    cursor = float(time_filter.start_ts)
    end = float(time_filter.end_ts)
    covered_started = False
    for start_ts, last_ts in intervals:
        if last_ts < cursor:
            continue
        if start_ts >= end:
            break
        if start_ts > cursor:
            if not covered_started and start_ts - cursor <= tolerance:
                pass
            else:
                missing.append((cursor, min(start_ts, end)))
        cursor = max(cursor, last_ts)
        covered_started = True
        if cursor >= end:
            return missing
    if cursor < end:
        if covered_started and end - cursor <= tolerance:
            return missing
        missing.append((cursor, end))
    return missing


def missing_for_selection(selection: CoverageSelection, time_filter: TimeFilter, tolerance_sec: float) -> list[tuple[float, float]]:
    return missing_intervals(time_filter, coverage_intervals(selection.selected, tolerance_sec), tolerance_sec)


def partial_effective_filter(
    requested: TimeFilter,
    trades_selection: CoverageSelection,
    books_selection: CoverageSelection,
    tolerance_sec: float,
    allow_missing_books: bool = False,
) -> TimeFilter:
    starts = [requested.start_ts, first_effective_coverage_ts(trades_selection, requested, tolerance_sec)]
    ends = [requested.end_ts, last_effective_coverage_ts(trades_selection, requested, tolerance_sec)]
    if not allow_missing_books:
        starts.append(first_effective_coverage_ts(books_selection, requested, tolerance_sec))
        ends.append(last_effective_coverage_ts(books_selection, requested, tolerance_sec))
    clean_starts = [value for value in starts if value is not None and value > 0]
    clean_ends = [value for value in ends if value is not None and value > 0]
    return TimeFilter(start_ts=max(clean_starts) if clean_starts else None, end_ts=min(clean_ends) if clean_ends else None)


def first_effective_coverage_ts(selection: CoverageSelection, requested: TimeFilter, tolerance_sec: float) -> float | None:
    if requested.start_ts is None or requested.end_ts is None:
        return selection.first_ts or None
    for start_ts, end_ts in coverage_intervals(selection.selected, tolerance_sec):
        if end_ts < requested.start_ts:
            continue
        if start_ts >= requested.end_ts:
            break
        return max(start_ts, requested.start_ts)
    return None


def last_effective_coverage_ts(selection: CoverageSelection, requested: TimeFilter, tolerance_sec: float) -> float | None:
    if requested.start_ts is None or requested.end_ts is None:
        return selection.last_ts or None
    last_value: float | None = None
    for start_ts, end_ts in coverage_intervals(selection.selected, tolerance_sec):
        if end_ts < requested.start_ts:
            continue
        if start_ts >= requested.end_ts:
            break
        last_value = min(end_ts, requested.end_ts)
    return last_value


def format_coverage_error(
    requested_window: RequestedWindow,
    trades_selection: CoverageSelection,
    books_selection: CoverageSelection,
    missing: Sequence[Mapping[str, Any]],
    coverage_tolerance_sec: float = 0.0,
) -> str:
    missing_text = "; ".join(
        f"{item.get('side')} missing {item.get('start_utc')} to {item.get('end_utc')}" for item in missing
    ) or "none"
    return "\n".join(
        [
            "Requested replay window is not fully covered by selected local files.",
            f"requested local window: {requested_window.requested_start_local} to {requested_window.requested_end_local}",
            f"requested UTC window: {requested_window.requested_start_utc} to {requested_window.requested_end_utc}",
            f"trades coverage: {format_selection_coverage(trades_selection)}",
            f"books coverage: {format_selection_coverage(books_selection)}",
            f"missing side and missing interval: {missing_text}",
            f"coverage boundary tolerance: {float(coverage_tolerance_sec)} sec",
            "Use --allow-partial-coverage to replay the intersection window.",
        ]
    )


def format_selection_coverage(selection: CoverageSelection) -> str:
    if not selection.selected:
        return "none"
    return f"{fmt_utc(selection.first_ts)} to {fmt_utc(selection.last_ts)} files={len(selection.selected)}"


def select_files_for_replay(
    input_path: Path,
    kind: str,
    symbol: str,
    multiplier: float,
    book_options: BookCleaningOptions,
    requested_filter: TimeFilter,
    auto_discover: bool,
    coverage_tolerance_sec: float = 0.0,
    cache: dict[str, Any] | None = None,
    cache_enabled: bool = False,
    cache_stats: CoverageCacheStats | None = None,
    timezone_name: str = "Asia/Shanghai",
    filename_date_hint_enabled: bool = True,
    filename_date_hint_padding_days: int = 2,
    full_directory_coverage_scan: bool = False,
    filename_hint_summary: dict[str, Any] | None = None,
) -> CoverageSelection:
    candidates = discover_supported_files(input_path)
    candidates_before = len(candidates)
    if not full_directory_coverage_scan and filename_date_hint_enabled:
        candidates = filter_candidates_by_filename_date_hint(
            candidates,
            requested_filter,
            timezone_name=timezone_name,
            padding_days=filename_date_hint_padding_days,
        )
    candidates_after = len(candidates)
    if filename_hint_summary is not None:
        prefix = "trades" if kind == "trade" else "books"
        filename_hint_summary[f"{prefix}_candidates_before"] = candidates_before
        filename_hint_summary[f"{prefix}_candidates_after"] = candidates_after
    log_info(
        "[LOCAL-FILENAME-HINT] kind=%s candidates_before=%d candidates_after=%d padding_days=%d full_scan=%s",
        kind,
        candidates_before,
        candidates_after,
        max(0, int(filename_date_hint_padding_days)),
        str(bool(full_directory_coverage_scan)).lower(),
    )
    coverage = [
        item
        for item in (
            get_file_coverage(
                path,
                kind=kind,
                symbol=symbol,
                multiplier=multiplier,
                book_options=book_options,
                cache=cache,
                cache_enabled=cache_enabled,
                cache_stats=cache_stats,
            )
            for path in candidates
        )
        if item is not None
    ]
    selected = [item for item in coverage if (item.overlaps(requested_filter, coverage_tolerance_sec) if auto_discover else True)]
    selected.sort(key=lambda item: (item.first_ts, str(item.path)))
    return CoverageSelection(selected=selected, all_coverage=coverage)


def filter_candidates_by_filename_date_hint(
    candidates: list[Path],
    requested_filter: TimeFilter,
    timezone_name: str,
    padding_days: int,
) -> list[Path]:
    if requested_filter.start_ts is None or requested_filter.end_ts is None:
        return candidates
    if requested_filter.start_ts >= requested_filter.end_ts:
        return candidates

    padding = max(0, int(padding_days))
    start_date = datetime.fromtimestamp(float(requested_filter.start_ts), timezone.utc).date()
    # end_ts is exclusive; subtract a tiny amount so an exact midnight end does not add an extra day.
    end_probe_ts = max(float(requested_filter.start_ts), float(requested_filter.end_ts) - 0.000001)
    end_date = datetime.fromtimestamp(end_probe_ts, timezone.utc).date()
    min_date = start_date - timedelta(days=padding)
    max_date = end_date + timedelta(days=padding)

    filtered: list[Path] = []
    for path in candidates:
        file_date = extract_date_from_name(path.name)
        if file_date is None:
            filtered.append(path)
            continue
        hint_date = file_date.date()
        if min_date <= hint_date <= max_date:
            filtered.append(path)

    if not filtered and candidates:
        log_warning(
            "[LOCAL-FILENAME-HINT-WARNING] candidates_after=0 fallback=full_candidates padding_days=%d timezone=%s requested_start=%s requested_end=%s",
            padding,
            timezone_name,
            fmt_utc(requested_filter.start_ts or 0.0),
            fmt_utc(requested_filter.end_ts or 0.0),
        )
        return candidates
    return filtered


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


def scan_file_coverage(
    path: Path,
    kind: str,
    symbol: str,
    multiplier: float,
    book_options: BookCleaningOptions,
) -> FileCoverage | None:
    scan_stats = Stats()
    first_ts = 0.0
    last_ts = 0.0
    row_count = 0
    malformed_ts_count = 0
    for row in iter_rows(path, scan_stats, TimeFilter()):
        if not row_matches_symbol(row, symbol):
            continue
        row_count += 1
        ts = extract_row_ts_only(row)
        if ts <= 0:
            malformed_ts_count += 1
            continue
        first_ts = ts if first_ts <= 0 else min(first_ts, ts)
        last_ts = max(last_ts, ts)
    if first_ts <= 0 or last_ts <= 0:
        return None
    return FileCoverage(path=path, first_ts=first_ts, last_ts=last_ts, row_count=row_count, malformed_ts_count=malformed_ts_count)


def extract_row_ts_only(row: Mapping[str, Any]) -> float:
    return normalize_ts(get_any(row, "ts", "timestamp", "time", "created_time", "0"))


def row_matches_symbol(row: Mapping[str, Any], symbol: str) -> bool:
    inst = get_any(row, "instId", "inst_id", "symbol", "instrument")
    return True if inst in (None, "") else str(inst) == str(symbol)


def get_file_coverage(
    path: Path,
    kind: str,
    symbol: str,
    multiplier: float,
    book_options: BookCleaningOptions,
    cache: dict[str, Any] | None = None,
    cache_enabled: bool = False,
    cache_stats: CoverageCacheStats | None = None,
) -> FileCoverage | None:
    if cache_enabled and cache is not None:
        cached = read_cached_coverage(path, kind, symbol, cache)
        if cached is not None:
            if cache_stats:
                cache_stats.hits += 1
            return cached
        if cache_stats:
            cache_stats.misses += 1

    coverage = scan_file_coverage(path, kind=kind, symbol=symbol, multiplier=multiplier, book_options=book_options)
    if cache_enabled and cache is not None and coverage is not None:
        write_cached_coverage(coverage, kind, symbol, cache)
    return coverage


def coverage_cache_key(path: Path, kind: str, symbol: str) -> str:
    absolute = str(path.resolve())
    return f"{kind}|{symbol}|{absolute}"


def file_fingerprint(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    return str(path.resolve()), int(stat.st_size), int(stat.st_mtime_ns)


def read_cached_coverage(path: Path, kind: str, symbol: str, cache: Mapping[str, Any]) -> FileCoverage | None:
    absolute, size, mtime_ns = file_fingerprint(path)
    entry = cache.get(coverage_cache_key(path, kind, symbol))
    if not isinstance(entry, Mapping):
        return None
    if (
        entry.get("path") != absolute
        or int(entry.get("size", -1)) != size
        or int(entry.get("mtime_ns", -1)) != mtime_ns
        or entry.get("kind") != kind
        or entry.get("symbol") != symbol
    ):
        return None
    first_ts = to_float(entry.get("first_ts"))
    last_ts = to_float(entry.get("last_ts"))
    if first_ts <= 0 or last_ts <= 0:
        return None
    return FileCoverage(
        path=path,
        first_ts=first_ts,
        last_ts=last_ts,
        row_count=int(entry.get("row_count", 0) or 0),
        malformed_ts_count=int(entry.get("malformed_ts_count", 0) or 0),
    )


def write_cached_coverage(coverage: FileCoverage, kind: str, symbol: str, cache: dict[str, Any]) -> None:
    absolute, size, mtime_ns = file_fingerprint(coverage.path)
    cache[coverage_cache_key(coverage.path, kind, symbol)] = {
        "path": absolute,
        "size": size,
        "mtime_ns": mtime_ns,
        "kind": kind,
        "symbol": symbol,
        "first_ts": coverage.first_ts,
        "last_ts": coverage.last_ts,
        "row_count": coverage.row_count,
        "malformed_ts_count": coverage.malformed_ts_count,
        "scanned_at_utc": fmt_utc(time.time()),
    }


def load_coverage_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log_warning("[LOCAL-COVERAGE-CACHE-WARNING] path=%s reason=load_failed:%s", path, exc)
        return {}
    if not isinstance(data, dict):
        log_warning("[LOCAL-COVERAGE-CACHE-WARNING] path=%s reason=invalid_root", path)
        return {}
    return data


def save_coverage_cache(path: Path, cache: Mapping[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    except Exception as exc:
        log_warning("[LOCAL-COVERAGE-CACHE-WARNING] path=%s reason=write_failed:%s", path, exc)


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
) -> Iterator[ReplayEvent]:
    trades = iter_trade_events(trade_files, symbol, multiplier, stats, time_filter)
    books = iter_book_events(book_files, symbol, multiplier, stats, book_cleaning, time_filter) if book_files else iter(())
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
) -> Iterator[ReplayEvent]:
    seq = 0
    cleaner = BookEventCleaner(options=options, stats=stats)
    raw_books = merge_many_raw_books(
        iter_normalized_book_rows_from_file(path, symbol, multiplier, stats, options, time_filter, assume_sorted=assume_sorted)
        for path in paths
    )
    for raw_book in raw_books:
        try:
            stats.raw_book_rows += 1
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


def fmt_local(ts: float, tz: ZoneInfo) -> str:
    if not ts or ts <= 0:
        return ""
    return datetime.fromtimestamp(float(ts), tz).isoformat(timespec="seconds")


if __name__ == "__main__":
    raise SystemExit(main())
