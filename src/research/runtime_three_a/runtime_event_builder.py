#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build V7.3 runtime events from OKX raw trade files."""

from __future__ import annotations

import argparse
import csv
import gzip
import heapq
import io
import json
import os
import resource
import shutil
import sys
import tarfile
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.research.a1_edge.io_utils import ensure_dir, write_json
from src.research.a1_edge.schema import parse_float, parse_timestamp
from src.research.runtime_three_a.runtime_event_source import RuntimeEventSource


RUNTIME_EVENT_FIELDS = [
    "ts",
    "symbol",
    "last_price",
    "active_buy_notional_3s",
    "active_sell_notional_3s",
    "cvd_delta_3s",
    "price_velocity_u_per_sec",
    "condition_available_ts",
    "condition_source",
    "contract_multiplier",
    "notional_mode",
]

MAX_JSON_LINE_BYTES = 16 * 1024 * 1024
MAX_JSON_LINE_CHARS = MAX_JSON_LINE_BYTES
MAX_JSON_FILE_BYTES = 64 * 1024 * 1024
JSON_FILE_DISABLED_ERROR = (
    "Raw .json files are disabled by default because json.load is not memory-safe. "
    "Use .jsonl/.data or pass --allow-json-file for small files."
)
GZ_JSON_DISABLED_ERROR = (
    "Compressed .json.gz is disabled because uncompressed json.load size is not memory-safe. "
    "Convert to .jsonl/.data."
)
NON_MONOTONIC_ERROR = "Non-monotonic trades detected. Re-run with --merge-sort-files or verify source file ordering."
NON_MONOTONIC_UNSAFE_WARNING = "runtime_events may be non-monotonic and unsafe for windowed backtest"
NO_TRADE_FILES_ERROR = (
    "No supported trade files found. Expected .data/.jsonl/.ndjson/.csv or OKX tar.gz containing those formats."
)
NO_VALID_TRADES_ERROR = (
    "No valid trades were converted into runtime_events. "
    "Check symbol, input format, contract multiplier, and filters."
)
SKIPPED_JSON_WARNING = "raw .json files were skipped; use .jsonl/.data or --allow-json-file for small files"
SKIPPED_METADATA_JSON_WARNING = (
    "metadata-like .json files were skipped; use --include-metadata-json to force"
)
SKIPPED_METADATA_JSON_GZ_WARNING = (
    "metadata-like .json.gz files were skipped; use --include-metadata-json to force"
)
RUNTIME_EVENTS_SCHEMA_VERSION = "v7.3.runtime_events.1"


RuntimeEventFields = RUNTIME_EVENT_FIELDS


@dataclass(frozen=True)
class NormalizedTrade:
    ts: float
    symbol: str
    price: float
    side: str
    notional: float
    contract_multiplier: float = 1.0


@dataclass
class RuntimeReadStats:
    """Mutable stats collected while reading trade files (e.g. inside archives)."""
    skipped_json_file_count: int = 0
    raw_rows_seen: int = 0


@dataclass
class BuildStats:
    symbol: str = ""
    contract_multiplier: float = 0.0
    notional_mode: str = ""
    bucket_sec: float = 0.0
    rolling_sec: float = 0.0
    merge_sort_files: bool = False
    allow_non_monotonic_output: bool = False
    max_json_line_bytes: int = MAX_JSON_LINE_BYTES
    allow_json_file: bool = False
    max_json_file_bytes: int = MAX_JSON_FILE_BYTES
    overwrite: bool = False
    output_mode: str = "single_file"
    shard_count: int = 0
    shard_files: list[str] | None = None
    manifest_path: str = ""
    output_warning: str = ""
    non_monotonic_trade_count: int = 0
    raw_rows_seen: int = 0
    normalized_trades_emitted: int = 0
    total_trades_read: int = 0  # deprecated alias; prefer normalized_trades_emitted
    runtime_events_written: int = 0
    first_ts: float = 0.0
    last_ts: float = 0.0
    peak_rss_mb: float = 0.0
    skipped_json_file_count: int = 0
    allow_empty: bool = False
    empty_output_warning: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "contract_multiplier": self.contract_multiplier,
            "notional_mode": self.notional_mode,
            "bucket_sec": self.bucket_sec,
            "rolling_sec": self.rolling_sec,
            "merge_sort_files": self.merge_sort_files,
            "allow_non_monotonic_output": self.allow_non_monotonic_output,
            "max_json_line_bytes": self.max_json_line_bytes,
            "allow_json_file": self.allow_json_file,
            "max_json_file_bytes": self.max_json_file_bytes,
            "overwrite": self.overwrite,
            "output_mode": self.output_mode,
            "shard_count": self.shard_count,
            "shard_files": list(self.shard_files or []),
            "manifest_path": self.manifest_path,
            "output_warning": self.output_warning,
            "non_monotonic_trade_count": self.non_monotonic_trade_count,
            "raw_rows_seen": self.raw_rows_seen,
            "normalized_trades_emitted": self.normalized_trades_emitted,
            "total_trades_read": self.total_trades_read,
            "runtime_events_written": self.runtime_events_written,
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
            "peak_rss_mb": self.peak_rss_mb,
            "skipped_json_file_count": self.skipped_json_file_count,
            "allow_empty": self.allow_empty,
            "empty_output_warning": self.empty_output_warning,
        }


RuntimeEventBuildStats = BuildStats


@dataclass
class RuntimeEventDayStats:
    day: str
    path: str = ""
    first_ts: float = 0.0
    last_ts: float = 0.0
    row_count: int = 0

    def touch(self, ts: float) -> None:
        if ts <= 0:
            return
        self.first_ts = ts if self.first_ts <= 0 else min(self.first_ts, ts)
        self.last_ts = max(self.last_ts, ts)
        self.row_count += 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "day": self.day,
            "path": self.path,
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
            "row_count": self.row_count,
        }


class RuntimeEventAccumulator:
    def __init__(
        self,
        symbol: str,
        bucket_sec: float,
        rolling_sec: float,
        contract_multiplier: float,
        notional_mode: str = "price_x_size_x_contract_multiplier",
    ):
        self.symbol = str(symbol)
        self.bucket_sec = max(float(bucket_sec), 0.001)
        self.rolling_sec = max(float(rolling_sec), 0.001)
        self.contract_multiplier = float(contract_multiplier)
        self.notional_mode = str(notional_mode)
        self.rolling: deque[NormalizedTrade] = deque()
        self.price_window: deque[tuple[float, float]] = deque()
        self.current_bucket: float | None = None
        self.pending_event: dict[str, Any] | None = None
        self.prev_trade: NormalizedTrade | None = None
        self.prev_ts: float | None = None
        self.rolling_buy_sum = 0.0
        self.rolling_sell_sum = 0.0
        self.rolling_signed_sum = 0.0

    def update_trade(
        self,
        ts: float,
        price: float,
        side: str,
        size: float | None = None,
        notional: float | None = None,
    ) -> list[dict[str, Any]]:
        ts = float(ts)
        price = float(price)
        side_text = str(side or "").strip().upper()
        if side_text in {"BID", "B", "BUYER"}:
            side_text = "BUY"
        elif side_text in {"ASK", "S", "SELLER"}:
            side_text = "SELL"
        event_notional = float(notional or 0.0)
        if event_notional <= 0 and size is not None:
            event_notional = price * float(size) * self.contract_multiplier
        if ts <= 0 or price <= 0 or event_notional <= 0 or side_text not in {"BUY", "SELL"}:
            return []
        if self.prev_ts is not None and ts < self.prev_ts:
            raise ValueError("non-monotonic trade timestamp")

        trade = NormalizedTrade(
            ts=ts,
            symbol=self.symbol,
            price=price,
            side=side_text,
            notional=event_notional,
            contract_multiplier=self.contract_multiplier,
        )
        emitted: list[dict[str, Any]] = []
        bucket = _bucket_start(trade.ts, self.bucket_sec)
        if self.current_bucket is not None and bucket != self.current_bucket and self.pending_event is not None:
            emitted.append(self.pending_event)
            self.pending_event = None
        self.current_bucket = bucket

        if trade.side == "BUY":
            self.rolling_buy_sum += trade.notional
            self.rolling_signed_sum += trade.notional
        else:
            self.rolling_sell_sum += trade.notional
            self.rolling_signed_sum -= trade.notional
        self.rolling.append(trade)
        self.price_window.append((trade.ts, trade.price))
        self.rolling_buy_sum, self.rolling_sell_sum, self.rolling_signed_sum = _trim_rolling_and_update_sums(
            self.rolling,
            trade.ts,
            self.rolling_sec,
            self.rolling_buy_sum,
            self.rolling_sell_sum,
            self.rolling_signed_sum,
        )
        _trim_price_window(self.price_window, trade.ts, self.rolling_sec)
        self.pending_event = runtime_event_from_trade(
            trade,
            price_window=self.price_window,
            prev_trade=self.prev_trade,
            rolling_sec=self.rolling_sec,
            rolling_buy_sum=self.rolling_buy_sum,
            rolling_sell_sum=self.rolling_sell_sum,
            rolling_signed_sum=self.rolling_signed_sum,
        )
        self.pending_event["condition_source"] = "runtime_event_accumulator"
        self.pending_event["notional_mode"] = self.notional_mode
        self.prev_trade = trade
        self.prev_ts = trade.ts
        return emitted

    def flush(self) -> list[dict[str, Any]]:
        if self.pending_event is None:
            return []
        event = self.pending_event
        self.pending_event = None
        return [event]


class RuntimeEventDailyCacheWriter:
    def __init__(self, path: Path, *, overwrite: bool = False) -> None:
        self.path = Path(path)
        self.tmp_path = self.path.with_name(f"{self.path.name}.tmp")
        self.lock_path = self.path.with_name(f"{self.path.name}.lock")
        self.overwrite = bool(overwrite)
        self.stats = RuntimeEventDayStats(day=self.path.stem, path=self.path.name)
        ensure_dir(self.path.parent)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            self._lock_fd = os.open(self.lock_path, flags)
        except FileExistsError as exc:
            raise RuntimeError(f"runtime_events day shard is locked: {self.lock_path}") from exc
        if self.path.exists() and not self.overwrite:
            self._release_lock()
            raise RuntimeError(f"runtime_events day shard already exists: {self.path}")
        if self.tmp_path.exists():
            self.tmp_path.unlink()
        self._handle = self.tmp_path.open("w", encoding="utf-8")
        self._committed = False

    def write(self, row: Mapping[str, Any]) -> None:
        self._handle.write(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n")
        self.stats.touch(float(row.get("ts") or 0.0))

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.close()

    def commit(self) -> RuntimeEventDayStats:
        self.close()
        self.tmp_path.replace(self.path)
        self._committed = True
        self._release_lock()
        return self.stats

    def cleanup(self) -> None:
        self.close()
        if self.tmp_path.exists():
            self.tmp_path.unlink()
        self._release_lock()

    def _release_lock(self) -> None:
        fd = getattr(self, "_lock_fd", None)
        if fd is not None:
            os.close(fd)
            self._lock_fd = None
        if self.lock_path.exists():
            self.lock_path.unlink()

    def __enter__(self) -> "RuntimeEventDailyCacheWriter":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if exc_type is not None or not self._committed:
            self.cleanup()


RuntimeEventDailyShardWriter = RuntimeEventDailyCacheWriter


class RuntimeEventCacheManager:
    def __init__(
        self,
        cache_root: Path,
        symbol: str,
        bucket_sec: float,
        rolling_sec: float,
        contract_multiplier: float,
        notional_mode: str = "price_x_size_x_contract_multiplier",
        schema_version: str = RUNTIME_EVENTS_SCHEMA_VERSION,
        overwrite: bool = False,
    ):
        self.cache_root = Path(cache_root)
        self.symbol = str(symbol)
        self.bucket_sec = max(float(bucket_sec), 0.001)
        self.rolling_sec = max(float(rolling_sec), 0.001)
        self.contract_multiplier = float(contract_multiplier)
        self.notional_mode = str(notional_mode)
        self.schema_version = str(schema_version)
        self.overwrite = bool(overwrite)

    def cache_dir(self) -> Path:
        return self.cache_root / self.symbol / runtime_events_param_key(
            bucket_sec=self.bucket_sec,
            rolling_sec=self.rolling_sec,
            contract_multiplier=self.contract_multiplier,
            notional_mode=self.notional_mode,
            schema_version=self.schema_version,
        )

    def expected_day_path(self, day: str) -> Path:
        return self.cache_dir() / f"{day}.jsonl"

    @property
    def manifest_path(self) -> Path:
        return self.cache_dir() / "runtime_events_manifest.json"

    @property
    def manifest_lock_path(self) -> Path:
        return self.cache_dir() / "runtime_events_manifest.lock"

    @property
    def cache_meta_path(self) -> Path:
        return self.cache_dir() / "runtime_events_cache_meta.json"

    def load_manifest(self) -> dict[str, Any]:
        if not self.manifest_path.exists():
            return self._empty_manifest()
        try:
            data = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return self._empty_manifest()
        return data if isinstance(data, dict) else self._empty_manifest()

    def has_valid_day(self, day: str) -> bool:
        if self.overwrite:
            return False
        path = self.expected_day_path(day)
        if not path.exists():
            return False
        shard = self._manifest_shards_by_day().get(day)
        if not shard:
            return False
        return self._shard_matches(shard, day) and int(parse_float(shard.get("row_count"))) > 0

    def begin_day_writer(self, day: str) -> RuntimeEventDailyCacheWriter:
        return RuntimeEventDailyCacheWriter(self.expected_day_path(day), overwrite=True)

    def finalize_day(self, day: str, stats: RuntimeEventDayStats | Mapping[str, Any]) -> None:
        ensure_dir(self.cache_dir())
        lock_fd = self._acquire_manifest_lock()
        try:
            shard = dict(stats.as_dict() if hasattr(stats, "as_dict") else stats)
            shard.update(
                {
                    "path": f"{day}.jsonl",
                    "day": day,
                    "bucket_sec": self.bucket_sec,
                    "rolling_sec": self.rolling_sec,
                    "contract_multiplier": self.contract_multiplier,
                    "notional_mode": self.notional_mode,
                    "schema_version": self.schema_version,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            manifest = self.load_manifest()
            shards = [
                item for item in (manifest.get("shards") or [])
                if isinstance(item, Mapping) and str(item.get("day") or Path(str(item.get("path") or "")).stem) != day
            ]
            shards.append(shard)
            manifest.update(self._manifest_header())
            manifest["shards"] = sorted(shards, key=lambda item: str(item.get("day") or item.get("path") or ""))
            _atomic_write_json(self.manifest_path, manifest)
            _atomic_write_json(self.cache_meta_path, {**self._manifest_header(), "updated_at": datetime.now(timezone.utc).isoformat()})
        finally:
            self._release_manifest_lock(lock_fd)

    def selected_source(self, days: list[str]) -> RuntimeEventSource | None:
        if not days:
            return None
        if not all(self.has_valid_day(day) for day in days):
            return None
        return RuntimeEventSource(self.cache_dir())

    def write_run_ref(
        self,
        run_dir: Path,
        selected_days: list[str],
        cache_hit_days: list[str],
        cache_miss_days: list[str],
        generated_days: list[str],
        runtime_events_used_by_zone_truth: bool = False,
    ) -> dict[str, Any]:
        cache_dir = self.cache_dir()
        size_mb = estimated_cache_size_mb(cache_dir)
        payload = {
            "cache_dir": str(cache_dir),
            "selected_days": list(selected_days),
            "bucket_sec": self.bucket_sec,
            "rolling_sec": self.rolling_sec,
            "contract_multiplier": self.contract_multiplier,
            "notional_mode": self.notional_mode,
            "schema_version": self.schema_version,
            "cache_hit_days": list(cache_hit_days),
            "cache_miss_days": list(cache_miss_days),
            "generated_days": list(generated_days),
            "reused_days": list(cache_hit_days),
            "estimated_cache_size_mb": size_mb,
            "runtime_events_used_by_zone_truth": bool(runtime_events_used_by_zone_truth),
        }
        ensure_dir(Path(run_dir))
        _atomic_write_json(Path(run_dir) / "runtime_events_ref.json", payload)
        return payload

    def _manifest_shards_by_day(self) -> dict[str, Mapping[str, Any]]:
        out: dict[str, Mapping[str, Any]] = {}
        for shard in self.load_manifest().get("shards") or []:
            if not isinstance(shard, Mapping):
                continue
            day = str(shard.get("day") or Path(str(shard.get("path") or "")).stem)
            if day:
                out[day] = shard
        return out

    def _shard_matches(self, shard: Mapping[str, Any], day: str) -> bool:
        return (
            str(shard.get("path") or "") == f"{day}.jsonl"
            and float(parse_float(shard.get("bucket_sec"))) == self.bucket_sec
            and float(parse_float(shard.get("rolling_sec"))) == self.rolling_sec
            and float(parse_float(shard.get("contract_multiplier"))) == self.contract_multiplier
            and str(shard.get("notional_mode") or "") == self.notional_mode
            and str(shard.get("schema_version") or "") == self.schema_version
        )

    def _manifest_header(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "bucket_sec": self.bucket_sec,
            "rolling_sec": self.rolling_sec,
            "contract_multiplier": self.contract_multiplier,
            "notional_mode": self.notional_mode,
            "schema_version": self.schema_version,
            "output_mode": "daily_cache",
        }

    def _empty_manifest(self) -> dict[str, Any]:
        return {**self._manifest_header(), "shards": []}

    def _acquire_manifest_lock(self) -> int:
        try:
            return os.open(self.manifest_lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise RuntimeError("runtime_events manifest is locked") from exc

    def _release_manifest_lock(self, lock_fd: int) -> None:
        os.close(lock_fd)
        if self.manifest_lock_path.exists():
            self.manifest_lock_path.unlink()


def runtime_events_param_key(
    *,
    bucket_sec: float,
    rolling_sec: float,
    contract_multiplier: float,
    notional_mode: str = "price_x_size_x_contract_multiplier",
    schema_version: str = RUNTIME_EVENTS_SCHEMA_VERSION,
) -> str:
    return (
        f"bucket_{_safe_number(bucket_sec)}s"
        f"_rolling_{_safe_number(rolling_sec)}s"
        f"_cm_{_safe_number(contract_multiplier)}"
        f"_nm_{_safe_token(notional_mode)}"
        f"_sv_{_safe_token(schema_version)}"
    )


def estimated_cache_size_mb(path: Path) -> float:
    if not path.exists():
        return 0.0
    total = 0
    for child in path.rglob("*.jsonl"):
        if child.is_file():
            total += child.stat().st_size
    return round(total / (1024.0 * 1024.0), 6)


def _safe_number(value: float) -> str:
    text = f"{float(value):.12g}"
    if text.endswith(".0"):
        text = text[:-2]
    return text.replace("-", "m").replace(".", "p")


def _safe_token(value: str) -> str:
    text = str(value)
    chars: list[str] = []
    for idx, ch in enumerate(text):
        if ch == "." and idx > 0 and idx + 1 < len(text) and text[idx - 1].isdigit() and text[idx + 1].isdigit():
            chars.append("p")
        elif ch.isalnum():
            chars.append(ch)
        else:
            chars.append("_")
    return "".join(chars).strip("_").lower()


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    ensure_dir(path.parent)
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build runtime_events.jsonl from OKX raw trades.")
    parser.add_argument("--symbol", required=True, help="OKX instrument id, e.g. ETH-USDT-SWAP.")
    parser.add_argument("--trades-dir", required=True, help="Directory or file containing raw OKX trades.")
    output = parser.add_mutually_exclusive_group(required=False)
    output.add_argument("--out", help="Output runtime_events JSONL path. Not recommended for long samples; prefer --out-dir --shard-by day.")
    output.add_argument("--out-dir", help="Cache root override. Default: data/derived/runtime_events.")
    parser.add_argument("--shard-by", choices=["day"], default="day", help="Shard runtime events by UTC day.")
    parser.add_argument("--bucket-sec", type=float, default=1.0)
    parser.add_argument("--rolling-sec", type=float, default=3.0)
    parser.add_argument("--contract-multiplier", type=float, default=None, help="Multiplier applied to raw size before notional = price * size. Required for SWAP symbols.")
    parser.add_argument("--merge-sort-files", action="store_true", help="Heap-merge all files by timestamp. Default is sequential to avoid opening many files.")
    parser.add_argument("--allow-non-monotonic-output", action="store_true", help="Allow non-monotonic runtime events with a strong warning. Unsafe for windowed backtests.")
    parser.add_argument("--max-json-line-bytes", type=int, default=MAX_JSON_LINE_BYTES, help="Maximum bytes per JSONL/.data/.ndjson line.")
    parser.add_argument("--allow-json-file", action="store_true", help="Allow small raw .json files that require json.load. Disabled by default.")
    parser.add_argument("--max-json-file-bytes", type=int, default=MAX_JSON_FILE_BYTES, help="Maximum bytes for --allow-json-file inputs.")
    parser.add_argument("--overwrite", action="store_true", help="Replace an existing output file/directory after a successful build.")
    parser.add_argument("--allow-empty", action="store_true", help="Allow empty output when no trade files are found or no trades match.")
    parser.add_argument("--include-metadata-json", action="store_true", help="Include metadata-like .json files (summary, manifest, metadata, meta, stats) when --allow-json-file is set.")
    parser.add_argument("--summary-out", help="Optional summary JSON path. Default: <out>.summary.json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    multiplier = _resolve_contract_multiplier(args.symbol, args.contract_multiplier)
    out_path = Path(args.out) if args.out else None
    out_dir = None
    if out_path is None:
        cache_root = Path(args.out_dir) if args.out_dir else ROOT / "data" / "derived" / "runtime_events"
        manager = RuntimeEventCacheManager(
            cache_root=cache_root,
            symbol=args.symbol,
            bucket_sec=float(args.bucket_sec),
            rolling_sec=float(args.rolling_sec),
            contract_multiplier=multiplier,
            notional_mode="price_x_size_x_contract_multiplier",
            overwrite=bool(args.overwrite),
        )
        out_dir = manager.cache_dir()
    elif args.out_dir:
        out_dir = Path(args.out_dir)
    summary = build_runtime_events(
        symbol=args.symbol,
        trades_path=Path(args.trades_dir),
        out_path=out_path,
        out_dir=out_dir,
        shard_by="day" if out_dir is not None else args.shard_by,
        bucket_sec=float(args.bucket_sec),
        rolling_sec=float(args.rolling_sec),
        contract_multiplier=multiplier,
        merge_sort_files=bool(args.merge_sort_files),
        allow_non_monotonic_output=bool(args.allow_non_monotonic_output),
        max_json_line_bytes=int(args.max_json_line_bytes),
        allow_json_file=bool(args.allow_json_file),
        max_json_file_bytes=int(args.max_json_file_bytes),
        overwrite=bool(args.overwrite),
        allow_empty=bool(args.allow_empty),
        include_metadata_json=bool(args.include_metadata_json),
    )
    summary_path = Path(args.summary_out) if args.summary_out else Path(str(out_path or out_dir) + ".summary.json")
    write_json(summary_path, summary)
    print(
        "[BUILD-RUNTIME-EVENTS] "
        f"total_trades_read={summary['total_trades_read']} "
        f"runtime_events_written={summary['runtime_events_written']} "
        f"first_ts={summary['first_ts']} "
        f"last_ts={summary['last_ts']} "
        f"peak_rss_mb={summary['peak_rss_mb']} "
        f"output_mode={summary['output_mode']} "
        f"shard_count={summary['shard_count']} "
        f"out={out_path or out_dir}"
    )
    return 0


def build_runtime_events(
    *,
    symbol: str,
    trades_path: Path,
    out_path: Path | None = None,
    out_dir: Path | None = None,
    shard_by: str | None = None,
    bucket_sec: float = 1.0,
    rolling_sec: float = 3.0,
    contract_multiplier: float | None = None,
    merge_sort_files: bool = False,
    allow_non_monotonic_output: bool = False,
    max_json_line_bytes: int = MAX_JSON_LINE_BYTES,
    allow_json_file: bool = False,
    max_json_file_bytes: int = MAX_JSON_FILE_BYTES,
    overwrite: bool = False,
    allow_empty: bool = False,
    include_metadata_json: bool = False,
) -> dict[str, Any]:
    if out_path is not None and out_dir is not None:
        raise SystemExit("--out and --out-dir are mutually exclusive")
    if out_path is None and out_dir is None:
        raise SystemExit("one of --out or --out-dir is required")
    if out_dir is not None and shard_by != "day":
        raise SystemExit("--out-dir requires --shard-by day")
    resolved_multiplier = _resolve_contract_multiplier(symbol, contract_multiplier)
    bucket_sec = max(float(bucket_sec), 0.001)
    rolling_sec = max(float(rolling_sec), 0.001)
    stats = BuildStats(
        symbol=symbol,
        contract_multiplier=float(resolved_multiplier),
        notional_mode="price_x_size_x_contract_multiplier",
        bucket_sec=bucket_sec,
        rolling_sec=rolling_sec,
        merge_sort_files=bool(merge_sort_files),
        allow_non_monotonic_output=bool(allow_non_monotonic_output),
        max_json_line_bytes=int(max_json_line_bytes),
        allow_json_file=bool(allow_json_file),
        max_json_file_bytes=int(max_json_file_bytes),
        overwrite=bool(overwrite),
        output_mode="sharded_by_day" if out_dir is not None else "single_file",
        shard_files=[],
        output_warning="single large runtime_events file is not recommended for long samples; use --out-dir --shard-by day" if out_path is not None else "",
        allow_empty=bool(allow_empty),
    )
    writer: _RuntimeEventWriter
    if out_dir is not None:
        writer = _ShardedRuntimeEventWriter(out_dir, overwrite=overwrite)
    else:
        writer = _SingleRuntimeEventWriter(out_path, overwrite=overwrite)  # type: ignore[arg-type]
    accumulator = RuntimeEventAccumulator(
        symbol=symbol,
        bucket_sec=bucket_sec,
        rolling_sec=rolling_sec,
        contract_multiplier=resolved_multiplier,
        notional_mode=stats.notional_mode,
    )
    prev_ts: float | None = None
    success = False

    try:
        trade_files, skipped_json = discover_trade_files(
            trades_path,
            allow_json_file=allow_json_file,
            include_metadata_json=include_metadata_json,
        )
        stats.skipped_json_file_count = skipped_json
        if skipped_json > 0:
            stats.output_warning = _join_warning(stats.output_warning, SKIPPED_JSON_WARNING)

        if not trade_files:
            if allow_empty:
                stats.empty_output_warning = "no supported trade files found; output will be empty"
                stats.output_warning = _join_warning(stats.output_warning, stats.empty_output_warning)
                writer.close()
                if isinstance(writer, _ShardedRuntimeEventWriter):
                    stats.manifest_path = str(writer.manifest_path)
                    writer.write_manifest(
                        symbol=symbol,
                        bucket_sec=bucket_sec,
                        rolling_sec=rolling_sec,
                        contract_multiplier=resolved_multiplier,
                        notional_mode=stats.notional_mode,
                        output_mode=stats.output_mode,
                    )
                writer.commit()
                success = True
                return stats.as_dict()
            raise SystemExit(NO_TRADE_FILES_ERROR)

        trade_iter_kwargs: dict[str, Any] = {
            "symbol": symbol,
            "contract_multiplier": resolved_multiplier,
            "merge_sort_files": merge_sort_files,
            "max_json_line_bytes": int(max_json_line_bytes),
        }
        if allow_json_file:
            trade_iter_kwargs["allow_json_file"] = True
            trade_iter_kwargs["max_json_file_bytes"] = int(max_json_file_bytes)
        trade_iter_kwargs["include_metadata_json"] = bool(include_metadata_json)
        read_stats = RuntimeReadStats()
        trade_iter_kwargs["read_stats"] = read_stats
        for trade in iter_normalized_trades(
            trade_files,
            **trade_iter_kwargs,
        ):
            stats.total_trades_read += 1
            stats.normalized_trades_emitted += 1
            if prev_ts is not None and trade.ts < prev_ts:
                stats.non_monotonic_trade_count += 1
                if not allow_non_monotonic_output:
                    raise SystemExit(NON_MONOTONIC_ERROR)
                if "non-monotonic trades detected" not in stats.output_warning:
                    stats.output_warning = _join_warning(
                        stats.output_warning,
                        "non-monotonic trades detected; consider --merge-sort-files",
                    )
                if NON_MONOTONIC_UNSAFE_WARNING not in stats.output_warning:
                    stats.output_warning = _join_warning(stats.output_warning, NON_MONOTONIC_UNSAFE_WARNING)
                for event in accumulator.flush():
                    event["condition_source"] = "okx_raw_trades_builder"
                    writer.write(event)
                    stats.runtime_events_written += 1
                accumulator = RuntimeEventAccumulator(
                    symbol=symbol,
                    bucket_sec=bucket_sec,
                    rolling_sec=rolling_sec,
                    contract_multiplier=resolved_multiplier,
                    notional_mode=stats.notional_mode,
                )
            stats.first_ts = trade.ts if stats.first_ts <= 0 else min(stats.first_ts, trade.ts)
            stats.last_ts = max(stats.last_ts, trade.ts)
            for event in accumulator.update_trade(
                trade.ts,
                trade.price,
                trade.side,
                notional=trade.notional,
            ):
                event["condition_source"] = "okx_raw_trades_builder"
                writer.write(event)
                stats.runtime_events_written += 1
            prev_ts = trade.ts

        for event in accumulator.flush():
            event["condition_source"] = "okx_raw_trades_builder"
            writer.write(event)
            stats.runtime_events_written += 1
        writer.close()

        stats.skipped_json_file_count += read_stats.skipped_json_file_count
        stats.raw_rows_seen = read_stats.raw_rows_seen
        if read_stats.skipped_json_file_count > 0:
            stats.output_warning = _join_warning(stats.output_warning, SKIPPED_JSON_WARNING)

        if stats.total_trades_read == 0 or stats.runtime_events_written == 0:
            if allow_empty:
                stats.empty_output_warning = "no valid trades were converted into runtime_events"
                stats.output_warning = _join_warning(stats.output_warning, stats.empty_output_warning)
                if isinstance(writer, _ShardedRuntimeEventWriter):
                    stats.manifest_path = str(writer.manifest_path)
                    writer.write_manifest(
                        symbol=symbol,
                        bucket_sec=bucket_sec,
                        rolling_sec=rolling_sec,
                        contract_multiplier=resolved_multiplier,
                        notional_mode=stats.notional_mode,
                        output_mode=stats.output_mode,
                    )
                writer.commit()
                success = True
                return stats.as_dict()
            writer.close()
            writer.cleanup()
            raise SystemExit(NO_VALID_TRADES_ERROR)

        stats.peak_rss_mb = _peak_rss_mb()
        stats.shard_files = writer.files
        stats.shard_count = len(writer.files)
        if isinstance(writer, _ShardedRuntimeEventWriter):
            stats.manifest_path = str(writer.manifest_path)
            writer.write_manifest(
                symbol=symbol,
                bucket_sec=bucket_sec,
                rolling_sec=rolling_sec,
                contract_multiplier=resolved_multiplier,
                notional_mode=stats.notional_mode,
                output_mode=stats.output_mode,
            )
        writer.commit()
        success = True
        return stats.as_dict()
    finally:
        if not success:
            writer.close()
            writer.cleanup()


def runtime_event_from_trade(
    trade: NormalizedTrade,
    *,
    price_window: deque[tuple[float, float]],
    prev_trade: NormalizedTrade | None,
    rolling_sec: float,
    rolling_buy_sum: float = 0.0,
    rolling_sell_sum: float = 0.0,
    rolling_signed_sum: float = 0.0,
) -> dict[str, Any]:
    velocity = _price_velocity(trade, price_window, prev_trade, rolling_sec)
    return {
        "ts": round(trade.ts, 8),
        "symbol": trade.symbol,
        "last_price": round(trade.price, 8),
        "active_buy_notional_3s": round(rolling_buy_sum, 8),
        "active_sell_notional_3s": round(rolling_sell_sum, 8),
        "cvd_delta_3s": round(rolling_signed_sum, 8),
        "price_velocity_u_per_sec": round(velocity, 8),
        "condition_available_ts": round(trade.ts, 8),
        "condition_source": "okx_raw_trades_builder",
        "contract_multiplier": round(float(trade.contract_multiplier), 8),
        "notional_mode": "price_x_size_x_contract_multiplier",
    }


class _RuntimeEventWriter:
    @property
    def files(self) -> list[str]:
        raise NotImplementedError

    def write(self, row: Mapping[str, Any]) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    def commit(self) -> None:
        raise NotImplementedError

    def cleanup(self) -> None:
        raise NotImplementedError


class _SingleRuntimeEventWriter(_RuntimeEventWriter):
    def __init__(self, path: Path, *, overwrite: bool = False) -> None:
        self.path = Path(path)
        self.tmp_path = self.path.with_name(f"{self.path.name}.tmp")
        self._overwrite = bool(overwrite)
        if self.path.exists() and not self._overwrite:
            raise SystemExit(f"output path already exists; pass --overwrite to replace it: {self.path}")
        if self.path.exists() and self.path.is_dir():
            raise SystemExit(f"output path is a directory, expected file: {self.path}")
        if self.tmp_path.exists():
            if not self._overwrite:
                raise SystemExit(f"temporary output path already exists; pass --overwrite to replace it: {self.tmp_path}")
            if self.tmp_path.is_dir():
                shutil.rmtree(self.tmp_path)
            else:
                self.tmp_path.unlink()
        ensure_dir(self.path.parent)
        self._handle = self.tmp_path.open("w", encoding="utf-8")

    @property
    def files(self) -> list[str]:
        return [str(self.path)]

    def write(self, row: Mapping[str, Any]) -> None:
        self._handle.write(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n")

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.close()

    def commit(self) -> None:
        self.close()
        self.tmp_path.replace(self.path)

    def cleanup(self) -> None:
        if self.tmp_path.exists():
            if self.tmp_path.is_dir():
                shutil.rmtree(self.tmp_path)
            else:
                self.tmp_path.unlink()


class _ShardedRuntimeEventWriter(_RuntimeEventWriter):
    def __init__(self, root: Path, *, overwrite: bool = False) -> None:
        self.root = Path(root)
        self.build_root = _building_dir_path(self.root)
        self._overwrite = bool(overwrite)
        if self.root.exists() and not self._overwrite:
            raise SystemExit(f"output directory already exists; pass --overwrite to replace it: {self.root}")
        if self.build_root.exists():
            if not self._overwrite:
                raise SystemExit(f"temporary output directory already exists; pass --overwrite to replace it: {self.build_root}")
            shutil.rmtree(self.build_root)
        ensure_dir(self.build_root)
        self._handle: Any | None = None
        self._current_path: Path | None = None
        self._seen: set[Path] = set()
        self._files: list[str] = []
        self._shards: dict[Path, dict[str, Any]] = {}

    @property
    def files(self) -> list[str]:
        return list(self._files)

    @property
    def manifest_path(self) -> Path:
        return self.root / "runtime_events_manifest.json"

    def write(self, row: Mapping[str, Any]) -> None:
        path = self.build_root / f"{_utc_day(float(row.get('ts') or 0.0))}.jsonl"
        if path != self._current_path:
            self.close()
            mode = "a" if path in self._seen else "w"
            ensure_dir(path.parent)
            self._handle = path.open(mode, encoding="utf-8")
            self._current_path = path
            if path not in self._seen:
                self._seen.add(path)
                self._files.append(str(self.root / path.name))
        self._handle.write(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n")
        ts = float(row.get("ts") or 0.0)
        shard = self._shards.setdefault(
            path,
            {"path": path.name, "first_ts": ts, "last_ts": ts, "row_count": 0},
        )
        shard["first_ts"] = ts if float(shard.get("first_ts") or 0.0) <= 0 else min(float(shard["first_ts"]), ts)
        shard["last_ts"] = max(float(shard.get("last_ts") or 0.0), ts)
        shard["row_count"] = int(shard.get("row_count") or 0) + 1

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def commit(self) -> None:
        self.close()
        if self.root.exists():
            if self.root.is_dir():
                shutil.rmtree(self.root)
            else:
                self.root.unlink()
        self.build_root.replace(self.root)

    def cleanup(self) -> None:
        self.close()
        if self.build_root.exists():
            shutil.rmtree(self.build_root)

    def write_manifest(
        self,
        *,
        symbol: str,
        bucket_sec: float,
        rolling_sec: float,
        contract_multiplier: float,
        notional_mode: str,
        output_mode: str,
        schema_version: str = RUNTIME_EVENTS_SCHEMA_VERSION,
    ) -> None:
        created_at = datetime.now(timezone.utc).isoformat()
        shards: list[dict[str, Any]] = []
        for path in sorted(self._shards):
            shard = dict(self._shards[path])
            day = str(shard.get("day") or Path(str(shard.get("path") or path.name)).stem)
            shard.update(
                {
                    "day": day,
                    "path": f"{day}.jsonl",
                    "bucket_sec": float(bucket_sec),
                    "rolling_sec": float(rolling_sec),
                    "contract_multiplier": float(contract_multiplier),
                    "notional_mode": str(notional_mode),
                    "schema_version": str(schema_version),
                    "created_at": created_at,
                }
            )
            shards.append(shard)
        manifest = {
            "symbol": symbol,
            "bucket_sec": float(bucket_sec),
            "rolling_sec": float(rolling_sec),
            "contract_multiplier": float(contract_multiplier),
            "notional_mode": notional_mode,
            "schema_version": str(schema_version),
            "output_mode": output_mode,
            "shards": shards,
        }
        write_json(self.build_root / "runtime_events_manifest.json", manifest)


def discover_trade_files(
    path: Path,
    *,
    allow_json_file: bool = False,
    include_metadata_json: bool = False,
) -> tuple[list[Path], int]:
    """Return (trade_files, skipped_json_file_count)."""
    if path.is_file():
        if _is_raw_json_path(path) and not allow_json_file:
            raise SystemExit(JSON_FILE_DISABLED_ERROR)
        return ([path] if _is_supported_trade_path(path, allow_json_file=allow_json_file) else [], 0)
    if not path.exists():
        raise SystemExit(f"trades path does not exist: {path}")
    files: list[Path] = []
    skipped_json: int = 0
    for child in sorted(path.rglob("*")):
        if not child.is_file() or any(part.startswith(".") for part in child.parts):
            continue
        if _is_raw_json_path(child):
            if not allow_json_file:
                skipped_json += 1
                continue
            if not include_metadata_json and _is_metadata_json_name(child.name):
                skipped_json += 1
                continue
        if not include_metadata_json and _is_metadata_json_gz_name(child.name):
            skipped_json += 1
            continue
        if _is_supported_trade_path(child, allow_json_file=allow_json_file):
            files.append(child)
    return files, skipped_json


def iter_normalized_trades(
    paths: Sequence[Path],
    *,
    symbol: str,
    contract_multiplier: float = 1.0,
    merge_sort_files: bool = False,
    max_json_line_bytes: int = MAX_JSON_LINE_BYTES,
    allow_json_file: bool = False,
    max_json_file_bytes: int = MAX_JSON_FILE_BYTES,
    read_stats: RuntimeReadStats | None = None,
    include_metadata_json: bool = False,
) -> Iterator[NormalizedTrade]:
    base_kwargs: dict[str, Any] = {
        "symbol": symbol,
        "contract_multiplier": contract_multiplier,
        "max_json_line_bytes": max_json_line_bytes,
        "include_metadata_json": bool(include_metadata_json),
    }
    if allow_json_file:
        base_kwargs["allow_json_file"] = True
        base_kwargs["max_json_file_bytes"] = max_json_file_bytes
    if read_stats is not None:
        base_kwargs["read_stats"] = read_stats

    if not merge_sort_files:
        for path in paths:
            yield from iter_normalized_trades_from_file(path, **base_kwargs)
        return
    heap: list[tuple[float, int, NormalizedTrade, Iterator[NormalizedTrade]]] = []
    for seq, path in enumerate(paths):
        iterator = iter_normalized_trades_from_file(path, **base_kwargs)
        trade = next(iterator, None)
        if trade is not None:
            heapq.heappush(heap, (trade.ts, seq, trade, iterator))
    while heap:
        _, seq, trade, iterator = heapq.heappop(heap)
        yield trade
        nxt = next(iterator, None)
        if nxt is not None:
            heapq.heappush(heap, (nxt.ts, seq, nxt, iterator))


def iter_normalized_trades_from_file(
    path: Path,
    *,
    symbol: str,
    contract_multiplier: float = 1.0,
    max_json_line_bytes: int = MAX_JSON_LINE_BYTES,
    allow_json_file: bool = False,
    max_json_file_bytes: int = MAX_JSON_FILE_BYTES,
    read_stats: RuntimeReadStats | None = None,
    include_metadata_json: bool = False,
) -> Iterator[NormalizedTrade]:
    for row in iter_trade_rows(
        path,
        max_json_line_bytes=max_json_line_bytes,
        allow_json_file=allow_json_file,
        max_json_file_bytes=max_json_file_bytes,
        read_stats=read_stats,
        include_metadata_json=include_metadata_json,
    ):
        trade = normalize_trade_row(row, symbol=symbol, contract_multiplier=contract_multiplier)
        if trade is not None:
            yield trade


def iter_trade_rows(
    path: Path,
    *,
    max_json_line_bytes: int = MAX_JSON_LINE_BYTES,
    allow_json_file: bool = False,
    max_json_file_bytes: int = MAX_JSON_FILE_BYTES,
    read_stats: RuntimeReadStats | None = None,
    include_metadata_json: bool = False,
) -> Iterator[dict[str, Any]]:
    name = path.name.lower()
    if name.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")):
        with tarfile.open(path, "r:*") as archive:
            for member in sorted((m for m in archive.getmembers() if m.isfile()), key=lambda item: item.name):
                inner = Path(member.name)
                if _is_raw_json_path(inner):
                    if not allow_json_file:
                        if read_stats is not None:
                            read_stats.skipped_json_file_count += 1
                        continue
                    if not include_metadata_json and _is_metadata_json_name(inner.name):
                        if read_stats is not None:
                            read_stats.skipped_json_file_count += 1
                        continue
                    _ensure_json_member_size(member.size, max_json_file_bytes, inner)
                if not _is_supported_inner(inner, allow_json_file=allow_json_file):
                    continue
                raw = archive.extractfile(member)
                if raw is None:
                    continue
                with raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
                    yield from iter_text_rows(
                        text,
                        inner.suffix.lower(),
                        max_json_line_bytes=max_json_line_bytes,
                        allow_json_file=allow_json_file,
                        read_stats=read_stats,
                    )
        return
    if name.endswith(".zip"):
        with ZipFile(path) as archive:
            for member in sorted(archive.namelist()):
                inner = Path(member)
                if member.endswith("/"):
                    continue
                if _is_raw_json_path(inner):
                    if not allow_json_file:
                        if read_stats is not None:
                            read_stats.skipped_json_file_count += 1
                        continue
                    if not include_metadata_json and _is_metadata_json_name(inner.name):
                        if read_stats is not None:
                            read_stats.skipped_json_file_count += 1
                        continue
                    _ensure_json_member_size(archive.getinfo(member).file_size, max_json_file_bytes, inner)
                if not _is_supported_inner(inner, allow_json_file=allow_json_file):
                    continue
                with archive.open(member) as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
                    yield from iter_text_rows(
                        text,
                        inner.suffix.lower(),
                        max_json_line_bytes=max_json_line_bytes,
                        allow_json_file=allow_json_file,
                        read_stats=read_stats,
                    )
        return
    if name.endswith(".gz"):
        inner_suffix = path.with_suffix("").suffix.lower()
        if inner_suffix == ".json":
            raise SystemExit(GZ_JSON_DISABLED_ERROR)
        with gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="") as handle:
            yield from iter_text_rows(
                handle,
                inner_suffix,
                max_json_line_bytes=max_json_line_bytes,
                allow_json_file=allow_json_file,
                read_stats=read_stats,
            )
        return
    if _is_raw_json_path(path):
        _ensure_json_file_allowed(allow_json_file)
        _ensure_json_file_size(path, max_json_file_bytes)
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        yield from iter_text_rows(
            handle,
            path.suffix.lower(),
            max_json_line_bytes=max_json_line_bytes,
            allow_json_file=allow_json_file,
            read_stats=read_stats,
        )


def iter_text_rows(
    handle: io.TextIOBase,
    suffix: str,
    *,
    max_json_line_bytes: int = MAX_JSON_LINE_BYTES,
    allow_json_file: bool = False,
    read_stats: RuntimeReadStats | None = None,
) -> Iterator[dict[str, Any]]:
    sample = handle.read(4096)
    handle.seek(0)
    fmt = _detect_format(suffix, sample)
    if fmt == "jsonl":
        for line in handle:
            if len(line) > int(max_json_line_bytes):
                raise ValueError("JSONL/.data line exceeds max-json-line-bytes; giant JSON arrays are not memory-safe.")
            text = line.strip()
            if not text:
                continue
            value = json.loads(text)
            for row in _expand_json(value):
                if read_stats is not None:
                    read_stats.raw_rows_seen += 1
                yield row
    elif fmt == "json":
        _ensure_json_file_allowed(allow_json_file)
        for row in _expand_json(json.load(handle)):
            if read_stats is not None:
                read_stats.raw_rows_seen += 1
            yield row
    else:
        has_header = _csv_has_header(sample)
        if has_header:
            for row in csv.DictReader(handle):
                if read_stats is not None:
                    read_stats.raw_rows_seen += 1
                yield row
        else:
            for row in csv.reader(handle):
                if row:
                    if read_stats is not None:
                        read_stats.raw_rows_seen += 1
                    yield {str(idx): value for idx, value in enumerate(row)}


def normalize_trade_row(row: Mapping[str, Any], *, symbol: str, contract_multiplier: float = 1.0) -> NormalizedTrade | None:
    row_symbol = _first(row, "instId", "inst_id", "symbol", "instrument")
    if row_symbol and str(row_symbol) != symbol:
        return None
    ts = parse_timestamp(_first(row, "ts", "timestamp", "time", "created_time", "3"))
    price = parse_float(_first(row, "px", "price", "trade_price", "0"))
    raw_size = parse_float(_first(row, "sz", "size", "qty", "amount", "1"))
    side = str(_first(row, "side", "direction", "2") or "").strip().upper()
    if side in {"BID", "B", "BUYER"}:
        side = "BUY"
    elif side in {"ASK", "S", "SELLER"}:
        side = "SELL"
    notional = parse_float(_first(row, "notional", "trade_notional", "sz_usdt", "amount_usdt", "notional_usdt"))
    if notional <= 0 and price > 0 and raw_size > 0:
        notional = price * raw_size * float(contract_multiplier)
    if ts <= 0 or price <= 0 or notional <= 0 or side not in {"BUY", "SELL"}:
        return None
    return NormalizedTrade(ts=ts, symbol=symbol, price=price, side=side, notional=notional, contract_multiplier=float(contract_multiplier))


def _bucket_start(ts: float, bucket_sec: float) -> float:
    return int(float(ts) / bucket_sec) * bucket_sec


def _trim_rolling_and_update_sums(
    rolling: deque[NormalizedTrade],
    ts: float,
    rolling_sec: float,
    rolling_buy_sum: float,
    rolling_sell_sum: float,
    rolling_signed_sum: float,
) -> tuple[float, float, float]:
    """Remove expired trades and update incremental sums inline (no list allocation)."""
    cutoff = float(ts) - float(rolling_sec)
    while rolling and rolling[0].ts < cutoff:
        removed = rolling.popleft()
        if removed.side == "BUY":
            rolling_buy_sum -= removed.notional
            rolling_signed_sum -= removed.notional
        elif removed.side == "SELL":
            rolling_sell_sum -= removed.notional
            rolling_signed_sum += removed.notional
    return rolling_buy_sum, rolling_sell_sum, rolling_signed_sum


def _trim_price_window(window: deque[tuple[float, float]], ts: float, rolling_sec: float) -> None:
    cutoff = float(ts) - float(rolling_sec)
    while len(window) > 1 and window[1][0] <= cutoff:
        window.popleft()


def _price_velocity(
    trade: NormalizedTrade,
    window: deque[tuple[float, float]],
    prev_trade: NormalizedTrade | None,
    rolling_sec: float,
) -> float:
    if window:
        base_ts, base_price = window[0]
        elapsed = max(trade.ts - base_ts, 0.0)
        if elapsed > 0:
            return (trade.price - base_price) / elapsed
    if prev_trade is not None and trade.ts > prev_trade.ts:
        return (trade.price - prev_trade.price) / (trade.ts - prev_trade.ts)
    return 0.0


def _resolve_contract_multiplier(symbol: str, value: float | None) -> float:
    if value is None and "-SWAP" in str(symbol).upper():
        raise SystemExit("--contract-multiplier is required for SWAP symbols to avoid incorrect notional.")
    return float(1.0 if value is None else value)


def _join_warning(existing: str, warning: str) -> str:
    return warning if not existing else f"{existing}; {warning}"


def _utc_day(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).date().isoformat()


def _is_supported_inner(path: Path, *, allow_json_file: bool = False) -> bool:
    name = path.name.lower()
    suffixes = (".data", ".csv", ".jsonl", ".ndjson")
    return name.endswith(suffixes) or (allow_json_file and name.endswith(".json"))


def _is_supported_trade_path(path: Path, *, allow_json_file: bool = False) -> bool:
    name = path.name.lower()
    suffixes = (".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz", ".zip", ".gz", ".data", ".csv", ".jsonl", ".ndjson")
    return name.endswith(suffixes) or (allow_json_file and name.endswith(".json"))


def _is_raw_json_path(path: Path) -> bool:
    return path.name.lower().endswith(".json")


def _is_metadata_json_name(name: str) -> bool:
    """Check if a .json filename looks like metadata (summary, manifest, etc.)."""
    stem = Path(name).stem.lower()
    metadata_keywords = ("summary", "manifest", "metadata", "meta", "stats")
    return any(kw in stem for kw in metadata_keywords)


def _is_metadata_json_gz_name(name: str) -> bool:
    """Check if a .json.gz filename looks like metadata (summary.json.gz, etc.)."""
    lower = name.lower()
    if not lower.endswith(".json.gz"):
        return False
    inner_stem = Path(lower[:-len(".gz")]).stem  # stem of the .json part
    metadata_keywords = ("summary", "manifest", "metadata", "meta", "stats")
    return any(kw in inner_stem for kw in metadata_keywords)


def _ensure_json_file_allowed(allow_json_file: bool) -> None:
    if not allow_json_file:
        raise SystemExit(JSON_FILE_DISABLED_ERROR)


def _ensure_json_file_size(path: Path, max_json_file_bytes: int) -> None:
    size = path.stat().st_size
    _ensure_json_member_size(size, max_json_file_bytes, path)


def _ensure_json_member_size(size: int, max_json_file_bytes: int, path: Path) -> None:
    if int(size) > int(max_json_file_bytes):
        raise SystemExit(
            f"Raw .json file exceeds max-json-file-bytes ({int(max_json_file_bytes)}); "
            f"json.load is not memory-safe for large files: {path}"
        )


def _building_dir_path(path: Path) -> Path:
    return path.with_name(f"{path.name}._building")


def _detect_format(suffix: str, sample: str) -> str:
    stripped = sample.lstrip()
    if not stripped:
        return ""
    first = stripped[0]
    if suffix in {".jsonl", ".ndjson"}:
        return "jsonl"
    if suffix == ".json":
        return "json"
    if suffix == ".data":
        return "jsonl" if first in {"{", "["} else "csv"
    if first in {"{", "["}:
        return "jsonl"
    return "csv"


def _csv_has_header(sample: str) -> bool:
    try:
        return csv.Sniffer().has_header(sample) if sample.strip() else False
    except csv.Error:
        return False


def _expand_json(value: Any) -> Iterator[dict[str, Any]]:
    rows = value if isinstance(value, list) else value.get("data") if isinstance(value, dict) and isinstance(value.get("data"), list) else [value]
    for row in rows:
        if isinstance(row, dict):
            yield row


def _first(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row.get(name) not in (None, ""):
            return row.get(name)
    return None


def _peak_rss_mb() -> float:
    try:
        rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    except Exception:
        return 0.0
    if sys.platform == "darwin":
        rss /= 1024.0 * 1024.0
    else:
        rss /= 1024.0
    return round(rss, 4)


if __name__ == "__main__":
    raise SystemExit(main())
