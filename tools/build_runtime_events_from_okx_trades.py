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

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.research.a1_edge.io_utils import ensure_dir, write_json
from src.research.a1_edge.schema import parse_float, parse_timestamp


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build runtime_events.jsonl from OKX raw trades.")
    parser.add_argument("--symbol", required=True, help="OKX instrument id, e.g. ETH-USDT-SWAP.")
    parser.add_argument("--trades-dir", required=True, help="Directory or file containing raw OKX trades.")
    output = parser.add_mutually_exclusive_group(required=True)
    output.add_argument("--out", help="Output runtime_events JSONL path. Not recommended for long samples; prefer --out-dir --shard-by day.")
    output.add_argument("--out-dir", help="Output directory for sharded runtime events.")
    parser.add_argument("--shard-by", choices=["day"], default=None, help="Shard runtime events under --out-dir as runtime_events/YYYY-MM-DD.jsonl.")
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
    out_dir = Path(args.out_dir) if args.out_dir else None
    summary = build_runtime_events(
        symbol=args.symbol,
        trades_path=Path(args.trades_dir),
        out_path=out_path,
        out_dir=out_dir,
        shard_by=args.shard_by,
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
    rolling: deque[NormalizedTrade] = deque()
    price_window: deque[tuple[float, float]] = deque()
    current_bucket: float | None = None
    pending_event: dict[str, Any] | None = None
    prev_trade: NormalizedTrade | None = None
    prev_ts: float | None = None
    rolling_buy_sum: float = 0.0
    rolling_sell_sum: float = 0.0
    rolling_signed_sum: float = 0.0
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
                    pending_event = None
                    raise SystemExit(NON_MONOTONIC_ERROR)
                if "non-monotonic trades detected" not in stats.output_warning:
                    stats.output_warning = _join_warning(
                        stats.output_warning,
                        "non-monotonic trades detected; consider --merge-sort-files",
                    )
                if NON_MONOTONIC_UNSAFE_WARNING not in stats.output_warning:
                    stats.output_warning = _join_warning(stats.output_warning, NON_MONOTONIC_UNSAFE_WARNING)
                if pending_event is not None:
                    writer.write(pending_event)
                    stats.runtime_events_written += 1
                    pending_event = None
                rolling.clear()
                price_window.clear()
                current_bucket = None
                prev_trade = None
                rolling_buy_sum = 0.0
                rolling_sell_sum = 0.0
                rolling_signed_sum = 0.0
            stats.first_ts = trade.ts if stats.first_ts <= 0 else min(stats.first_ts, trade.ts)
            stats.last_ts = max(stats.last_ts, trade.ts)
            bucket = _bucket_start(trade.ts, bucket_sec)
            if current_bucket is not None and bucket != current_bucket and pending_event is not None:
                writer.write(pending_event)
                stats.runtime_events_written += 1
            current_bucket = bucket
            if trade.side == "BUY":
                rolling_buy_sum += trade.notional
                rolling_signed_sum += trade.notional
            elif trade.side == "SELL":
                rolling_sell_sum += trade.notional
                rolling_signed_sum -= trade.notional
            rolling.append(trade)
            price_window.append((trade.ts, trade.price))
            for removed in _trim_rolling(rolling, trade.ts, rolling_sec):
                if removed.side == "BUY":
                    rolling_buy_sum -= removed.notional
                    rolling_signed_sum -= removed.notional
                elif removed.side == "SELL":
                    rolling_sell_sum -= removed.notional
                    rolling_signed_sum += removed.notional
            _trim_price_window(price_window, trade.ts, rolling_sec)
            pending_event = runtime_event_from_trade(
                trade,
                price_window=price_window,
                prev_trade=prev_trade,
                rolling_sec=rolling_sec,
                rolling_buy_sum=rolling_buy_sum,
                rolling_sell_sum=rolling_sell_sum,
                rolling_signed_sum=rolling_signed_sum,
            )
            prev_trade = trade
            prev_ts = trade.ts

        if pending_event is not None:
            writer.write(pending_event)
            stats.runtime_events_written += 1
            pending_event = None
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
    ) -> None:
        manifest = {
            "symbol": symbol,
            "bucket_sec": bucket_sec,
            "rolling_sec": rolling_sec,
            "contract_multiplier": contract_multiplier,
            "notional_mode": notional_mode,
            "output_mode": output_mode,
            "shards": [self._shards[path] for path in sorted(self._shards)],
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


def _trim_rolling(rolling: deque[NormalizedTrade], ts: float, rolling_sec: float) -> list[NormalizedTrade]:
    """Remove expired trades and return them so caller can update incremental sums."""
    cutoff = float(ts) - float(rolling_sec)
    removed: list[NormalizedTrade] = []
    while rolling and rolling[0].ts < cutoff:
        removed.append(rolling.popleft())
    return removed


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
