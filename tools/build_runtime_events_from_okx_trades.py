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
import sys
import tarfile
from collections import deque
from dataclasses import dataclass
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
]


@dataclass(frozen=True)
class NormalizedTrade:
    ts: float
    symbol: str
    price: float
    side: str
    notional: float


@dataclass
class BuildStats:
    total_trades_read: int = 0
    runtime_events_written: int = 0
    first_ts: float = 0.0
    last_ts: float = 0.0
    peak_rss_mb: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "total_trades_read": self.total_trades_read,
            "runtime_events_written": self.runtime_events_written,
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
            "peak_rss_mb": self.peak_rss_mb,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build runtime_events.jsonl from OKX raw trades.")
    parser.add_argument("--symbol", required=True, help="OKX instrument id, e.g. ETH-USDT-SWAP.")
    parser.add_argument("--trades-dir", required=True, help="Directory or file containing raw OKX trades.")
    parser.add_argument("--out", required=True, help="Output runtime_events JSONL path.")
    parser.add_argument("--bucket-sec", type=float, default=1.0)
    parser.add_argument("--rolling-sec", type=float, default=3.0)
    parser.add_argument("--contract-multiplier", type=float, default=1.0, help="Multiplier applied to raw size before notional = price * size.")
    parser.add_argument("--summary-out", help="Optional summary JSON path. Default: <out>.summary.json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = build_runtime_events(
        symbol=args.symbol,
        trades_path=Path(args.trades_dir),
        out_path=Path(args.out),
        bucket_sec=float(args.bucket_sec),
        rolling_sec=float(args.rolling_sec),
        contract_multiplier=float(args.contract_multiplier),
    )
    summary_path = Path(args.summary_out) if args.summary_out else Path(str(args.out) + ".summary.json")
    write_json(summary_path, summary)
    print(
        "[BUILD-RUNTIME-EVENTS] "
        f"total_trades_read={summary['total_trades_read']} "
        f"runtime_events_written={summary['runtime_events_written']} "
        f"first_ts={summary['first_ts']} "
        f"last_ts={summary['last_ts']} "
        f"peak_rss_mb={summary['peak_rss_mb']} "
        f"out={args.out}"
    )
    return 0


def build_runtime_events(
    *,
    symbol: str,
    trades_path: Path,
    out_path: Path,
    bucket_sec: float = 1.0,
    rolling_sec: float = 3.0,
    contract_multiplier: float = 1.0,
) -> dict[str, Any]:
    stats = BuildStats()
    ensure_dir(out_path.parent)
    bucket_sec = max(float(bucket_sec), 0.001)
    rolling_sec = max(float(rolling_sec), 0.001)
    rolling: deque[NormalizedTrade] = deque()
    price_window: deque[tuple[float, float]] = deque()
    current_bucket: float | None = None
    pending_event: dict[str, Any] | None = None
    prev_trade: NormalizedTrade | None = None

    with out_path.open("w", encoding="utf-8") as out:
        for trade in iter_normalized_trades(discover_trade_files(trades_path), symbol=symbol, contract_multiplier=contract_multiplier):
            stats.total_trades_read += 1
            stats.first_ts = trade.ts if stats.first_ts <= 0 else min(stats.first_ts, trade.ts)
            stats.last_ts = max(stats.last_ts, trade.ts)
            bucket = _bucket_start(trade.ts, bucket_sec)
            if current_bucket is not None and bucket != current_bucket and pending_event is not None:
                out.write(json.dumps(pending_event, ensure_ascii=False, sort_keys=True) + "\n")
                stats.runtime_events_written += 1
            current_bucket = bucket
            rolling.append(trade)
            price_window.append((trade.ts, trade.price))
            _trim_rolling(rolling, trade.ts, rolling_sec)
            _trim_price_window(price_window, trade.ts, rolling_sec)
            pending_event = runtime_event_from_trade(
                trade,
                rolling=rolling,
                price_window=price_window,
                prev_trade=prev_trade,
                rolling_sec=rolling_sec,
            )
            prev_trade = trade

        if pending_event is not None:
            out.write(json.dumps(pending_event, ensure_ascii=False, sort_keys=True) + "\n")
            stats.runtime_events_written += 1

    stats.peak_rss_mb = _peak_rss_mb()
    return stats.as_dict()


def runtime_event_from_trade(
    trade: NormalizedTrade,
    *,
    rolling: Iterable[NormalizedTrade],
    price_window: deque[tuple[float, float]],
    prev_trade: NormalizedTrade | None,
    rolling_sec: float,
) -> dict[str, Any]:
    buy = 0.0
    sell = 0.0
    signed = 0.0
    for item in rolling:
        if item.side == "BUY":
            buy += item.notional
            signed += item.notional
        elif item.side == "SELL":
            sell += item.notional
            signed -= item.notional
    velocity = _price_velocity(trade, price_window, prev_trade, rolling_sec)
    return {
        "ts": round(trade.ts, 8),
        "symbol": trade.symbol,
        "last_price": round(trade.price, 8),
        "active_buy_notional_3s": round(buy, 8),
        "active_sell_notional_3s": round(sell, 8),
        "cvd_delta_3s": round(signed, 8),
        "price_velocity_u_per_sec": round(velocity, 8),
        "condition_available_ts": round(trade.ts, 8),
        "condition_source": "okx_raw_trades_builder",
    }


def discover_trade_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path] if _is_supported_trade_path(path) else []
    if not path.exists():
        raise SystemExit(f"trades path does not exist: {path}")
    return sorted(
        child for child in path.rglob("*")
        if child.is_file() and not any(part.startswith(".") for part in child.parts) and _is_supported_trade_path(child)
    )


def iter_normalized_trades(paths: Sequence[Path], *, symbol: str, contract_multiplier: float = 1.0) -> Iterator[NormalizedTrade]:
    heap: list[tuple[float, int, NormalizedTrade, Iterator[NormalizedTrade]]] = []
    for seq, path in enumerate(paths):
        iterator = iter_normalized_trades_from_file(path, symbol=symbol, contract_multiplier=contract_multiplier)
        trade = next(iterator, None)
        if trade is not None:
            heapq.heappush(heap, (trade.ts, seq, trade, iterator))
    while heap:
        _, seq, trade, iterator = heapq.heappop(heap)
        yield trade
        nxt = next(iterator, None)
        if nxt is not None:
            heapq.heappush(heap, (nxt.ts, seq, nxt, iterator))


def iter_normalized_trades_from_file(path: Path, *, symbol: str, contract_multiplier: float = 1.0) -> Iterator[NormalizedTrade]:
    for row in iter_trade_rows(path):
        trade = normalize_trade_row(row, symbol=symbol, contract_multiplier=contract_multiplier)
        if trade is not None:
            yield trade


def iter_trade_rows(path: Path) -> Iterator[dict[str, Any]]:
    name = path.name.lower()
    if name.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")):
        with tarfile.open(path, "r:*") as archive:
            for member in sorted((m for m in archive.getmembers() if m.isfile()), key=lambda item: item.name):
                inner = Path(member.name)
                if not _is_supported_inner(inner):
                    continue
                raw = archive.extractfile(member)
                if raw is None:
                    continue
                with raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
                    yield from iter_text_rows(text, inner.suffix.lower())
        return
    if name.endswith(".zip"):
        with ZipFile(path) as archive:
            for member in sorted(archive.namelist()):
                inner = Path(member)
                if member.endswith("/") or not _is_supported_inner(inner):
                    continue
                with archive.open(member) as raw:
                    text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
                    yield from iter_text_rows(text, inner.suffix.lower())
        return
    if name.endswith(".gz"):
        inner_suffix = path.with_suffix("").suffix.lower()
        with gzip.open(path, "rt", encoding="utf-8", errors="replace", newline="") as handle:
            yield from iter_text_rows(handle, inner_suffix)
        return
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        yield from iter_text_rows(handle, path.suffix.lower())


def iter_text_rows(handle: io.TextIOBase, suffix: str) -> Iterator[dict[str, Any]]:
    sample = handle.read(4096)
    handle.seek(0)
    fmt = _detect_format(suffix, sample)
    if fmt == "jsonl":
        for line in handle:
            text = line.strip()
            if not text:
                continue
            value = json.loads(text)
            yield from _expand_json(value)
    elif fmt == "json":
        yield from _expand_json(json.load(handle))
    else:
        has_header = _csv_has_header(sample)
        if has_header:
            yield from csv.DictReader(handle)
        else:
            for row in csv.reader(handle):
                if row:
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
    return NormalizedTrade(ts=ts, symbol=symbol, price=price, side=side, notional=notional)


def _bucket_start(ts: float, bucket_sec: float) -> float:
    return int(float(ts) / bucket_sec) * bucket_sec


def _trim_rolling(rolling: deque[NormalizedTrade], ts: float, rolling_sec: float) -> None:
    cutoff = float(ts) - float(rolling_sec)
    while rolling and rolling[0].ts < cutoff:
        rolling.popleft()


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


def _is_supported_inner(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith((".data", ".csv", ".jsonl", ".ndjson", ".json"))


def _is_supported_trade_path(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz", ".zip", ".gz", ".data", ".csv", ".jsonl", ".ndjson", ".json"))


def _detect_format(suffix: str, sample: str) -> str:
    stripped = sample.lstrip()
    if not stripped:
        return ""
    first = stripped[0]
    if suffix in {".jsonl", ".ndjson"}:
        return "jsonl"
    if suffix == ".json":
        return "json" if first == "[" else "jsonl"
    if first in {"{", "["}:
        return "json" if first == "[" else "jsonl"
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
