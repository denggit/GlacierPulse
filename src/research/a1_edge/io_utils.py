#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence
from zoneinfo import ZoneInfo

from .schema import parse_float, parse_timestamp


KLINE_FIELDS = ["timestamp", "open", "high", "low", "close", "volume"]
TIMESTAMP_ALIASES = ("timestamp", "ts", "datetime", "time")
TIMESTAMP_EPOCH_SEC_ALIASES = ("timestamp_epoch_sec", "timestamp_sec", "epoch_sec")
TIMESTAMP_MS_ALIASES = ("timestamp_ms", "ts_ms")
MS_EPOCH_THRESHOLD = 10_000_000_000
MIN_REASONABLE_KLINE_EPOCH_SEC = 1_500_000_000
KLINE_ALIASES = {
    "open": ("open",),
    "high": ("high",),
    "low": ("low",),
    "close": ("close",),
    "volume": ("volume", "vol"),
}


def ensure_dir(path: Path | str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def read_jsonl(path: Path | str) -> List[Dict[str, Any]]:
    p = Path(path)
    records: List[Dict[str, Any]] = []
    if not p.exists() or p.stat().st_size == 0:
        return records
    with p.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                value = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL at {p}:{line_no}: {exc}") from exc
            if isinstance(value, dict):
                records.append(value)
    return records


def read_csv(path: Path | str) -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return []
    with p.open("r", encoding="utf-8", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def write_json(path: Path | str, data: Mapping[str, Any]) -> None:
    p = Path(path)
    ensure_dir(p.parent)
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")


def write_jsonl(path: Path | str, rows: Iterable[Mapping[str, Any]]) -> None:
    p = Path(path)
    ensure_dir(p.parent)
    with p.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n")


def write_csv(path: Path | str, rows: Iterable[Mapping[str, Any]], fieldnames: Sequence[str]) -> None:
    p = Path(path)
    ensure_dir(p.parent)
    with p.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def _find_column(columns: Sequence[str], aliases: Sequence[str], label: str) -> str:
    lower_to_original = {c.lower().strip(): c for c in columns}
    for alias in aliases:
        if alias.lower() in lower_to_original:
            return lower_to_original[alias.lower()]
    raise ValueError(f"Kline CSV missing required column for {label}. Accepted names: {', '.join(aliases)}")


def _find_optional_column(columns: Sequence[str], aliases: Sequence[str]) -> str | None:
    lower_to_original = {c.lower().strip(): c for c in columns}
    for alias in aliases:
        if alias.lower() in lower_to_original:
            return lower_to_original[alias.lower()]
    return None


def _is_present(value: Any) -> bool:
    return value is not None and value != ""


def _parse_numeric(value: Any) -> float | None:
    if not _is_present(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_okx_ts_ms_to_epoch_sec(ts_ms: Any) -> float:
    ts = float(ts_ms)
    if ts > MS_EPOCH_THRESHOLD:
        return ts / 1000.0
    return ts


def parse_kline_timestamp(value: Any, kline_timezone: str | None = "Asia/Shanghai") -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return parse_timestamp(value)
    text = str(value).strip()
    if not text:
        return 0.0
    try:
        return parse_timestamp(float(text))
    except ValueError:
        pass

    iso = text.replace("Z", "+00:00").replace("z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return 0.0
    if dt.tzinfo is None:
        if kline_timezone is None:
            raise ValueError(
                f"Kline timestamp '{text}' has no timezone. Pass kline_timezone, for example Asia/Shanghai."
            )
        dt = dt.replace(tzinfo=ZoneInfo(str(kline_timezone)))
    return dt.timestamp()


def _parse_row_timestamp(row: Mapping[str, Any], columns: Sequence[str], kline_timezone: str | None) -> float:
    epoch_col = _find_optional_column(columns, TIMESTAMP_EPOCH_SEC_ALIASES)
    ms_col = _find_optional_column(columns, TIMESTAMP_MS_ALIASES)
    ms_value = _parse_numeric(row.get(ms_col)) if ms_col else None
    if ms_value is not None and ms_value > MS_EPOCH_THRESHOLD:
        return ms_value / 1000.0

    if epoch_col and _is_present(row.get(epoch_col)):
        ts = float(row.get(epoch_col))
        if MIN_REASONABLE_KLINE_EPOCH_SEC <= ts < MS_EPOCH_THRESHOLD:
            return ts
        if 1_000_000 < ts < 10_000_000 and ms_value is not None and ms_value > MS_EPOCH_THRESHOLD:
            return ms_value / 1000.0
        if ts > MS_EPOCH_THRESHOLD:
            return ts / 1000.0

    ts_col = _find_optional_column(columns, TIMESTAMP_ALIASES)
    ts = parse_kline_timestamp(row.get(ts_col), kline_timezone=kline_timezone) if ts_col else 0.0
    if ts <= 0:
        utc_col = _find_optional_column(columns, ("timestamp_utc",))
        ts = parse_kline_timestamp(row.get(utc_col), kline_timezone="UTC") if utc_col else 0.0
    if ts <= 0:
        local_col = _find_optional_column(columns, ("timestamp_local",))
        ts = parse_kline_timestamp(row.get(local_col), kline_timezone=kline_timezone) if local_col else 0.0
    if ts < MIN_REASONABLE_KLINE_EPOCH_SEC:
        if epoch_col and _is_present(row.get(epoch_col)):
            raise ValueError(
                "Kline timestamp_epoch_sec looks too small, possible ms/sec double conversion. "
                "Regenerate the CSV or include a valid timestamp_ms/timestamp_utc column."
            )
        if ms_value is not None:
            raise ValueError("Kline timestamp_ms looks too small for milliseconds and no valid timestamp string was available.")
        raise ValueError(
            "Kline timestamp looks too small, possible ms/sec double conversion or invalid timezone source."
        )
    return ts


def normalize_klines(rows: Iterable[Mapping[str, Any]], kline_timezone: str | None = "Asia/Shanghai") -> List[Dict[str, float]]:
    raw = list(rows or [])
    if not raw:
        return []
    columns = list(raw[0].keys())
    mapping = {name: _find_column(columns, aliases, name) for name, aliases in KLINE_ALIASES.items()}
    normalized: List[Dict[str, float]] = []
    for row in raw:
        ts = _parse_row_timestamp(row, columns, kline_timezone)
        if ts <= 0:
            continue
        normalized.append(
            {
                "timestamp": ts,
                "open": parse_float(row.get(mapping["open"])),
                "high": parse_float(row.get(mapping["high"])),
                "low": parse_float(row.get(mapping["low"])),
                "close": parse_float(row.get(mapping["close"])),
                "volume": parse_float(row.get(mapping["volume"])),
            }
        )
    normalized.sort(key=lambda x: x["timestamp"])
    return normalized


def read_kline_csv(path: Path | str, kline_timezone: str | None = "Asia/Shanghai") -> List[Dict[str, float]]:
    return normalize_klines(read_csv(path), kline_timezone=kline_timezone)


def parse_windows(spec: str | Sequence[str | int]) -> List[int]:
    if isinstance(spec, str):
        parts = [p.strip() for p in spec.split(",") if p.strip()]
    else:
        parts = list(spec)
    windows: List[int] = []
    for part in parts:
        text = str(part).strip().lower()
        if text.endswith("m"):
            windows.append(int(float(text[:-1]) * 60))
        elif text.endswith("s"):
            windows.append(int(float(text[:-1])))
        else:
            windows.append(int(float(text)))
    return windows
