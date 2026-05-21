#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from .schema import parse_float, parse_timestamp


KLINE_FIELDS = ["timestamp", "open", "high", "low", "close", "volume"]
TIMESTAMP_ALIASES = ("timestamp", "ts", "datetime", "time")
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


def normalize_klines(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, float]]:
    raw = list(rows or [])
    if not raw:
        return []
    columns = list(raw[0].keys())
    ts_col = _find_column(columns, TIMESTAMP_ALIASES, "timestamp")
    mapping = {name: _find_column(columns, aliases, name) for name, aliases in KLINE_ALIASES.items()}
    normalized: List[Dict[str, float]] = []
    for row in raw:
        ts = parse_timestamp(row.get(ts_col))
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


def read_kline_csv(path: Path | str) -> List[Dict[str, float]]:
    return normalize_klines(read_csv(path))


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
