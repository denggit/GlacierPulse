#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import heapq
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from src.research.a1_edge.schema import parse_float


@dataclass(frozen=True)
class RuntimeEventFile:
    path: Path
    first_ts: float
    last_ts: float


class RuntimeEventSource:
    def __init__(self, paths: Iterable[str | Path] | str | Path | None = None) -> None:
        self.files = _discover_files(paths)
        self.mode = _source_mode(self.files, paths)
        self.window_reads = 0
        self.max_window_ticks = 0
        self.ticks_materialized_count = 0
        self.candidate_file_scans = 0

    @classmethod
    def from_paths(cls, *paths: str | Path | None) -> "RuntimeEventSource | None":
        selected = [Path(path) for path in paths if path]
        if not selected:
            return None
        return cls(selected)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        yield from _merge_sorted_files(self.files)

    def get_window(self, start_ts: float, end_ts: float, symbol: str | None = None) -> Iterator[dict[str, Any]]:
        self.window_reads += 1
        count = 0
        candidates = _candidate_files(self.files, start_ts, end_ts)
        self.candidate_file_scans += len(candidates)
        try:
            for row in _merge_sorted_files(candidates):
                ts = _event_ts(row)
                if ts < start_ts:
                    continue
                if ts > end_ts:
                    break
                if _symbol_matches(symbol or "", row):
                    count += 1
                    yield row
        finally:
            self.max_window_ticks = max(self.max_window_ticks, count)

    def memory_profile(self) -> dict[str, Any]:
        return {
            "runtime_event_source_mode": self.mode,
            "runtime_ticks_materialized_count": self.ticks_materialized_count,
            "runtime_window_reads": self.window_reads,
            "runtime_max_window_ticks": self.max_window_ticks,
            "runtime_candidate_file_scans": self.candidate_file_scans,
        }


RuntimeEventReader = RuntimeEventSource


def iter_runtime_event_file(path: Path | str) -> Iterator[dict[str, Any]]:
    p = Path(path)
    suffix = p.suffix.lower()
    if suffix == ".csv":
        with p.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                yield dict(row)
        return
    with p.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                value = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid runtime event JSONL at {p}:{line_no}: {exc}") from exc
            if isinstance(value, dict):
                yield value


def _discover_files(paths: Iterable[str | Path] | str | Path | None) -> list[RuntimeEventFile]:
    if paths is None:
        return []
    raw_paths = _coerce_paths(paths)
    files: list[Path] = []
    for path in raw_paths:
        if path.is_dir():
            files.extend(sorted(
                child for child in [*path.rglob("*.jsonl"), *path.rglob("*.csv")]
                if not _is_hidden_path(child)
            ))
        elif path.exists() and _is_runtime_event_file(path) and not _is_hidden_path(path):
            files.append(path)
    profiled: list[RuntimeEventFile] = []
    for path in sorted(set(files)):
        first_ts, last_ts = _file_time_bounds(path)
        if first_ts > 0:
            profiled.append(RuntimeEventFile(path=path, first_ts=first_ts, last_ts=last_ts or first_ts))
    return sorted(profiled, key=lambda item: (item.first_ts, str(item.path)))


def _source_mode(files: list[RuntimeEventFile], paths: object) -> str:
    if not files:
        return "empty"
    if any(path.is_dir() for path in _coerce_paths(paths)):
        return "directory_stream"
    return "file_stream" if len(files) == 1 else "multi_file_stream"


def _coerce_paths(paths: object) -> list[Path]:
    if paths is None:
        return []
    if isinstance(paths, (str, Path)):
        return [Path(paths)]
    return [Path(path) for path in paths]  # type: ignore[arg-type]


def _candidate_files(files: list[RuntimeEventFile], start_ts: float, end_ts: float) -> list[RuntimeEventFile]:
    return [file for file in files if file.last_ts >= start_ts and file.first_ts <= end_ts]


def _is_runtime_event_file(path: Path) -> bool:
    return path.suffix.lower() in {".jsonl", ".csv"}


def _is_hidden_path(path: Path) -> bool:
    return any(part.startswith(".") for part in path.parts)


def _merge_sorted_files(files: list[RuntimeEventFile]) -> Iterator[dict[str, Any]]:
    heap: list[tuple[float, int, dict[str, Any], Iterator[dict[str, Any]]]] = []
    for seq, file in enumerate(files):
        iterator = iter_runtime_event_file(file.path)
        first = _next_valid(iterator)
        if first is not None:
            heapq.heappush(heap, (_event_ts(first), seq, first, iterator))
    while heap:
        _, seq, row, iterator = heapq.heappop(heap)
        yield row
        nxt = _next_valid(iterator)
        if nxt is not None:
            heapq.heappush(heap, (_event_ts(nxt), seq, nxt, iterator))


def _next_valid(iterator: Iterator[dict[str, Any]]) -> dict[str, Any] | None:
    for row in iterator:
        if _event_ts(row) > 0:
            return row
    return None


def _file_time_bounds(path: Path) -> tuple[float, float]:
    first = 0.0
    last = 0.0
    for row in iter_runtime_event_file(path):
        ts = _event_ts(row)
        if ts <= 0:
            continue
        if first <= 0:
            first = ts
        last = ts
    return first, last


def _event_ts(row: Mapping[str, Any]) -> float:
    for name in ("ts", "timestamp", "event_ts", "recv_ts"):
        value = parse_float(row.get(name))
        if value > 0:
            return value / 1000.0 if value > 10_000_000_000 else value
    return 0.0


def _symbol_matches(symbol: str, row: Mapping[str, Any]) -> bool:
    row_symbol = str(row.get("symbol") or row.get("instId") or "")
    return not symbol or not row_symbol or symbol == row_symbol
