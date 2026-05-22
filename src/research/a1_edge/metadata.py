#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import hashlib
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


ANALYZER_VERSION = "V6.3.10.4"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _non_empty_line_count(path: Path) -> int:
    with path.open("r", encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def _csv_data_row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8") as f:
        rows = sum(1 for line in f if line.strip())
    return max(0, rows - 1)


def get_git_commit(repo_root: Path | None = None) -> str:
    env_commit = os.environ.get("GLACIER_GIT_COMMIT", "").strip()
    if env_commit:
        return env_commit
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root or Path.cwd()),
            text=True,
            capture_output=True,
            check=False,
        )
    except Exception:
        return "UNKNOWN"
    commit = result.stdout.strip()
    return commit if result.returncode == 0 and commit else "UNKNOWN"


def build_run_metadata(
    events_path: Path | str,
    klines_path: Path | str,
    out_dir: Path | str,
    analysis_parameters: Mapping[str, Any],
    repo_root: Path | str | None = None,
) -> dict[str, Any]:
    events = Path(events_path).resolve()
    klines = Path(klines_path).resolve()
    out = Path(out_dir).resolve()
    root = Path(repo_root).resolve() if repo_root else Path.cwd()
    params = dict(analysis_parameters)
    metadata = {
        "events_file_path": str(events),
        "events_file_name": events.name,
        "events_file_size_bytes": events.stat().st_size,
        "events_file_sha256": _sha256(events),
        "events_file_line_count": _non_empty_line_count(events),
        "klines_file_path": str(klines),
        "klines_file_name": klines.name,
        "klines_file_size_bytes": klines.stat().st_size,
        "klines_file_sha256": _sha256(klines),
        "klines_file_row_count": _csv_data_row_count(klines),
        "out_dir": str(out),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "analyzer_version": ANALYZER_VERSION,
        "git_commit": get_git_commit(root),
        "python_version": platform.python_version() or sys.version.split()[0],
        "analysis_parameters": params,
    }
    for key in (
        "symbol",
        "windows",
        "random_samples_per_event",
        "seed",
        "min_risk_u",
        "min_risk_pct",
        "roundtrip_fee_pct",
        "baseline_risk_mode",
        "kline_timezone",
        "exclude_near_a1_minutes",
        "min_group_sample_size",
    ):
        if key in params:
            metadata[key] = params[key]
    return metadata
