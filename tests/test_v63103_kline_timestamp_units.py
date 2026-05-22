import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from src.research.a1_edge.forward_metrics import A1ForwardMetricsAnalyzer
from src.research.a1_edge.io_utils import normalize_klines, parse_kline_timestamp
from src.research.a1_edge.random_baseline import RandomBaselineSampler
from src.research.a1_edge.schema import A1EdgeEvent
from tools.analyze_a1_edge import _build_data_quality
from tools.export_history_k import prepare_export_dataframe


BASE_TS = 1779364800.0
BASE_MS = 1779364800000


def _row(**extra):
    row = {"open": 100, "high": 101, "low": 99, "close": 100, "volume": 1}
    row.update(extra)
    return row


def _klines(count=90):
    return [
        {"timestamp": BASE_TS + i * 60, "open": 100 + i * 0.1, "high": 101 + i * 0.1, "low": 99 + i * 0.1, "close": 100 + i * 0.1, "volume": 1}
        for i in range(count)
    ]


def test_timestamp_ms_parses_to_epoch_seconds_once():
    rows = normalize_klines([_row(timestamp_ms=BASE_MS)])
    assert rows[0]["timestamp"] == BASE_TS


def test_timestamp_epoch_sec_is_not_divided_again():
    rows = normalize_klines([_row(timestamp_epoch_sec=BASE_TS)])
    assert rows[0]["timestamp"] == BASE_TS


def test_bad_epoch_sec_is_repaired_by_valid_timestamp_ms():
    rows = normalize_klines([_row(timestamp_epoch_sec=1779364.8, timestamp_ms=BASE_MS)])
    assert rows[0]["timestamp"] == BASE_TS


def test_bad_epoch_sec_without_timestamp_ms_raises():
    with pytest.raises(ValueError, match="too small"):
        normalize_klines([_row(timestamp_epoch_sec=1779364.8)])


def test_beijing_and_iso_strings_parse_to_expected_epoch_seconds():
    assert parse_kline_timestamp("2026-05-21 20:00:00", "Asia/Shanghai") == BASE_TS
    assert parse_kline_timestamp("2026-05-21T20:00:00+08:00") == BASE_TS


def test_export_dataframe_writes_ms_and_sec_on_same_axis():
    df = pd.DataFrame({"open": [100], "high": [101], "low": [99], "close": [100], "volume": [1]}, index=pd.to_datetime(["2026-05-21 20:00:00"]))
    df.index.name = "timestamp"
    out = prepare_export_dataframe(df, timezone_name="Asia/Shanghai")
    row = out.iloc[0]
    assert row["timestamp_epoch_sec"] == BASE_TS
    assert row["timestamp_ms"] == BASE_MS
    assert row["timestamp_utc"] == "2026-05-21T12:00:00+00:00"
    assert row["timestamp_local"] == "2026-05-21T20:00:00+08:00"
    assert row["timezone"] == "Asia/Shanghai"


def test_data_quality_has_overlap_when_kline_and_event_times_intersect():
    event = A1EdgeEvent.from_mapping({"zone_id": "z1", "direction": "BUY", "reaction_event_ts": BASE_TS + 300, "last_price": 100, "frozen_low": 99, "frozen_high": 101})
    forward = A1ForwardMetricsAnalyzer(windows_sec=[900, 3600]).analyze([event], _klines(90))
    quality = _build_data_quality([event], _klines(90), forward)
    assert quality["events_outside_kline_range"] == 0
    assert quality["valid_events_15m_count"] == 1
    assert quality["valid_events_60m_count"] == 1
    assert any(row.future_bar_count > 0 for row in forward if row.window_sec == 900)


def test_random_baseline_not_empty_when_kline_coverage_is_sufficient():
    event = A1EdgeEvent.from_mapping({"zone_id": "z1", "direction": "BUY", "reaction_event_ts": BASE_TS + 300, "last_price": 100, "frozen_low": 99, "frozen_high": 101})
    rows = RandomBaselineSampler(samples_per_event=3, random_seed=1, exclude_near_a1_minutes=0, windows_sec=[900]).sample([event], _klines(90))
    assert rows
    assert any(not row.insufficient_future_data for row in rows)


def test_cli_with_overlapping_ms_klines_does_not_report_kline_coverage_failure(tmp_path):
    events = tmp_path / "events.jsonl"
    events.write_text(json.dumps({"zone_id": "z1", "direction": "BUY", "reaction_event_ts": BASE_TS + 300, "last_price": 100, "frozen_low": 99, "frozen_high": 101}) + "\n", encoding="utf-8")
    klines = tmp_path / "k.csv"
    lines = ["timestamp_ms,timestamp_epoch_sec,timestamp,open,high,low,close,volume"]
    for i in range(90):
        ts_ms = int((BASE_TS + i * 60) * 1000)
        lines.append(f"{ts_ms},1779364.8,broken,{100+i*0.1},{101+i*0.1},{99+i*0.1},{100+i*0.1},1")
    klines.write_text("\n".join(lines) + "\n", encoding="utf-8")
    out = tmp_path / "out"
    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_a1_edge.py",
            "--events",
            str(events),
            "--klines",
            str(klines),
            "--out",
            str(out),
            "--random-samples-per-event",
            "2",
            "--seed",
            "42",
            "--kline-timezone",
            "Asia/Shanghai",
        ],
        cwd=str(Path(__file__).resolve().parents[1]),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "kline_min_ts=1779364800.0" in result.stdout
    summary = json.loads((out / "a1_edge_summary.json").read_text(encoding="utf-8"))
    assert summary["decision"] != "INSUFFICIENT_KLINE_COVERAGE"
    assert summary["data_quality"]["events_outside_kline_range"] == 0
    assert summary["data_quality"]["valid_events_15m_count"] > 0
    assert (out / "a1_random_baseline.csv").stat().st_size > 0
