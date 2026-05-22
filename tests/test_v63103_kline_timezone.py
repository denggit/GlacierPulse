import csv
import json
import subprocess
import sys
from pathlib import Path

from src.research.a1_edge.forward_metrics import compute_forward_metric
from src.research.a1_edge.hypothesis_simulator import A1HypothesisSimulator
from src.research.a1_edge.io_utils import normalize_klines, parse_kline_timestamp, read_kline_csv
from src.research.a1_edge.random_baseline import A1RandomBaselineComparator
from src.research.a1_edge.schema import A1EdgeEvent


EXPECTED_TS = 1779380580.0


def test_timestamp_epoch_sec_takes_priority_over_timestamp_string():
    rows = [{"timestamp_epoch_sec": str(EXPECTED_TS), "timestamp": "1970-01-01 00:00:00", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]
    assert normalize_klines(rows)[0]["timestamp"] == EXPECTED_TS


def test_timestamp_ms_divides_by_1000():
    rows = [{"timestamp_ms": str(int(EXPECTED_TS * 1000)), "timestamp": "1970-01-01 00:00:00", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]
    assert normalize_klines(rows)[0]["timestamp"] == EXPECTED_TS


def test_naive_beijing_timestamp_parses_as_asia_shanghai_not_utc():
    local_ts = parse_kline_timestamp("2026-05-22 00:23:00", kline_timezone="Asia/Shanghai")
    utc_ts = parse_kline_timestamp("2026-05-22 00:23:00", kline_timezone="UTC")
    assert abs(local_ts - EXPECTED_TS) < 1
    assert utc_ts - local_ts == 8 * 3600


def test_iso_timestamp_with_offset_and_z_parse_correctly():
    assert parse_kline_timestamp("2026-05-22T00:23:00+08:00") == EXPECTED_TS
    assert parse_kline_timestamp("2026-05-21T16:23:00Z") == EXPECTED_TS


def test_read_kline_csv_reads_legacy_local_timestamp(tmp_path):
    path = tmp_path / "legacy.csv"
    path.write_text("timestamp,open,high,low,close,volume\n2026-05-22 00:23:00,1,2,0,1,3\n", encoding="utf-8")
    rows = read_kline_csv(path, kline_timezone="Asia/Shanghai")
    assert rows[0]["timestamp"] == EXPECTED_TS


def test_analyze_cli_supports_kline_timezone_help():
    result = subprocess.run(
        [sys.executable, "tools/analyze_a1_edge.py", "--help"],
        cwd=str(Path(__file__).resolve().parents[1]),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0
    assert "--kline-timezone" in result.stdout
    assert "Asia/Shanghai" in result.stdout


def test_no_overlap_cli_emits_time_warning(tmp_path):
    events = tmp_path / "events.jsonl"
    events.write_text(json.dumps({"zone_id": "z1", "direction": "BUY", "reaction_event_ts": EXPECTED_TS, "last_price": 100, "frozen_low": 99, "frozen_high": 101}) + "\n", encoding="utf-8")
    klines = tmp_path / "k.csv"
    with klines.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_epoch_sec", "timestamp", "open", "high", "low", "close", "volume"])
        for i in range(5):
            writer.writerow([1779300000 + i * 60, "2026-05-21 02:00:00", 100, 101, 99, 100, 1])
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
            "--kline-timezone",
            "Asia/Shanghai",
        ],
        cwd=str(Path(__file__).resolve().parents[1]),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "[A1-EDGE-TIME-WARN] no overlap" in result.stdout
    summary = json.loads((out / "a1_edge_summary.json").read_text(encoding="utf-8"))
    assert summary["decision"] == "INSUFFICIENT_KLINE_COVERAGE"


def test_insufficient_future_data_flag_when_future_bars_are_too_sparse():
    event = A1EdgeEvent.from_mapping({"zone_id": "z1", "direction": "BUY", "reaction_event_ts": 30, "last_price": 100, "frozen_low": 99, "frozen_high": 101})
    bars = [
        {"timestamp": 0, "open": 100, "high": 100, "low": 100, "close": 100, "volume": 1},
        {"timestamp": 60, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1},
    ]
    metric = compute_forward_metric(event, bars, 900)
    assert metric.future_bar_count == 1
    assert metric.insufficient_future_data is True


def test_insufficient_rows_do_not_enter_comparator_core_stats():
    event = A1EdgeEvent.from_mapping({"zone_id": "z1", "direction": "BUY", "reaction_event_ts": 30, "last_price": 100, "frozen_low": 99, "frozen_high": 101})
    a1_metrics = [
        {"event_key": event.event_key, "window_sec": 900, "directional_mfe_r": 100, "hit_plus_1r": True, "total_range_u": 100, "insufficient_future_data": True},
        {"event_key": event.event_key, "window_sec": 900, "directional_mfe_r": 1, "hit_plus_1r": False, "total_range_u": 1, "insufficient_future_data": False},
    ]
    random_rows = [
        {"source_event_key": event.event_key, "window_sec": 900, "directional_mfe_r": 50, "hit_plus_1r": True, "total_range_u": 50, "insufficient_future_data": True},
        {"source_event_key": event.event_key, "window_sec": 900, "directional_mfe_r": 2, "hit_plus_1r": False, "total_range_u": 2, "insufficient_future_data": False},
    ]
    summary = A1RandomBaselineComparator(min_group_sample_size=1).summarize([event], a1_metrics, random_rows)
    overall = next(row for row in summary if row["dimension"] == "ALL")
    assert overall["a1_avg_mfe_r_15m"] == 1
    assert overall["random_avg_mfe_r_15m"] == 2
    assert overall["a1_sample_count"] == 1
    assert overall["random_sample_count"] == 1


def test_hypothesis_summary_excludes_insufficient_future_data():
    event = A1EdgeEvent.from_mapping({"zone_id": "z1", "direction": "BUY", "reaction_event_ts": 30, "last_price": 100, "frozen_low": 99, "frozen_high": 101})
    bars = [
        {"timestamp": 0, "open": 100, "high": 100, "low": 100, "close": 100, "volume": 1},
        {"timestamp": 60, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1},
    ]
    simulator = A1HypothesisSimulator(window_sec=900, min_group_sample_size=1)
    rows = simulator.simulate([event], bars)
    assert any(row.insufficient_future_data for row in rows if not row.is_skipped)
    summary = simulator.summarize(rows)
    overall = next(row for row in summary if row["dimension"] == "ALL")
    assert overall["valid_count"] == 0
