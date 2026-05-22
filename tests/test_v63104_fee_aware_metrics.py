import csv
import json
import subprocess
import sys

from src.research.a1_edge.forward_metrics import A1ForwardMetricsAnalyzer, compute_forward_metric
from src.research.a1_edge.hypothesis_simulator import A1HypothesisSimulator
from src.research.a1_edge.random_baseline import A1RandomBaselineComparator, RandomBaselineSampler
from src.research.a1_edge.schema import A1EdgeEvent


def _event(entry=2000.0):
    return A1EdgeEvent.from_mapping(
        {
            "zone_id": "z1",
            "direction": "BUY",
            "reaction_event_ts": 0,
            "last_price": entry,
            "frozen_low": entry - 2,
            "frozen_high": entry + 2,
            "a1_reaction_type": "A",
            "reaction_event_kind": "CONFIRMED",
        }
    )


def _bars(high):
    return [
        {"timestamp": 0, "open": 2000, "high": 2000, "low": 2000, "close": 2000, "volume": 1},
        {"timestamp": 60, "open": 2000, "high": high, "low": 2000, "close": high, "volume": 1},
    ]


def _epoch_bars(high=2004, n=20, base=1_779_373_200):
    return [
        {"timestamp": base + i * 60, "open": 2000, "high": high, "low": 1999, "close": 2001, "volume": 1}
        for i in range(n)
    ]


def test_fee_share_and_net_hits_for_forward_metrics():
    metric = compute_forward_metric(
        _event(),
        _bars(2002),
        60,
        entry_price=2000,
        event_ts=0,
        risk_u_override=2,
        roundtrip_fee_pct=0.001,
    )
    assert metric.fee_u == 2
    assert metric.fee_share_r == 1.0
    assert metric.directional_mfe_r == 1.0
    assert metric.net_directional_mfe_r == 0.0
    assert metric.hit_plus_1r is True
    assert metric.net_hit_plus_1r is False

    two_r = compute_forward_metric(
        _event(),
        _bars(2004),
        60,
        entry_price=2000,
        event_ts=0,
        risk_u_override=2,
        roundtrip_fee_pct=0.001,
    )
    assert two_r.directional_mfe_r == 2.0
    assert two_r.net_directional_mfe_r == 1.0
    assert two_r.net_hit_plus_1r is True


def test_small_risk_keeps_raw_hit_but_rejects_net_hit():
    metric = compute_forward_metric(
        _event(),
        _bars(2000.5),
        60,
        entry_price=2000,
        event_ts=0,
        risk_u_override=0.5,
        roundtrip_fee_pct=0.001,
    )
    assert metric.fee_share_r > 1.0
    assert metric.hit_plus_1r is True
    assert metric.net_hit_plus_1r is False


def test_random_baseline_csv_outputs_fee_aware_fields(tmp_path):
    base = 1_779_373_200
    klines = _epoch_bars(n=8, base=base)
    event = A1EdgeEvent.from_mapping(
        {
            "zone_id": "z1",
            "direction": "BUY",
            "reaction_event_ts": base + 30,
            "last_price": 2000,
            "frozen_low": 1998,
            "frozen_high": 2002,
        }
    )
    baseline = RandomBaselineSampler(
        samples_per_event=1,
        random_seed=1,
        exclude_near_a1_minutes=0,
        windows_sec=[60],
        roundtrip_fee_pct=0.001,
    ).sample([event], klines)
    summary = A1RandomBaselineComparator(min_group_sample_size=1).summarize([event], [], baseline)
    A1RandomBaselineComparator(min_group_sample_size=1).export(baseline, summary, tmp_path)
    row = next(csv.DictReader((tmp_path / "a1_random_baseline.csv").open(encoding="utf-8")))
    assert "net_directional_mfe_r" in row
    assert "net_hit_plus_1r" in row
    assert "fee_share_r" in row


def test_comparator_edge_label_uses_net_fields_not_raw_fields():
    event = _event()
    a1_metrics = [
        {
            "event_key": event.event_key,
            "window_sec": 900,
            "directional_mfe_r": 10.0,
            "net_directional_mfe_r": 0.5,
            "hit_plus_1r": True,
            "net_hit_plus_1r": False,
            "total_range_u": 1,
        }
    ]
    random_rows = [
        {
            "source_event_key": event.event_key,
            "window_sec": 900,
            "directional_mfe_r": 1.0,
            "net_directional_mfe_r": 1.0,
            "hit_plus_1r": False,
            "net_hit_plus_1r": True,
            "total_range_u": 1,
        }
    ]
    summary = A1RandomBaselineComparator(min_group_sample_size=1).summarize([event], a1_metrics, random_rows)
    overall = next(row for row in summary if row["dimension"] == "ALL")
    assert overall["raw_mfe_edge_15m"] > 0
    assert overall["net_mfe_edge_15m"] <= 0
    assert overall["edge_label"] != "STRONG_DIRECTIONAL_EDGE"


def test_forward_random_summary_csvs_include_net_fields(tmp_path):
    base = 1_779_373_200
    event = A1EdgeEvent.from_mapping(
        {
            "zone_id": "z1",
            "direction": "BUY",
            "reaction_event_ts": base + 30,
            "last_price": 2000,
            "frozen_low": 1998,
            "frozen_high": 2002,
        }
    )
    klines = _epoch_bars(base=base)
    forward = A1ForwardMetricsAnalyzer(windows_sec=[60, 900], min_risk_u=1, roundtrip_fee_pct=0.001)
    forward_rows = forward.analyze([event], klines)
    forward.export(forward_rows, tmp_path)
    fwd_row = next(csv.DictReader((tmp_path / "a1_forward_metrics.csv").open(encoding="utf-8")))
    assert {"fee_share_r", "net_directional_mfe_r", "net_hit_plus_1r"}.issubset(fwd_row)

    baseline = RandomBaselineSampler(samples_per_event=1, exclude_near_a1_minutes=0, windows_sec=[900]).sample([event], klines)
    summary = A1RandomBaselineComparator(min_group_sample_size=1).summarize(
        [event], [row.to_dict() for row in forward_rows], baseline
    )
    A1RandomBaselineComparator(min_group_sample_size=1).export(baseline, summary, tmp_path)
    summary_row = next(csv.DictReader((tmp_path / "a1_vs_random_summary.csv").open(encoding="utf-8")))
    assert {"a1_avg_net_mfe_r_15m", "random_avg_net_mfe_r_15m", "net_mfe_edge_15m"}.issubset(summary_row)
    assert {"a1_net_hit_1r_rate_15m", "random_net_hit_1r_rate_15m", "net_hit_1r_edge_15m"}.issubset(summary_row)


def test_hypothesis_realized_proxy_still_subtracts_fee():
    base = 1_779_373_200
    event = A1EdgeEvent.from_mapping(
        {
            "zone_id": "z1",
            "direction": "BUY",
            "reaction_event_ts": base,
            "last_price": 2000,
            "frozen_low": 1998,
            "frozen_high": 2002,
            "a1_reaction_type": "A",
        }
    )
    rows = A1HypothesisSimulator(roundtrip_fee_pct=0.001, stop_buffer_u=0, min_group_sample_size=1).simulate(
        [event], _epoch_bars(high=2002, base=base)
    )
    immediate = next(row for row in rows if row.hypothesis_type == "IMMEDIATE_ENTRY")
    assert immediate.fee_u == 2
    assert immediate.fee_share_r == 1.0
    assert immediate.hit_1r is True
    assert immediate.net_hit_1r is False
    assert immediate.realized_r_proxy == 0.0


def test_cli_roundtrip_fee_pct_reaches_outputs(tmp_path):
    events = tmp_path / "events.jsonl"
    base = 1_779_373_200
    events.write_text(json.dumps({"zone_id": "z1", "direction": "BUY", "reaction_event_ts": base + 60, "last_price": 2000, "frozen_low": 1998, "frozen_high": 2002}) + "\n", encoding="utf-8")
    klines = tmp_path / "k.csv"
    klines.write_text(
        "timestamp,open,high,low,close,volume\n"
        + "\n".join(f"{base + i * 60},2000,2004,1999,2001,1" for i in range(20))
        + "\n",
        encoding="utf-8",
    )
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
            "--roundtrip-fee-pct",
            "0.002",
            "--random-samples-per-event",
            "1",
            "--exclude-near-a1-minutes",
            "0",
        ],
        cwd=str(__import__("pathlib").Path(__file__).resolve().parents[1]),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "[A1-EDGE-FEE] roundtrip_fee_pct=0.002" in result.stdout
    metadata = json.loads((out / "a1_run_metadata.json").read_text(encoding="utf-8"))
    assert metadata["analysis_parameters"]["roundtrip_fee_pct"] == 0.002
    row = next(csv.DictReader((out / "a1_forward_metrics.csv").open(encoding="utf-8")))
    assert float(row["roundtrip_fee_pct"]) == 0.002
