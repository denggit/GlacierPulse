import csv
import json

from src.research.a1_edge.metadata import build_run_metadata, get_git_commit
from src.research.a1_edge.report_builder import A1EdgeReportBuilder


def test_run_metadata_file_hash_counts_and_parameters(tmp_path):
    events = tmp_path / "events.jsonl"
    events.write_text('{"a": 1}\n\n{"b": 2}\n', encoding="utf-8")
    klines = tmp_path / "klines.csv"
    klines.write_text("timestamp,open,high,low,close,volume\n1,1,2,0,1,10\n2,1,2,0,1,11\n", encoding="utf-8")
    params = {
        "symbol": "ETH-USDT-SWAP",
        "windows": [60, 900],
        "random_samples_per_event": 10,
        "seed": 42,
        "min_risk_u": 1.0,
        "min_risk_pct": 0.0003,
        "roundtrip_fee_pct": 0.001,
        "baseline_risk_mode": "source",
        "kline_timezone": "Asia/Shanghai",
        "exclude_near_a1_minutes": 5.0,
        "min_group_sample_size": 30,
    }
    metadata = build_run_metadata(events, klines, tmp_path / "out", params, repo_root=tmp_path)
    assert len(metadata["events_file_sha256"]) == 64
    assert len(metadata["klines_file_sha256"]) == 64
    assert metadata["events_file_line_count"] == 2
    assert metadata["klines_file_row_count"] == 2
    assert metadata["analysis_parameters"]["roundtrip_fee_pct"] == 0.001
    assert metadata["analysis_parameters"]["kline_timezone"] == "Asia/Shanghai"
    assert metadata["analysis_parameters"]["seed"] == 42
    assert metadata["analysis_parameters"]["random_samples_per_event"] == 10


def test_git_commit_failure_returns_unknown(monkeypatch, tmp_path):
    monkeypatch.delenv("GLACIER_GIT_COMMIT", raising=False)

    def _raise(*args, **kwargs):
        raise OSError("git unavailable")

    monkeypatch.setattr("src.research.a1_edge.metadata.subprocess.run", _raise)
    assert get_git_commit(tmp_path) == "UNKNOWN"


def test_summary_json_and_markdown_include_metadata_and_fee_model(tmp_path):
    metadata = {
        "events_file_name": "events.jsonl",
        "events_file_sha256": "abcdef1234567890",
        "events_file_line_count": 11,
        "klines_file_name": "k.csv",
        "klines_file_sha256": "123456abcdef7890",
        "klines_file_row_count": 1440,
        "git_commit": "fedcba9876543210",
        "analysis_parameters": {
            "kline_timezone": "Asia/Shanghai",
            "roundtrip_fee_pct": 0.001,
        },
    }
    fee_model = {
        "fee_model": "roundtrip_notional_pct",
        "roundtrip_fee_pct": 0.001,
        "description": "Fee-aware net R subtracts entry_price * roundtrip_fee_pct from favorable movement.",
    }
    event = {"zone_id": "z1", "event_key": "z1|A|CONFIRMED|1.0", "direction": "BUY", "event_ts": 1, "a1_reaction_type": "A"}
    random_summary = [
        {
            "dimension": "ALL",
            "group": "ALL",
            "edge_label": "STRONG_DIRECTIONAL_EDGE",
            "a1_sample_count": 1,
            "a1_avg_net_mfe_r_15m": 1.2,
            "random_avg_net_mfe_r_15m": 0.5,
            "net_mfe_edge_15m": 0.7,
            "a1_net_hit_1r_rate_15m": 0.6,
            "random_net_hit_1r_rate_15m": 0.2,
            "net_hit_1r_edge_15m": 0.4,
            "a1_avg_fee_share_r_15m": 0.3,
            "a1_avg_raw_mfe_r_15m": 1.5,
            "random_avg_raw_mfe_r_15m": 0.6,
            "raw_mfe_edge_15m": 0.9,
        }
    ]
    A1EdgeReportBuilder(min_group_sample_size=1, min_total_events=1).build(
        events=[event],
        forward_metrics=[],
        random_baseline=[{"baseline_id": f"r{i}"} for i in range(5)],
        random_summary=random_summary,
        hypothesis_results=[],
        hypothesis_summary=[{"valid_count": 1, "avg_realized_r_proxy": 0.1}],
        out_dir=tmp_path,
        metadata=metadata,
        fee_model=fee_model,
    )
    summary = json.loads((tmp_path / "a1_edge_summary.json").read_text(encoding="utf-8"))
    assert summary["metadata"]["events_file_sha256"] == metadata["events_file_sha256"]
    assert summary["fee_model"]["roundtrip_fee_pct"] == 0.001
    assert summary["decision_basis"]["uses_fee_aware_metrics"] is True

    report = (tmp_path / "a1_go_no_go_report.md").read_text(encoding="utf-8")
    assert "abcdef123456" in report
    assert "123456abcdef" in report
    assert "fedcba987654" in report
    assert "roundtrip_fee_pct: 0.001" in report
    assert "fee_model: roundtrip_notional_pct" in report
    assert "raw metrics are diagnostic only" in report


def test_cli_metadata_and_report_hashes(tmp_path):
    base = 1_779_373_200
    events = tmp_path / "events.jsonl"
    events.write_text(
        '{"zone_id":"z1","direction":"BUY","reaction_event_ts":%d,"last_price":2000,"frozen_low":1998,"frozen_high":2002}\n'
        % (base + 60),
        encoding="utf-8",
    )
    klines = tmp_path / "k.csv"
    with klines.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        for i in range(20):
            writer.writerow([base + i * 60, 2000, 2004, 1999, 2001, 1])

    out = tmp_path / "out"
    from tools.analyze_a1_edge import main

    code = main(
        [
            "--events",
            str(events),
            "--klines",
            str(klines),
            "--out",
            str(out),
            "--random-samples-per-event",
            "1",
            "--seed",
            "42",
            "--kline-timezone",
            "Asia/Shanghai",
            "--roundtrip-fee-pct",
            "0.001",
            "--exclude-near-a1-minutes",
            "0",
        ]
    )
    assert code == 0
    metadata = json.loads((out / "a1_run_metadata.json").read_text(encoding="utf-8"))
    assert metadata["events_file_sha256"]
    assert metadata["klines_file_sha256"]
    assert metadata["events_file_line_count"] == 1
    assert metadata["klines_file_row_count"] == 20
    assert metadata["git_commit"]
    assert metadata["analysis_parameters"]["seed"] == 42
    assert metadata["analysis_parameters"]["random_samples_per_event"] == 1
    summary = json.loads((out / "a1_edge_summary.json").read_text(encoding="utf-8"))
    assert "metadata" in summary
    assert "fee_model" in summary
    assert "decision_basis" in summary
    report = (out / "a1_go_no_go_report.md").read_text(encoding="utf-8")
    assert metadata["events_file_sha256"][:12] in report
    assert metadata["klines_file_sha256"][:12] in report
