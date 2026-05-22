#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

from src.research.phase1_truth.recorder import Phase1CandidateRecorder
from src.research.phase1_truth.tracker import Phase1TruthTracker


def _candidate(event_key="e1", ts=100.0, result="ICEBERG"):
    return {
        "event_key": event_key,
        "symbol": "ETH-USDT-SWAP",
        "direction": "BUY",
        "result": result,
        "trigger_ts": ts - 0.2,
        "settle_ts": ts,
        "trigger_recv_ts": ts - 0.2,
        "settle_recv_ts": ts,
        "trigger_price": 100,
        "settle_price": 100,
        "zone_lower": 99,
        "zone_upper": 100.5,
        "local_zone_width": 1.5,
        "active_notional": 1_000_000,
        "active_side_ratio": 1.0,
        "trade_count": 20,
        "hidden_volume": 1_500_000,
        "absorption_rate": 0.8,
        "book_reduction": 100_000,
        "start_thickness_usdt": 800_000,
        "end_thickness_usdt": 700_000,
        "price_displacement": 0,
        "session_tag": "US_OPEN",
    }


def _read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_candidate_settled_writes_jsonl(tmp_path):
    path = tmp_path / "phase1_candidates.jsonl"
    recorder = Phase1CandidateRecorder(jsonl_path=str(path))
    assert recorder.record_settled(_candidate())
    rows = _read_jsonl(path)
    assert rows[0]["record_type"] == "candidate_settled"


def test_candidate_finalized_writes_jsonl_and_preserves_jsonl(tmp_path):
    path = tmp_path / "phase1_candidates.jsonl"
    recorder = Phase1CandidateRecorder(jsonl_path=str(path))
    recorder.record_settled(_candidate("e1"))
    recorder.record_finalized({**_candidate("e1"), "truth_score_total": 88})
    rows = _read_jsonl(path)
    assert [row["record_type"] for row in rows] == ["candidate_settled", "candidate_finalized"]
    assert all(isinstance(row, dict) for row in rows)


def test_tracker_finalizes_after_120s_window(tmp_path):
    path = tmp_path / "phase1_candidates.jsonl"
    tracker = Phase1TruthTracker(
        recorder=Phase1CandidateRecorder(jsonl_path=str(path)),
        finalize_after_sec=120,
        max_active_observations=10,
    )
    tracker.register_candidate_settlement(_candidate(ts=100))
    tracker.on_trade({"ts": 221, "recv_ts": 221, "price": 100.1, "size": 1, "side": "buy"})
    rows = _read_jsonl(path)
    assert rows[-1]["record_type"] == "candidate_finalized"
    assert rows[-1]["post_features"]["finalize_reason"] == "finalize_after_sec"
    assert "observation_age_sec" in rows[-1]["post_features"]
    assert "has_any_post_trade" in rows[-1]["post_features"]
    assert "has_book_recovery_data" in rows[-1]["post_features"]
    assert "has_5s_trade_window" in rows[-1]["post_features"]
    assert "has_30s_trade_window" in rows[-1]["post_features"]
    assert "has_120s_observation" in rows[-1]["post_features"]


def test_tracker_capacity_guard_does_not_crash(tmp_path):
    path = tmp_path / "phase1_candidates.jsonl"
    tracker = Phase1TruthTracker(
        recorder=Phase1CandidateRecorder(jsonl_path=str(path)),
        max_active_observations=1,
    )
    tracker.register_candidate_settlement(_candidate("e1"))
    tracker.register_candidate_settlement(_candidate("e2"))
    rows = _read_jsonl(path)
    assert any(row.get("dropped_due_to_capacity") for row in rows if row["record_type"] == "candidate_finalized")
