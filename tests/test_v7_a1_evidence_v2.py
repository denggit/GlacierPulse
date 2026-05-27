#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.zone_truth.a1_evidence_v2 import attach_a1_evidence_v2, classify_a1_evidence_event


def test_hidden_reload_sets_flag():
    out = classify_a1_evidence_event({"result": "ICEBERG", "active_notional": 1_000_000})
    assert out["hidden_reload_iceberg_flag"] is True
    assert out["a1_primary_evidence_type"] == "HIDDEN_RELOAD_ICEBERG"


def test_visible_wall_absorption_sets_flag():
    out = classify_a1_evidence_event(
        {
            "direction": "BUY",
            "result": "IGNORE",
            "trigger_price": 100,
            "settle_price": 100,
            "start_thickness_usdt": 10_000_000,
            "active_notional": 5_000_000,
            "end_thickness_usdt": 5_000_000,
            "book_reduction": 5_000_000,
        }
    )
    assert out["visible_wall_absorption_flag"] is True
    assert out["visible_wall_withdrawal_excess_ratio"] == 0


def test_spoofing_withdrawal_blocks_visible_wall():
    out = classify_a1_evidence_event(
        {
            "direction": "BUY",
            "trigger_price": 100,
            "settle_price": 100,
            "start_thickness_usdt": 10_000_000,
            "active_notional": 1_000_000,
            "end_thickness_usdt": 1_000_000,
            "book_reduction": 9_000_000,
        }
    )
    assert out["spoofing_withdrawal_flag"] is True
    assert out["visible_wall_absorption_flag"] is False


def test_failed_wall_flag_when_price_breaks_far():
    out = classify_a1_evidence_event(
        {
            "direction": "BUY",
            "trigger_price": 100,
            "settle_price": 97,
            "min_trade_price": 97,
            "start_thickness_usdt": 2_000_000,
            "active_notional": 2_000_000,
            "end_thickness_usdt": 0,
            "book_reduction": 2_000_000,
        }
    )
    assert out["failed_wall_flag"] is True


def test_cluster_and_ladder_absorption_flags():
    pies = [
        {"event_key": "p1", "direction": "BUY", "settle_ts": 1000, "result": "ICEBERG", "active_notional": 400_000, "min_trade_price": 100.0, "max_trade_price": 100.0},
        {"event_key": "p2", "direction": "BUY", "settle_ts": 1004, "result": "ICEBERG", "active_notional": 400_000, "min_trade_price": 100.5, "max_trade_price": 100.5},
        {"event_key": "p3", "direction": "BUY", "settle_ts": 1008, "result": "ICEBERG", "active_notional": 400_000, "min_trade_price": 101.0, "max_trade_price": 101.0},
    ]
    out = attach_a1_evidence_v2({"zone_id": "z1", "direction": "BUY"}, pies)
    assert out["cluster_absorption_flag"] is True
    assert out["ladder_absorption_flag"] is True
