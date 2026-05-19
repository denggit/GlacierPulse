#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategy.phase2_orderflow_evaluator import Phase2OrderflowEvaluator
from src.strategy.phase3_candidate_evaluator import Phase3CandidateEvaluator
from src.strategy.phase3_trade_outcome_evaluator import Phase3OutcomeEvaluator
from src.strategy.virtual_position_manager import VirtualPositionManager


def _frozen_zone(zone_id="iz-1", frozen_ts=100.0):
    return {
        "zone_id": zone_id,
        "direction": "BUY",
        "is_frozen": True,
        "frozen_ts": frozen_ts,
        "frozen_reason": "HIGH_ICEBERG",
        "frozen_state": "DISCOVERED",
        "frozen_event_id": "pie-1",
        "frozen_zone_lower": 3000.0,
        "frozen_zone_upper": 3001.5,
        "live_zone_lower": 3000.0,
        "live_zone_upper": 3001.5,
    }


def test_v62_skeleton_imports():
    assert Phase2OrderflowEvaluator()
    assert Phase3CandidateEvaluator()
    assert VirtualPositionManager()
    assert Phase3OutcomeEvaluator()


def test_phase2_registers_frozen_zone_once(caplog):
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    zone = _frozen_zone()

    with caplog.at_level(logging.INFO):
        assert evaluator.register_frozen_zone(zone, now_ts=101.0) is True
        assert evaluator.register_frozen_zone(zone, now_ts=102.0) is False

    assert len(evaluator.active_zones) == 1
    assert evaluator.get_active_zone("iz-1")["frozen_event_id"] == "pie-1"
    phase2_logs = [
        record.message
        for record in caplog.records
        if "[PHASE2-REGISTERED]" in record.message
    ]
    assert len(phase2_logs) == 1
    assert "zone_id=iz-1" in phase2_logs[0]
    assert "frozen_reason=HIGH_ICEBERG" in phase2_logs[0]


def test_phase2_limits_active_zones_by_oldest():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=2, zone_ttl_seconds=1800)

    assert evaluator.register_frozen_zone(_frozen_zone("iz-1"), now_ts=100.0) is True
    assert evaluator.register_frozen_zone(_frozen_zone("iz-2"), now_ts=101.0) is True
    assert evaluator.register_frozen_zone(_frozen_zone("iz-3"), now_ts=102.0) is True

    assert list(evaluator.active_zones.keys()) == ["iz-2", "iz-3"]


def test_phase2_prunes_expired_zones_before_registering_new_one():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=10)

    assert evaluator.register_frozen_zone(_frozen_zone("iz-old"), now_ts=100.0) is True
    assert evaluator.register_frozen_zone(_frozen_zone("iz-new"), now_ts=111.0) is True

    assert list(evaluator.active_zones.keys()) == ["iz-new"]


def test_phase2_trade_ticks_update_orderflow_windows_and_buy_break_depth():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)

    assert evaluator.register_frozen_zone(_frozen_zone("iz-buy", frozen_ts=100.0), now_ts=100.0) is True
    evaluator.on_trade({"price": 2999.0, "size": 2.0, "side": "buy", "ts": 101.1})
    evaluator.on_trade({"price": 2998.0, "size": 1.0, "side": "sell", "ts": 101.2})

    zone = evaluator.get_active_zone("iz-buy")
    assert zone["active_buy_notional_1s"] == 5998.0
    assert zone["active_sell_notional_1s"] == 2998.0
    assert zone["cvd_delta_3s"] == 3000.0
    assert zone["tick_count_10s"] == 2
    assert zone["min_price_seen_after_frozen"] == 2998.0
    assert zone["max_price_seen_after_frozen"] == 2999.0
    assert zone["break_depth_u"] == 2.0
    assert zone["break_depth_pct"] == 2.0 / 3000.0

    evaluator.on_trade({"price": 3002.0, "size": 1.0, "side": "buy", "ts": 112.0})
    zone = evaluator.get_active_zone("iz-buy")
    assert zone["active_buy_notional_10s"] == 3002.0
    assert zone["active_sell_notional_10s"] == 0.0
    assert zone["tick_count_10s"] == 1


def test_phase2_sell_zone_tracks_upside_break_depth_from_price_updates():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    zone = _frozen_zone("iz-sell", frozen_ts=200.0)
    zone["direction"] = "SELL"

    assert evaluator.register_frozen_zone(zone, now_ts=200.0) is True
    evaluator.on_price(price=3003.0, ts=201.0)

    snapshot = evaluator.get_active_zone("iz-sell")
    assert snapshot["last_price"] == 3003.0
    assert snapshot["sweep_extreme"] == 3003.0
    assert snapshot["break_depth_u"] == 1.5
    assert snapshot["break_depth_pct"] == 1.5 / 3001.5


def test_phase2_bucket_window_boundaries_use_exact_second_count():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)

    assert evaluator.register_frozen_zone(_frozen_zone("iz-boundary", frozen_ts=90.0), now_ts=90.0) is True
    for bucket_ts in range(90, 101):
        evaluator.on_orderflow(
            {
                "ts": float(bucket_ts),
                "active_buy_notional": float(bucket_ts),
                "active_sell_notional": float(bucket_ts * 10),
                "tick_count": 1,
            }
        )
    evaluator.on_price(price=3000.0, ts=100.5)

    zone = evaluator.get_active_zone("iz-boundary")
    assert zone["active_buy_notional_1s"] == 100.0
    assert zone["active_sell_notional_1s"] == 1000.0
    assert zone["tick_count_1s"] == 1
    assert zone["active_buy_notional_3s"] == 98.0 + 99.0 + 100.0
    assert zone["active_sell_notional_3s"] == 980.0 + 990.0 + 1000.0
    assert zone["tick_count_3s"] == 3
    assert zone["active_buy_notional_10s"] == sum(float(ts) for ts in range(91, 101))
    assert zone["active_sell_notional_10s"] == sum(float(ts * 10) for ts in range(91, 101))
    assert zone["tick_count_10s"] == 10


def test_phase2_expired_log_includes_expire_reason(caplog):
    ttl_evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=10)
    capacity_evaluator = Phase2OrderflowEvaluator(max_active_zones=1, zone_ttl_seconds=1800)

    with caplog.at_level(logging.INFO):
        assert ttl_evaluator.register_frozen_zone(_frozen_zone("iz-ttl"), now_ts=100.0) is True
        assert ttl_evaluator.register_frozen_zone(_frozen_zone("iz-new"), now_ts=111.0) is True
        assert capacity_evaluator.register_frozen_zone(_frozen_zone("iz-cap-1"), now_ts=200.0) is True
        assert capacity_evaluator.register_frozen_zone(_frozen_zone("iz-cap-2"), now_ts=201.0) is True

    expired_logs = [
        record.getMessage()
        for record in caplog.records
        if "[PHASE2-ZONE-EXPIRED]" in record.getMessage()
    ]
    assert any("zone_id=iz-ttl" in message and "expire_reason=TTL" in message for message in expired_logs)
    assert any(
        "zone_id=iz-cap-1" in message and "expire_reason=CAPACITY_LIMIT" in message
        for message in expired_logs
    )
