#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategy.phase2_orderflow_evaluator import Phase2OrderflowEvaluator
from src.strategy.phase1_zone_engine import Phase1Engine
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


def _sell_frozen_zone(zone_id="iz-sell", frozen_ts=100.0):
    zone = _frozen_zone(zone_id=zone_id, frozen_ts=frozen_ts)
    zone["direction"] = "SELL"
    return zone


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

    snapshot = evaluator.get_active_zone("iz-1")
    for field in (
        "previous_state",
        "state_updated_ts",
        "state_entered_ts",
        "has_tested_zone",
        "has_swept_boundary",
        "has_absorbed_after_sweep",
        "has_reclaimed_boundary",
        "has_retested_inside_zone",
        "has_failed",
        "has_confirmed",
        "time_below_boundary_ms",
        "time_above_boundary_ms",
        "absorption_score",
        "pressure_decay_score",
        "reclaim_score",
        "retest_score",
        "phase2_total_score",
        "phase2_type",
        "book_update_count",
        "bid_depth_near_zone",
        "ask_depth_near_zone",
        "bid_depth_near_sweep",
        "ask_depth_near_sweep",
        "bid_reload_count",
        "ask_reload_count",
        "bid_reduction_1s",
        "ask_reduction_1s",
        "book_absorption_score",
        "relevant_book_depth_available",
        "reload_score",
        "last_book_ts",
    ):
        assert field in snapshot


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


def test_phase2_book_update_tracks_local_depth_reload_and_absorption_for_buy_zone():
    evaluator = Phase2OrderflowEvaluator(
        max_active_zones=20,
        zone_ttl_seconds=1800,
        book_near_zone_range_usdt=1.0,
        book_near_sweep_range_usdt=1.0,
    )

    assert evaluator.register_frozen_zone(_frozen_zone("iz-book", frozen_ts=100.0), now_ts=100.0) is True
    evaluator.on_trade({"price": 2999.0, "size": 50.0, "side": "sell", "ts": 101.0})
    evaluator.on_book_update(
        {
            "ts": 101.1,
            "bids": {3000.0: 40.0, 2999.2: 5.0, 2998.0: 100.0},
            "asks": {3000.5: 3.0, 3002.0: 100.0},
        }
    )
    evaluator.on_book_update(
        {
            "ts": 101.2,
            "bids": {3000.0: 20.0, 2999.2: 5.0, 2998.0: 100.0},
            "asks": {3000.5: 3.0, 3002.0: 100.0},
        }
    )

    snapshot_after_drop = evaluator.get_active_zone("iz-book")
    assert snapshot_after_drop["book_update_count"] == 2
    assert snapshot_after_drop["bid_depth_near_zone"] == 3000.0 * 20.0 + 2999.2 * 5.0
    assert snapshot_after_drop["ask_depth_near_zone"] == 3000.5 * 3.0
    assert snapshot_after_drop["bid_reduction_1s"] > 0
    assert snapshot_after_drop["relevant_book_depth_available"] is True
    assert 0.0 < snapshot_after_drop["book_absorption_score"] <= 1.0

    evaluator.on_book_update(
        {
            "ts": 101.4,
            "bids": {3000.0: 38.0, 2999.2: 5.0, 2998.0: 100.0},
            "asks": {3000.5: 3.0, 3002.0: 100.0},
        }
    )
    snapshot_after_reload = evaluator.get_active_zone("iz-book")
    assert snapshot_after_reload["bid_reload_count"] == 1
    assert snapshot_after_reload["reload_score"] > 0.0


def test_phase2_buy_book_absorption_is_zero_when_bid_depth_is_unavailable():
    evaluator = Phase2OrderflowEvaluator(
        max_active_zones=20,
        zone_ttl_seconds=1800,
        book_near_zone_range_usdt=1.0,
        book_near_sweep_range_usdt=1.0,
    )

    assert evaluator.register_frozen_zone(_frozen_zone("iz-buy-no-depth", frozen_ts=100.0), now_ts=100.0) is True
    evaluator.on_trade({"price": 2999.0, "size": 50.0, "side": "sell", "ts": 101.0})
    evaluator.on_book_update(
        {
            "ts": 101.1,
            "bids": {3100.0: 100.0},
            "asks": {3101.0: 100.0},
        }
    )

    snapshot = evaluator.get_active_zone("iz-buy-no-depth")
    assert snapshot["active_sell_notional_1s"] > 0
    assert snapshot["bid_depth_near_zone"] == 0.0
    assert snapshot["bid_depth_near_sweep"] == 0.0
    assert snapshot["relevant_book_depth_available"] is False
    assert snapshot["book_absorption_score"] == 0.0


def test_phase2_sell_book_absorption_is_zero_when_ask_depth_is_unavailable():
    evaluator = Phase2OrderflowEvaluator(
        max_active_zones=20,
        zone_ttl_seconds=1800,
        book_near_zone_range_usdt=1.0,
        book_near_sweep_range_usdt=1.0,
    )
    zone = _frozen_zone("iz-sell-no-depth", frozen_ts=100.0)
    zone["direction"] = "SELL"

    assert evaluator.register_frozen_zone(zone, now_ts=100.0) is True
    evaluator.on_trade({"price": 3003.0, "size": 50.0, "side": "buy", "ts": 101.0})
    evaluator.on_book_update(
        {
            "ts": 101.1,
            "bids": {3001.5: 100.0},
            "asks": {3100.0: 100.0},
        }
    )

    snapshot = evaluator.get_active_zone("iz-sell-no-depth")
    assert snapshot["active_buy_notional_1s"] > 0
    assert snapshot["ask_depth_near_zone"] == 0.0
    assert snapshot["ask_depth_near_sweep"] == 0.0
    assert snapshot["relevant_book_depth_available"] is False
    assert snapshot["book_absorption_score"] == 0.0


def test_phase2_book_update_skips_incomplete_book_data():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)

    assert evaluator.register_frozen_zone(_frozen_zone("iz-skip", frozen_ts=100.0), now_ts=100.0) is True
    evaluator.on_book_update({"ts": 101.0, "bids": [[3000.0, 10.0]]})

    snapshot = evaluator.get_active_zone("iz-skip")
    assert snapshot["book_update_count"] == 0


def test_phase1_book_update_bypasses_phase2_and_catches_failures(caplog):
    class _Ctx:
        current_price = 3000.0
        bids = {3000.0: 1.0}
        asks = {3001.0: 1.0}

    class _Phase2Spy:
        def __init__(self):
            self.payload = None

        def on_book_update(self, book_data):
            self.payload = book_data

    class _Phase2Boom:
        def on_book_update(self, book_data):
            raise RuntimeError("book failed")

    engine = Phase1Engine(_Ctx(), iceberg_detector=None)
    spy = _Phase2Spy()
    engine.phase2_orderflow_evaluator = spy
    assert engine.on_book_update({"ts": 101.0, "recv_ts": 101.0, "bids": [], "asks": []}) is None
    assert spy.payload["bids"] is engine.ctx.bids
    assert spy.payload["asks"] is engine.ctx.asks

    engine.phase2_orderflow_evaluator = _Phase2Boom()
    with caplog.at_level(logging.ERROR):
        assert engine.on_book_update({"ts": 102.0, "recv_ts": 102.0, "bids": [], "asks": []}) is None

    assert any("[PHASE2-BOOK-FAILED]" in record.getMessage() for record in caplog.records)


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
    assert all("relevant_book_depth_available=" in message for message in expired_logs)


def test_phase2_buy_clean_sweep_reclaim_path_confirms(caplog):
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    assert evaluator.register_frozen_zone(_frozen_zone("iz-buy-clean", frozen_ts=100.0), now_ts=100.0) is True

    with caplog.at_level(logging.INFO):
        evaluator.on_price(price=3000.2, ts=101.0)
        evaluator.on_trade({"price": 2999.0, "size": 50.0, "side": "sell", "ts": 101.1})
        evaluator.on_book_update(
            {
                "ts": 101.2,
                "bids": {3000.0: 80.0, 2999.2: 20.0},
                "asks": {3000.5: 5.0, 3002.0: 100.0},
            }
        )
        evaluator.on_trade({"price": 3000.1, "size": 50.0, "side": "buy", "ts": 102.2})
        evaluator.on_price(price=3000.3, ts=103.0)

    snapshot = evaluator.get_active_zone("iz-buy-clean")
    assert snapshot["state"] == "PHASE2_CONFIRMED"
    assert snapshot["phase2_type"] == "SWEEP_RECLAIM"
    assert snapshot["has_tested_zone"] is True
    assert snapshot["has_swept_boundary"] is True
    assert snapshot["has_absorbed_after_sweep"] is True
    assert snapshot["has_reclaimed_boundary"] is True
    assert snapshot["has_retested_inside_zone"] is True
    assert snapshot["phase2_total_score"] >= evaluator.min_total_score

    messages = [record.getMessage() for record in caplog.records]
    assert any("[PHASE2-STATE]" in message and "state=PHASE2_CONFIRMED" in message for message in messages)
    assert any("[PHASE2-CONFIRMED]" in message and "phase2_type=SWEEP_RECLAIM" in message for message in messages)


def test_phase2_sell_clean_sweep_reclaim_path_confirms():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    assert evaluator.register_frozen_zone(_sell_frozen_zone("iz-sell-clean", frozen_ts=100.0), now_ts=100.0) is True

    evaluator.on_trade({"price": 3002.0, "size": 50.0, "side": "buy", "ts": 101.1})
    evaluator.on_book_update(
        {
            "ts": 101.2,
            "bids": {3001.0: 5.0, 2999.0: 100.0},
            "asks": {3001.5: 80.0, 3002.0: 20.0},
        }
    )
    evaluator.on_trade({"price": 3001.4, "size": 50.0, "side": "sell", "ts": 102.2})
    evaluator.on_price(price=3001.2, ts=103.0)

    snapshot = evaluator.get_active_zone("iz-sell-clean")
    assert snapshot["state"] == "PHASE2_CONFIRMED"
    assert snapshot["phase2_type"] == "SWEEP_RECLAIM"
    assert snapshot["has_swept_boundary"] is True
    assert snapshot["has_absorbed_after_sweep"] is True
    assert snapshot["has_reclaimed_boundary"] is True
    assert snapshot["has_retested_inside_zone"] is True


def test_phase2_buy_without_relevant_book_depth_does_not_absorb_from_book_score():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    assert evaluator.register_frozen_zone(_frozen_zone("iz-no-depth-state", frozen_ts=100.0), now_ts=100.0) is True

    evaluator.on_trade({"price": 2999.0, "size": 50.0, "side": "sell", "ts": 101.1})
    evaluator.on_book_update(
        {
            "ts": 101.2,
            "bids": {3100.0: 100.0},
            "asks": {3101.0: 100.0},
        }
    )

    snapshot = evaluator.get_active_zone("iz-no-depth-state")
    assert snapshot["relevant_book_depth_available"] is False
    assert snapshot["book_absorption_score"] == 0.0
    assert snapshot["state"] == "PHASE2_SWEEPING_LOW"
    assert snapshot["has_absorbed_after_sweep"] is False


def test_phase2_break_depth_soft_does_not_fail():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    assert evaluator.register_frozen_zone(_frozen_zone("iz-soft-depth", frozen_ts=100.0), now_ts=100.0) is True

    evaluator.on_price(price=2988.0, ts=101.0)

    snapshot = evaluator.get_active_zone("iz-soft-depth")
    assert snapshot["break_depth_pct"] > evaluator.max_sweep_depth_pct_soft
    assert snapshot["break_depth_pct"] < evaluator.max_sweep_depth_pct_hard
    assert snapshot["state"] == "PHASE2_SWEEPING_LOW"
    assert snapshot["has_failed"] is False


def test_phase2_break_depth_hard_fails():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    assert evaluator.register_frozen_zone(_frozen_zone("iz-hard-depth", frozen_ts=100.0), now_ts=100.0) is True

    evaluator.on_price(price=2975.9, ts=101.0)

    snapshot = evaluator.get_active_zone("iz-hard-depth")
    assert snapshot["break_depth_pct"] >= evaluator.max_sweep_depth_pct_hard
    assert snapshot["state"] == "PHASE2_FAILED"
    assert snapshot["has_failed"] is True
    assert snapshot["phase2_reason"] == "hard_sweep_depth_exceeded"


def test_phase2_timeout_transitions_before_prune(caplog):
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=10)
    assert evaluator.register_frozen_zone(_frozen_zone("iz-timeout-state", frozen_ts=100.0), now_ts=100.0) is True

    with caplog.at_level(logging.INFO):
        evaluator.on_price(price=3000.0, ts=111.1)

    assert evaluator.get_active_zone("iz-timeout-state") is None
    messages = [record.getMessage() for record in caplog.records]
    assert any("[PHASE2-STATE]" in message and "state=PHASE2_TIMEOUT" in message for message in messages)
    assert any("[PHASE2-ZONE-EXPIRED]" in message and "expire_reason=TTL" in message for message in messages)


def test_phase2_state_machine_does_not_call_trader(monkeypatch):
    from src.execution.trader import IcebergTrader

    called = {"process_signal": 0, "request": 0}

    async def _boom_process_signal(self, signal, current_price):
        called["process_signal"] += 1
        raise AssertionError("real trader must not be called")

    async def _boom_request(self, method, endpoint, payload=None):
        called["request"] += 1
        raise AssertionError("order API must not be called")

    monkeypatch.setattr(IcebergTrader, "process_signal", _boom_process_signal)
    monkeypatch.setattr(IcebergTrader, "_request", _boom_request)

    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    assert evaluator.register_frozen_zone(_frozen_zone("iz-no-trade", frozen_ts=100.0), now_ts=100.0) is True
    evaluator.on_price(price=3000.2, ts=101.0)
    evaluator.on_trade({"price": 2999.0, "size": 50.0, "side": "sell", "ts": 101.1})
    evaluator.on_book_update(
        {
            "ts": 101.2,
            "bids": {3000.0: 80.0},
            "asks": {3000.5: 5.0},
        }
    )
    evaluator.on_trade({"price": 3000.1, "size": 50.0, "side": "buy", "ts": 102.2})
    evaluator.on_price(price=3000.3, ts=103.0)

    assert evaluator.get_active_zone("iz-no-trade")["state"] == "PHASE2_CONFIRMED"
    assert called == {"process_signal": 0, "request": 0}
