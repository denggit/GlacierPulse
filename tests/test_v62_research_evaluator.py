#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategy.phase2_orderflow_evaluator import Phase2OrderflowEvaluator
from src.strategy.phase1_zone_engine import Phase1Engine
from src.strategy.phase3_candidate_evaluator import Phase3CandidateEvaluator
from config import research_evaluator as research_config
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


def _phase2_confirmed_event(
    zone_id="p3-buy",
    direction="BUY",
    phase2_type="SWEEP_RECLAIM",
    last_price=3000.3,
    suggested_stop=2998.5,
    phase2_total_score=0.83,
    absorption_score=0.9,
    relevant_book_depth_available=True,
):
    return {
        "zone_id": zone_id,
        "direction": direction,
        "state": "PHASE2_CONFIRMED",
        "phase2_type": phase2_type,
        "confirmed_ts": 103.0,
        "last_price": last_price,
        "frozen_low": 3000.0,
        "frozen_high": 3001.5,
        "live_low": 3000.0,
        "live_high": 3001.5,
        "sweep_extreme": 2999.0 if direction == "BUY" else 3002.0,
        "suggested_stop": suggested_stop,
        "risk_to_stop_u": abs(last_price - suggested_stop),
        "risk_to_stop_pct": abs(last_price - suggested_stop) / last_price,
        "phase2_total_score": phase2_total_score,
        "absorption_score": absorption_score,
        "pressure_decay_score": 0.8,
        "reclaim_score": 0.9,
        "retest_score": 0.85,
        "book_absorption_score": 0.9,
        "relevant_book_depth_available": relevant_book_depth_available,
        "reload_score": 0.3,
        "bid_reload_count": 1,
        "ask_reload_count": 0,
        "has_swept_boundary": phase2_type != "CLEAN_HOLD",
        "has_absorbed_after_sweep": phase2_type != "CLEAN_HOLD",
        "has_reclaimed_boundary": phase2_type == "SWEEP_RECLAIM",
        "has_retested_inside_zone": phase2_type == "SWEEP_RECLAIM",
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


def test_phase2_buy_below_zone_absorption_confirms_with_relevant_book_depth(caplog):
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    assert evaluator.register_frozen_zone(_frozen_zone("iz-buy-below", frozen_ts=100.0), now_ts=100.0) is True

    with caplog.at_level(logging.INFO):
        evaluator.on_trade({"price": 2999.0, "size": 50.0, "side": "sell", "ts": 101.1})
        evaluator.on_book_update(
            {
                "ts": 101.2,
                "bids": {3000.0: 80.0, 2999.2: 20.0},
                "asks": {3000.5: 5.0, 3002.0: 100.0},
            }
        )
        evaluator.on_price(price=2999.0, ts=101.3)

    snapshot = evaluator.get_active_zone("iz-buy-below")
    assert snapshot["state"] == "PHASE2_CONFIRMED"
    assert snapshot["phase2_type"] == "BELOW_ZONE_ABSORPTION"
    assert snapshot["has_swept_boundary"] is True
    assert snapshot["has_absorbed_after_sweep"] is True
    assert snapshot["has_reclaimed_boundary"] is False
    assert snapshot["has_retested_inside_zone"] is False
    assert snapshot["phase2_total_score"] >= evaluator.min_below_zone_total_score

    messages = [record.getMessage() for record in caplog.records]
    assert any(
        "[PHASE2-CONFIRMED]" in message
        and "phase2_type=BELOW_ZONE_ABSORPTION" in message
        and "confirm_reason=below_zone_book_absorption_with_relevant_depth" in message
        and "suggested_stop=" in message
        for message in messages
    )


def test_phase2_sell_below_zone_absorption_confirms_with_relevant_book_depth():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    assert evaluator.register_frozen_zone(_sell_frozen_zone("iz-sell-below", frozen_ts=100.0), now_ts=100.0) is True

    evaluator.on_trade({"price": 3002.0, "size": 50.0, "side": "buy", "ts": 101.1})
    evaluator.on_book_update(
        {
            "ts": 101.2,
            "bids": {3001.0: 5.0, 2999.0: 100.0},
            "asks": {3001.5: 80.0, 3002.0: 20.0},
        }
    )
    evaluator.on_price(price=3002.0, ts=101.3)

    snapshot = evaluator.get_active_zone("iz-sell-below")
    assert snapshot["state"] == "PHASE2_CONFIRMED"
    assert snapshot["phase2_type"] == "BELOW_ZONE_ABSORPTION"
    assert snapshot["has_swept_boundary"] is True
    assert snapshot["has_absorbed_after_sweep"] is True
    assert snapshot["has_reclaimed_boundary"] is False
    assert snapshot["has_retested_inside_zone"] is False


def test_phase2_below_zone_absorption_does_not_override_sweep_reclaim():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    assert evaluator.register_frozen_zone(_frozen_zone("iz-below-priority", frozen_ts=100.0), now_ts=100.0) is True

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

    snapshot = evaluator.get_active_zone("iz-below-priority")
    assert snapshot["state"] == "PHASE2_CONFIRMED"
    assert snapshot["phase2_type"] == "SWEEP_RECLAIM"
    assert snapshot["phase2_type"] != "BELOW_ZONE_ABSORPTION"


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


def test_phase2_below_zone_no_depth_without_pressure_decay_does_not_confirm():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    assert evaluator.register_frozen_zone(_frozen_zone("iz-below-no-depth", frozen_ts=100.0), now_ts=100.0) is True

    evaluator.on_trade({"price": 2999.0, "size": 50.0, "side": "sell", "ts": 101.1})
    evaluator.on_book_update(
        {
            "ts": 101.2,
            "bids": {3100.0: 100.0},
            "asks": {3101.0: 100.0},
        }
    )
    evaluator.on_price(price=2999.0, ts=101.3)

    snapshot = evaluator.get_active_zone("iz-below-no-depth")
    assert snapshot["state"] != "PHASE2_CONFIRMED"
    assert snapshot["phase2_type"] != "BELOW_ZONE_ABSORPTION"
    assert snapshot["relevant_book_depth_available"] is False
    assert snapshot["pressure_decay_score"] < evaluator.min_absorption_score


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


def test_phase3_sweep_reclaim_accepts_research_candidate(caplog):
    evaluator = Phase3CandidateEvaluator()
    event = _phase2_confirmed_event(zone_id="p3-sweep")

    with caplog.at_level(logging.INFO):
        result = evaluator.evaluate_phase2_confirmed(event)

    assert result["candidate_type"] == "SWEEP_RECLAIM_RETEST_ENTRY"
    assert result["decision"] == "ACCEPT_RESEARCH_CANDIDATE"
    assert result["risk_distance_u"] > 0
    assert result["risk_distance_pct"] > 0
    assert result["total_loss_pct"] > result["risk_distance_pct"]
    assert result["final_margin_usage_pct"] <= research_config.PHASE3_MAX_MARGIN_USAGE_PCT
    assert any("[PHASE3-CANDIDATE]" in record.getMessage() for record in caplog.records)


def test_phase3_clean_hold_rejects_low_phase2_score():
    evaluator = Phase3CandidateEvaluator()
    event = _phase2_confirmed_event(
        zone_id="p3-clean-low",
        phase2_type="CLEAN_HOLD",
        last_price=3000.8,
        suggested_stop=0.0,
        phase2_total_score=0.71,
    )

    result = evaluator.evaluate_phase2_confirmed(event)

    assert result["candidate_type"] == "CLEAN_HOLD_LOW_RISK"
    assert result["decision"] == "REJECT_TOO_LOW_PHASE2_SCORE"


def test_phase3_clean_hold_accepts_when_score_is_high_enough():
    evaluator = Phase3CandidateEvaluator()
    event = _phase2_confirmed_event(
        zone_id="p3-clean-ok",
        phase2_type="CLEAN_HOLD",
        last_price=3000.8,
        suggested_stop=0.0,
        phase2_total_score=0.73,
    )

    result = evaluator.evaluate_phase2_confirmed(event)

    assert result["candidate_type"] == "CLEAN_HOLD_LOW_RISK"
    assert result["decision"] == "ACCEPT_RESEARCH_CANDIDATE"
    assert result["suggested_stop"] == event["frozen_low"] - research_config.PHASE3_CLEAN_HOLD_STOP_BUFFER_USDT


def test_phase3_below_zone_absorption_accepts_with_relevant_book_depth():
    evaluator = Phase3CandidateEvaluator()
    event = _phase2_confirmed_event(
        zone_id="p3-below-depth",
        phase2_type="BELOW_ZONE_ABSORPTION",
        last_price=2999.0,
        suggested_stop=2998.5,
        phase2_total_score=0.75,
        absorption_score=0.8,
        relevant_book_depth_available=True,
    )

    result = evaluator.evaluate_phase2_confirmed(event)

    assert result["candidate_type"] == "BELOW_ZONE_ABSORPTION_ENTRY"
    assert result["decision"] == "ACCEPT_RESEARCH_CANDIDATE"


def test_phase3_below_zone_with_relevant_book_depth_still_uses_risk_gate():
    evaluator = Phase3CandidateEvaluator()
    event = _phase2_confirmed_event(
        zone_id="p3-below-depth-too-far",
        phase2_type="BELOW_ZONE_ABSORPTION",
        last_price=2999.0,
        suggested_stop=0.0,
        phase2_total_score=0.75,
        absorption_score=0.8,
        relevant_book_depth_available=True,
    )
    event["sweep_extreme"] = 2900.0

    result = evaluator.evaluate_phase2_confirmed(event)

    assert result["decision"] == "REJECT_TOO_FAR_FROM_STOP"


def test_phase3_below_zone_absorption_waits_without_relevant_book_depth_by_default():
    evaluator = Phase3CandidateEvaluator()
    event = _phase2_confirmed_event(
        zone_id="p3-below-no-depth",
        phase2_type="BELOW_ZONE_ABSORPTION",
        last_price=2999.0,
        suggested_stop=2998.5,
        phase2_total_score=0.75,
        absorption_score=0.8,
        relevant_book_depth_available=False,
    )

    result = evaluator.evaluate_phase2_confirmed(event)

    assert result["decision"] == "WAIT_RECLAIM_OR_MORE_FLOW"


def test_phase3_below_zone_wait_decision_is_not_overwritten_by_risk_gate():
    evaluator = Phase3CandidateEvaluator()
    invalid_stop_event = _phase2_confirmed_event(
        zone_id="p3-below-wait-invalid-stop",
        phase2_type="BELOW_ZONE_ABSORPTION",
        last_price=2999.0,
        suggested_stop=0.0,
        phase2_total_score=0.75,
        absorption_score=0.8,
        relevant_book_depth_available=False,
    )
    invalid_stop_event["sweep_extreme"] = 3000.0
    too_far_event = _phase2_confirmed_event(
        zone_id="p3-below-wait-too-far",
        phase2_type="BELOW_ZONE_ABSORPTION",
        last_price=2999.0,
        suggested_stop=0.0,
        phase2_total_score=0.75,
        absorption_score=0.8,
        relevant_book_depth_available=False,
    )
    too_far_event["sweep_extreme"] = 2900.0

    invalid_result = evaluator.evaluate_phase2_confirmed(invalid_stop_event)
    too_far_result = evaluator.evaluate_phase2_confirmed(too_far_event)

    assert invalid_result["decision"] == "WAIT_RECLAIM_OR_MORE_FLOW"
    assert invalid_result["decision_reason"] == "below_zone_without_relevant_book_depth_wait_reclaim_or_more_flow"
    assert too_far_result["decision"] == "WAIT_RECLAIM_OR_MORE_FLOW"
    assert too_far_result["decision_reason"] == "below_zone_without_relevant_book_depth_wait_reclaim_or_more_flow"
    assert too_far_result["risk_distance_pct"] > research_config.PHASE3_MAX_RISK_DISTANCE_PCT


def test_phase3_rejects_invalid_buy_and_sell_stops():
    evaluator = Phase3CandidateEvaluator()
    buy_event = _phase2_confirmed_event(
        zone_id="p3-invalid-buy",
        direction="BUY",
        last_price=3000.0,
        suggested_stop=3000.0,
    )
    sell_event = _phase2_confirmed_event(
        zone_id="p3-invalid-sell",
        direction="SELL",
        last_price=3000.0,
        suggested_stop=2999.0,
    )

    assert evaluator.evaluate_phase2_confirmed(buy_event)["decision"] == "REJECT_INVALID_STOP"
    assert evaluator.evaluate_phase2_confirmed(sell_event)["decision"] == "REJECT_INVALID_STOP"


def test_phase3_rejects_when_stop_is_too_far():
    evaluator = Phase3CandidateEvaluator()
    event = _phase2_confirmed_event(
        zone_id="p3-too-far",
        last_price=3000.0,
        suggested_stop=2970.0,
    )

    result = evaluator.evaluate_phase2_confirmed(event)

    assert result["decision"] == "REJECT_TOO_FAR_FROM_STOP"


def test_phase3_rejects_margin_usage_below_minimum(monkeypatch):
    monkeypatch.setattr(research_config, "PHASE3_MIN_MARGIN_USAGE_PCT", 0.96)
    evaluator = Phase3CandidateEvaluator()
    event = _phase2_confirmed_event(zone_id="p3-margin-small")

    result = evaluator.evaluate_phase2_confirmed(event)

    assert result["decision"] == "REJECT_MARGIN_TOO_SMALL"
    assert result["final_margin_usage_pct"] < research_config.PHASE3_MIN_MARGIN_USAGE_PCT


def test_phase3_rejects_duplicate_zone_id():
    evaluator = Phase3CandidateEvaluator()
    event = _phase2_confirmed_event(zone_id="p3-duplicate")

    first = evaluator.evaluate_phase2_confirmed(event)
    second = evaluator.evaluate_phase2_confirmed(dict(event))

    assert first["decision"] == "ACCEPT_RESEARCH_CANDIDATE"
    assert second["decision"] == "REJECT_DUPLICATE_ZONE"


def test_phase3_candidate_does_not_call_trader_or_order_api(monkeypatch):
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

    evaluator = Phase3CandidateEvaluator()
    result = evaluator.evaluate_phase2_confirmed(_phase2_confirmed_event(zone_id="p3-no-trade"))

    assert result["decision"] == "ACCEPT_RESEARCH_CANDIDATE"
    assert called == {"process_signal": 0, "request": 0}


def test_phase2_confirmed_events_pop_once():
    evaluator = Phase2OrderflowEvaluator(max_active_zones=20, zone_ttl_seconds=1800)
    assert evaluator.register_frozen_zone(_frozen_zone("iz-event-queue", frozen_ts=100.0), now_ts=100.0) is True

    evaluator.on_trade({"price": 2999.0, "size": 50.0, "side": "sell", "ts": 101.1})
    evaluator.on_book_update(
        {
            "ts": 101.2,
            "bids": {3000.0: 80.0, 2999.2: 20.0},
            "asks": {3000.5: 5.0, 3002.0: 100.0},
        }
    )
    evaluator.on_price(price=2999.0, ts=101.3)

    events = evaluator.pop_confirmed_events()
    assert len(events) == 1
    event = events[0]
    assert event["zone_id"] == "iz-event-queue"
    assert event["state"] == "PHASE2_CONFIRMED"
    assert event["phase2_type"] == "BELOW_ZONE_ABSORPTION"
    assert "suggested_stop" in event
    assert "risk_to_stop_u" in event
    assert evaluator.pop_confirmed_events() == []


def test_phase1_forwards_phase2_confirmed_events_and_catches_phase3_failures(caplog):
    class _Ctx:
        current_price = 3000.0
        bids = {}
        asks = {}

    class _Phase2WithEvent:
        def __init__(self):
            self.event = _phase2_confirmed_event(zone_id="p3-forward")

        def on_trade(self, trade_data):
            return None

        def pop_confirmed_events(self):
            return [self.event]

    class _Phase3Spy:
        def __init__(self):
            self.events = []

        def evaluate_phase2_confirmed(self, event):
            self.events.append(event)
            return {"decision": "ACCEPT_RESEARCH_CANDIDATE"}

    class _Phase3Boom:
        def evaluate_phase2_confirmed(self, event):
            raise RuntimeError("phase3 failed")

    engine = Phase1Engine(_Ctx(), iceberg_detector=None)
    phase2 = _Phase2WithEvent()
    phase3 = _Phase3Spy()
    engine.phase2_orderflow_evaluator = phase2
    engine.phase3_candidate_evaluator = phase3

    engine._update_phase2_orderflow(
        trade_data={"price": 3000.0, "size": 1.0, "side": "buy", "ts": 101.0},
        price=3000.0,
        trade_ts=101.0,
    )
    assert phase3.events == [phase2.event]

    engine.phase3_candidate_evaluator = _Phase3Boom()
    with caplog.at_level(logging.ERROR):
        engine._update_phase2_orderflow(
            trade_data={"price": 3000.0, "size": 1.0, "side": "buy", "ts": 102.0},
            price=3000.0,
            trade_ts=102.0,
        )

    assert any("[PHASE3-CANDIDATE-FAILED]" in record.getMessage() for record in caplog.records)


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
    evaluator.on_trade({"price": 2999.0, "size": 50.0, "side": "sell", "ts": 101.1})
    evaluator.on_book_update(
        {
            "ts": 101.2,
            "bids": {3000.0: 80.0},
            "asks": {3000.5: 5.0},
        }
    )
    evaluator.on_price(price=2999.0, ts=101.3)

    assert evaluator.get_active_zone("iz-no-trade")["state"] == "PHASE2_CONFIRMED"
    assert evaluator.get_active_zone("iz-no-trade")["phase2_type"] == "BELOW_ZONE_ABSORPTION"
    assert called == {"process_signal": 0, "request": 0}


def _candidate(decision="ACCEPT_RESEARCH_CANDIDATE", direction="BUY", price=3000.0, stop=2998.5, zone_id="z1"):
    return {
        "decision": decision,
        "zone_id": zone_id,
        "direction": direction,
        "candidate_price": price,
        "suggested_stop": stop,
        "risk_distance_u": abs(price-stop),
        "risk_distance_pct": abs(price-stop)/price,
        "final_margin_usage_pct": 0.1,
        "leverage": 10,
        "phase2_type": "SWEEP_RECLAIM",
        "candidate_type": "RESEARCH",
    }


def _accepted_candidate(direction="BUY", price=3000.0, stop=2998.0, zone_id="z1", phase2_total_score=0.8):
    c = _candidate(direction=direction, price=price, stop=stop, zone_id=zone_id)
    c["phase2_total_score"] = phase2_total_score
    return c


def test_virtual_accept_opens_wait_reject_block_and_single_position(caplog):
    m = VirtualPositionManager()
    with caplog.at_level(logging.INFO):
        assert m.on_candidate(_candidate())
        assert m.get_active_position() is not None
        assert m.get_active_position()["direction"] == "LONG"
        assert m.get_active_position()["virtual_size_eth"] > 0
        assert m.on_candidate(_candidate(zone_id="z2")) is None
    assert any("[VIRTUAL-POSITION-OPEN]" in r.message for r in caplog.records)
    assert any("[VIRTUAL-POSITION-SKIP]" in r.message for r in caplog.records)
    assert VirtualPositionManager().on_candidate(_candidate(decision="WAIT_RECLAIM_OR_MORE_FLOW")) is None
    assert VirtualPositionManager().on_candidate(_candidate(decision="REJECT_TOO_FAR_FROM_STOP")) is None




def test_virtual_open_ts_prefers_candidate_ts_and_fallback_to_ts():
    m1 = VirtualPositionManager()
    c1 = _candidate()
    c1["candidate_ts"] = 12345.6
    c1["ts"] = 99999.0
    m1.on_candidate(c1)
    assert m1.get_active_position()["open_ts"] == 12345.6

    m2 = VirtualPositionManager()
    c2 = _candidate()
    c2["ts"] = 888.0
    m2.on_candidate(c2)
    assert m2.get_active_position()["open_ts"] == 888.0


def test_virtual_skip_and_reject_counters_and_logs(caplog):
    m = VirtualPositionManager()
    with caplog.at_level(logging.INFO):
        assert m.on_candidate(_candidate(zone_id="z1"))
        assert m.on_candidate(_candidate(zone_id="z2")) is None
    assert m.total_skipped == 1
    assert m.total_rejected == 0
    assert any("[VIRTUAL-POSITION-SKIP]" in r.message and "reason=support_phase2_score_too_low" in r.message for r in caplog.records)

    m2 = VirtualPositionManager()
    assert m2.on_candidate(_candidate(direction="BUY", price=3000.0, stop=3001.0)) is None
    assert m2.total_rejected == 1
    assert m2.total_skipped == 0

    m3 = VirtualPositionManager()
    assert m3.on_candidate(_candidate(decision="WAIT_RECLAIM_OR_MORE_FLOW")) is None
    assert m3.total_rejected == 0
    assert m3.total_skipped == 0


def test_virtual_breakeven_moves_stop_for_long(monkeypatch):
    monkeypatch.setattr(research_config, "VIRTUAL_TAKE_PROFIT_R_MULTIPLE", 10.0)
    monkeypatch.setattr(research_config, "VIRTUAL_BREAKEVEN_ENABLED", True)
    monkeypatch.setattr(research_config, "VIRTUAL_TRAILING_ENABLED", False)
    monkeypatch.setattr(research_config, "VIRTUAL_BREAKEVEN_TRIGGER_R", 1.0)

    m = VirtualPositionManager()
    m.on_candidate(_accepted_candidate(direction="BUY", price=3000.0, stop=2998.0))
    m.on_price(3002.0, ts=1.0)

    pos = m.get_active_position()
    assert pos["breakeven_activated"] is True
    assert pos["dynamic_stop"] > pos["initial_stop"]
    assert pos["dynamic_stop"] >= pos["open_price"]
    assert pos["stop_update_count"] >= 1
    summary = m.summary()
    assert summary["active_dynamic_stop"] == pos["dynamic_stop"]
    assert summary["active_initial_stop"] == pos["initial_stop"]
    assert summary["active_best_price"] == pos["best_price"]
    assert summary["active_breakeven_activated"] is True
    assert summary["active_stop_update_count"] == pos["stop_update_count"]


def test_virtual_breakeven_moves_stop_for_short(monkeypatch):
    monkeypatch.setattr(research_config, "VIRTUAL_TAKE_PROFIT_R_MULTIPLE", 10.0)
    monkeypatch.setattr(research_config, "VIRTUAL_BREAKEVEN_ENABLED", True)
    monkeypatch.setattr(research_config, "VIRTUAL_TRAILING_ENABLED", False)
    monkeypatch.setattr(research_config, "VIRTUAL_BREAKEVEN_TRIGGER_R", 1.0)

    m = VirtualPositionManager()
    m.on_candidate(_accepted_candidate(direction="SELL", price=3000.0, stop=3002.0))
    m.on_price(2998.0, ts=1.0)

    pos = m.get_active_position()
    assert pos["breakeven_activated"] is True
    assert pos["dynamic_stop"] < pos["initial_stop"]
    assert pos["dynamic_stop"] <= pos["open_price"]


def test_virtual_trailing_moves_stop_after_favorable_move(monkeypatch):
    monkeypatch.setattr(research_config, "VIRTUAL_TAKE_PROFIT_R_MULTIPLE", 10.0)
    monkeypatch.setattr(research_config, "VIRTUAL_BREAKEVEN_ENABLED", False)
    monkeypatch.setattr(research_config, "VIRTUAL_TRAILING_ENABLED", True)
    monkeypatch.setattr(research_config, "VIRTUAL_TRAILING_TRIGGER_R", 1.5)
    monkeypatch.setattr(research_config, "VIRTUAL_TRAILING_DISTANCE_R", 0.8)

    m = VirtualPositionManager()
    m.on_candidate(_accepted_candidate(direction="BUY", price=3000.0, stop=2998.0))
    m.on_price(3004.0, ts=1.0)

    pos = m.get_active_position()
    assert pos["trailing_activated"] is True
    assert pos["dynamic_stop"] > pos["initial_stop"]
    assert pos["last_stop_update_reason"] == "TRAILING"


def test_virtual_stop_cannot_move_backward_long_and_short(monkeypatch):
    monkeypatch.setattr(research_config, "VIRTUAL_TAKE_PROFIT_R_MULTIPLE", 10.0)
    monkeypatch.setattr(research_config, "VIRTUAL_BREAKEVEN_ENABLED", False)
    monkeypatch.setattr(research_config, "VIRTUAL_TRAILING_ENABLED", True)
    monkeypatch.setattr(research_config, "VIRTUAL_TRAILING_TRIGGER_R", 1.5)
    monkeypatch.setattr(research_config, "VIRTUAL_TRAILING_DISTANCE_R", 0.8)

    long_m = VirtualPositionManager()
    long_m.on_candidate(_accepted_candidate(direction="BUY", price=3000.0, stop=2998.0))
    long_m.on_price(3004.0, ts=1.0)
    long_stop = long_m.get_active_position()["dynamic_stop"]
    long_m.on_price(3003.0, ts=2.0)
    assert long_m.get_active_position()["dynamic_stop"] == long_stop

    short_m = VirtualPositionManager()
    short_m.on_candidate(_accepted_candidate(direction="SELL", price=3000.0, stop=3002.0))
    short_m.on_price(2996.0, ts=1.0)
    short_stop = short_m.get_active_position()["dynamic_stop"]
    short_m.on_price(2997.0, ts=2.0)
    assert short_m.get_active_position()["dynamic_stop"] == short_stop


def test_virtual_dynamic_stop_used_for_stop_loss(monkeypatch):
    monkeypatch.setattr(research_config, "VIRTUAL_TAKE_PROFIT_R_MULTIPLE", 10.0)
    monkeypatch.setattr(research_config, "VIRTUAL_BREAKEVEN_ENABLED", True)
    monkeypatch.setattr(research_config, "VIRTUAL_TRAILING_ENABLED", False)

    m = VirtualPositionManager()
    m.on_candidate(_accepted_candidate(direction="BUY", price=3000.0, stop=2998.0))
    m.on_price(3002.0, ts=1.0)
    dynamic_stop = m.get_active_position()["dynamic_stop"]
    m.on_price(dynamic_stop, ts=2.0)

    closed = m.get_closed_positions()[-1]
    assert closed["close_reason"] == "STOP_LOSS"
    assert closed["exit_stop_used"] == dynamic_stop
    original_stop_r = (2998.0 - 3000.0) / 2.0
    assert closed["realized_r_multiple"] != original_stop_r


def test_virtual_same_direction_support_update_does_not_add_position(caplog):
    m = VirtualPositionManager()
    m.on_candidate(_accepted_candidate(direction="BUY", price=3000.0, stop=2998.0, zone_id="z1"))
    first = m.get_active_position()

    support = _accepted_candidate(direction="BUY", price=3001.0, stop=2998.5, zone_id="z2")
    with caplog.at_level(logging.INFO):
        assert m.on_candidate(support)

    pos = m.get_active_position()
    assert pos["position_id"] == first["position_id"]
    assert pos["virtual_size_eth"] == first["virtual_size_eth"]
    assert m.total_opened == 1
    assert m.active_position is not None
    assert pos["support_update_count"] == 1
    assert "z2" in pos["support_zone_ids"]
    assert any("[VIRTUAL-SUPPORT-UPDATE]" in r.message for r in caplog.records)


def test_virtual_same_direction_support_can_improve_stop():
    m = VirtualPositionManager()
    m.on_candidate(_accepted_candidate(direction="BUY", price=3000.0, stop=2998.0, zone_id="z1"))
    m.on_price(3001.0, ts=1.0)
    m.on_candidate(_accepted_candidate(direction="BUY", price=3001.0, stop=2999.0, zone_id="z2"))

    pos = m.get_active_position()
    assert pos["dynamic_stop"] == 2999.0
    assert pos["last_stop_update_reason"] == "SUPPORT_CANDIDATE"
    assert pos["stop_update_count"] == 1


def test_virtual_same_direction_support_cannot_worsen_stop():
    m = VirtualPositionManager()
    m.on_candidate(_accepted_candidate(direction="BUY", price=3000.0, stop=2998.0, zone_id="z1"))
    m.on_price(3001.0, ts=1.0)
    m.on_candidate(_accepted_candidate(direction="BUY", price=3001.0, stop=2999.0, zone_id="z2"))
    improved_stop = m.get_active_position()["dynamic_stop"]
    m.on_candidate(_accepted_candidate(direction="BUY", price=3001.0, stop=2998.0, zone_id="z3"))

    assert m.get_active_position()["dynamic_stop"] == improved_stop


def test_virtual_opposite_direction_candidate_skipped(caplog):
    m = VirtualPositionManager()
    m.on_candidate(_accepted_candidate(direction="BUY", price=3000.0, stop=2998.0, zone_id="z1"))
    before_skipped = m.total_skipped

    with caplog.at_level(logging.INFO):
        assert m.on_candidate(_accepted_candidate(direction="SELL", price=3000.0, stop=3002.0, zone_id="z2")) is None

    assert m.get_active_position()["direction"] == "LONG"
    assert m.total_opened == 1
    assert m.total_closed == 0
    assert m.total_skipped == before_skipped + 1
    assert any("reason=opposite_direction_active_position_exists" in r.message for r in caplog.records)


def test_virtual_support_zone_ids_bounded(monkeypatch):
    monkeypatch.setattr(research_config, "VIRTUAL_SUPPORT_MAX_ZONE_IDS", 2)
    m = VirtualPositionManager()
    m.on_candidate(_accepted_candidate(direction="BUY", price=3000.0, stop=2998.0, zone_id="z1"))
    for i in range(2, 5):
        m.on_candidate(_accepted_candidate(direction="BUY", price=3001.0, stop=2998.5, zone_id=f"z{i}"))

    assert m.get_active_position()["support_zone_ids"] == ["z3", "z4"]


def test_virtual_breakeven_and_trailing_disabled_configs_respected(monkeypatch):
    monkeypatch.setattr(research_config, "VIRTUAL_TAKE_PROFIT_R_MULTIPLE", 10.0)
    monkeypatch.setattr(research_config, "VIRTUAL_BREAKEVEN_ENABLED", False)
    monkeypatch.setattr(research_config, "VIRTUAL_TRAILING_ENABLED", False)

    m = VirtualPositionManager()
    m.on_candidate(_accepted_candidate(direction="BUY", price=3000.0, stop=2998.0))
    m.on_price(3004.0, ts=1.0)

    pos = m.get_active_position()
    assert pos["breakeven_activated"] is False
    assert pos["trailing_activated"] is False
    assert pos["dynamic_stop"] == pos["initial_stop"]


def test_virtual_position_manager_does_not_call_trader_or_order_api(monkeypatch):
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

    m = VirtualPositionManager()
    m.on_candidate(_accepted_candidate(direction="BUY", price=3000.0, stop=2998.0))
    m.on_price(3002.0, ts=1.0)
    m.on_candidate(_accepted_candidate(direction="BUY", price=3001.0, stop=2999.0, zone_id="z2"))
    m.on_candidate(_accepted_candidate(direction="SELL", price=3000.0, stop=3002.0, zone_id="z3"))

    assert called == {"process_signal": 0, "request": 0}


def test_virtual_summary_uses_cumulative_stats_not_closed_window():
    m = VirtualPositionManager()
    m.closed_positions = __import__('collections').deque(maxlen=2)

    m.on_candidate(_candidate(zone_id="a")); m.on_price(2998.5, ts=1)
    m.on_candidate(_candidate(zone_id="b")); tp = m.get_active_position()["take_profit_price"]; m.on_price(tp, ts=2)
    m.on_candidate(_candidate(zone_id="c")); tp2 = m.get_active_position()["take_profit_price"]; m.on_price(tp2, ts=3)

    summary = m.summary()
    assert len(m.get_closed_positions()) == 2
    assert summary["closed_positions_count"] == 2
    assert summary["closed_positions_maxlen"] == 2
    assert summary["total_closed"] == 3
    assert summary["cumulative_realized_pnl_u"] == summary["total_realized_pnl_u"]

    assert summary["win_count"] + summary["loss_count"] == 3
    assert summary["win_count"] == 2
    assert summary["loss_count"] == 1
    expected_avg_r = (-1.0 + research_config.VIRTUAL_TAKE_PROFIT_R_MULTIPLE + research_config.VIRTUAL_TAKE_PROFIT_R_MULTIPLE) / 3.0
    assert abs(summary["avg_realized_r"] - expected_avg_r) < 1e-9
    assert summary["active_dynamic_stop"] == 0.0
    assert summary["active_initial_stop"] == 0.0
    assert summary["active_best_price"] == 0.0
    assert summary["active_breakeven_activated"] is False
    assert summary["active_trailing_activated"] is False
    assert summary["active_stop_update_count"] == 0
    assert summary["active_support_update_count"] == 0

def test_virtual_long_short_stop_and_take_profit_and_maxlen():
    m = VirtualPositionManager()
    m.closed_positions = __import__('collections').deque(maxlen=2)
    m.on_candidate(_candidate(zone_id="a")); m.on_price(2998.5, ts=1)
    c1 = m.get_closed_positions()[-1]; assert c1["close_reason"] == "STOP_LOSS" and c1["realized_pnl_u"] < 0
    m.on_candidate(_candidate(zone_id="b")); tp = m.get_active_position()["take_profit_price"]; m.on_price(tp, ts=2)
    c2 = m.get_closed_positions()[-1]; assert c2["close_reason"] == "TAKE_PROFIT_R_MULTIPLE" and c2["realized_pnl_u"] > 0
    assert c2["realized_r_multiple"] >= research_config.VIRTUAL_TAKE_PROFIT_R_MULTIPLE - 1e-9
    m.on_candidate(_candidate(direction="SELL", price=3000, stop=3001.5, zone_id="c")); m.on_price(3001.5, ts=3)
    assert m.get_closed_positions()[-1]["close_reason"] == "STOP_LOSS"
    m.on_candidate(_candidate(direction="SELL", price=3000, stop=3001.5, zone_id="d")); tp2 = m.get_active_position()["take_profit_price"]; m.on_price(tp2, ts=4)
    assert m.get_closed_positions()[-1]["close_reason"] == "TAKE_PROFIT_R_MULTIPLE"
    m.on_candidate(_candidate(zone_id="e")); m.on_price(2998.5, ts=5)
    assert len(m.get_closed_positions()) == 2


def test_phase1_virtual_integration_and_execution_modes(monkeypatch):
    class _Ctx:
        current_price = 3000.0
        bids = {3000.0: 1.0}
        asks = {3001.0: 1.0}
    class _P2:
        def pop_confirmed_events(self): return [{"zone_id":"x"}]
        def on_trade(self, t): pass
        def on_book_update(self, b): pass
    class _P3:
        def __init__(self): self.called=False
        def evaluate_phase2_confirmed(self, e): self.called=True; return _candidate()
    class _V:
        def __init__(self): self.c=[]; self.p=[]
        def on_candidate(self, r): self.c.append(r)
        def on_price(self, **kw): self.p.append(kw)
    eng=Phase1Engine(_Ctx(), iceberg_detector=None)
    eng.phase2_orderflow_evaluator=_P2(); eng.phase3_candidate_evaluator=_P3(); eng.virtual_position_manager=_V()
    eng._drain_phase2_confirmed_events(); assert len(eng.virtual_position_manager.c)==1
    eng.on_trade({"price":3000,"size":200,"side":"sell","ts":10}); assert eng.virtual_position_manager.p
    monkeypatch.setattr("src.strategy.phase1_zone_engine.REAL_EXECUTION_ENABLED", True)
    monkeypatch.setattr("src.strategy.phase1_zone_engine.VIRTUAL_POSITION_MANAGER_ENABLED", True)
    monkeypatch.setattr("src.strategy.phase1_zone_engine.VIRTUAL_SHADOW_MODE", False)
    assert Phase1Engine(_Ctx(), iceberg_detector=None).virtual_position_manager is None
    monkeypatch.setattr("src.strategy.phase1_zone_engine.VIRTUAL_SHADOW_MODE", True)
    assert Phase1Engine(_Ctx(), iceberg_detector=None).virtual_position_manager is not None
