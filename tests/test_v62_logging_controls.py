#!/usr/bin/env python
# -*- coding: utf-8 -*-

import importlib
import logging
import sys
from collections import deque
from types import SimpleNamespace

import pytest

from config import research_evaluator as research_config
from src.monitoring.research_runtime_monitor import ResearchRuntimeMonitor
from src.strategy.a1_absorption.engine import A1AbsorptionEngine as Phase1Engine
from src.strategy.execution_research.candidate_risk_evaluator import CandidateRiskEvaluator as Phase3CandidateEvaluator
from src.strategy.execution_research.virtual_position_manager import ResearchVirtualPositionManager as VirtualPositionManager
from src.utils.log_noise import suppressed_log_counter


@pytest.fixture(autouse=True)
def restore_research_config():
    yield
    importlib.reload(research_config)


def _reload_config_with_profile(monkeypatch, profile):
    monkeypatch.setenv("V62_LOG_PROFILE", profile)
    return importlib.reload(research_config)


def _phase2_event(zone_id="p3-log-control"):
    return {
        "zone_id": zone_id,
        "direction": "BUY",
        "state": "PHASE2_CONFIRMED",
        "phase2_type": "SWEEP_RECLAIM",
        "confirmed_ts": 103.0,
        "last_price": 3000.3,
        "frozen_low": 3000.0,
        "frozen_high": 3001.5,
        "live_low": 3000.0,
        "live_high": 3001.5,
        "sweep_extreme": 2999.0,
        "suggested_stop": 2998.5,
        "risk_to_stop_u": 1.8,
        "risk_to_stop_pct": 1.8 / 3000.3,
        "phase2_total_score": 0.83,
        "absorption_score": 0.9,
        "pressure_decay_score": 0.8,
        "reclaim_score": 0.9,
        "retest_score": 0.85,
        "book_absorption_score": 0.4,
        "relevant_book_depth_available": True,
        "reload_score": 0.3,
        "has_swept_boundary": True,
        "has_absorbed_after_sweep": True,
        "has_reclaimed_boundary": True,
        "has_retested_inside_zone": True,
    }


def _candidate(zone_id="z-log-control"):
    return {
        "decision": "ACCEPT_RESEARCH_CANDIDATE",
        "zone_id": zone_id,
        "direction": "BUY",
        "candidate_ts": 100.0,
        "candidate_price": 3000.0,
        "suggested_stop": 2998.0,
        "risk_distance_u": 2.0,
        "risk_distance_pct": 2.0 / 3000.0,
        "total_loss_pct": 0.002,
        "final_margin_usage_pct": 0.1,
        "leverage": 10.0,
        "phase2_type": "SWEEP_RECLAIM",
        "candidate_type": "SWEEP_RECLAIM_RETEST_ENTRY",
    }


def _phase1_engine_for_quality_signal():
    class _Detector:
        def detect_buy_iceberg(self, active_notional, book_reduction):
            return {
                "is_iceberg": True,
                "hidden_volume": 2_500_000.0,
                "absorption_rate": 0.9,
                "active_volume": 2_500_000.0,
                "confidence": 0.9,
                "behavior": "ICEBERG_ABSORPTION",
            }

        def detect_sell_iceberg(self, active_notional, book_reduction):
            return self.detect_buy_iceberg(active_notional, book_reduction)

    class _ZoneTracker:
        def expire_old_zones(self, now_ts):
            return []

        def update(self, event, current_price=0.0):
            return None

        def drain_finalized_zones(self):
            return []

    engine = Phase1Engine.__new__(Phase1Engine)
    engine.ctx = SimpleNamespace(
        current_price=3000.0,
        bids={2999.0: 100.0, 3000.0: 100.0},
        asks={3000.0: 100.0, 3001.0: 100.0},
    )
    engine.iceberg_radar = _Detector()
    engine.pending_events = deque(
        [
            {
                "event_id": "pie-quality",
                "direction": "BUY",
                "trigger_price": 3000.0,
                "trigger_ts": 0.1,
                "trigger_recv_ts": 0.1,
                "active_notional": 2_500_000.0,
                "active_size": 833.0,
                "side": "sell",
                "zone_lower": 2999.0,
                "zone_upper": 3000.0,
                "start_thickness_usdt": 600_000.0,
                "book_updates_after_cutoff": 1,
                "trade_count": 3,
                "min_trade_price": 2999.5,
                "max_trade_price": 3000.0,
                "last_trade_ts": 0.1,
                "last_trade_recv_ts": 0.1,
                "status": "WAITING_BOOK",
            }
        ]
    )
    engine.max_wait_ms = 1000
    engine.max_price_deviation = 2.0
    engine.min_book_updates_after_cutoff = 2
    engine.zone_tracker = _ZoneTracker()
    engine.outcome_evaluator = SimpleNamespace(finalize_zone=lambda *args, **kwargs: None)
    engine.a1_reaction_evaluator = None
    engine.candidate_risk_evaluator = None
    engine.execution_outcome_evaluator = None
    engine.virtual_position_manager = None
    engine.research_runtime_monitor = None
    return engine


def test_research_log_profile_defaults_detailed(monkeypatch):
    cfg = _reload_config_with_profile(monkeypatch, "RESEARCH_DETAILED")

    assert cfg.V62_LOG_PENDING_ICEBERG_ENABLED is True
    assert cfg.V62_LOG_IGNORE_ICEBERG_ENABLED is True
    assert cfg.V62_LOG_SPOOFING_WITHDRAWAL_ENABLED is True
    assert cfg.V62_LOG_A1_ZONE_NEW_ENABLED is True
    assert cfg.V62_LOG_VIRTUAL_POSITION_UPDATE_ENABLED is True


def test_research_log_profile_key_events(monkeypatch):
    cfg = _reload_config_with_profile(monkeypatch, "RESEARCH_KEY_EVENTS")

    assert cfg.V62_LOG_PENDING_ICEBERG_ENABLED is False
    assert cfg.V62_LOG_IGNORE_ICEBERG_ENABLED is False
    assert cfg.V62_LOG_SPOOFING_WITHDRAWAL_ENABLED is False
    assert cfg.V62_LOG_A1_ZONE_NEW_ENABLED is False
    assert cfg.V62_LOG_A1_ZONE_FROZEN_ENABLED is True
    assert cfg.V62_LOG_PHASE2_CONFIRMED_ENABLED is True
    assert cfg.V62_LOG_PHASE3_CANDIDATE_ENABLED is True
    assert cfg.V62_LOG_VIRTUAL_POSITION_OPEN_ENABLED is True
    assert cfg.V62_LOG_VIRTUAL_POSITION_UPDATE_ENABLED is False
    assert cfg.V62_LOG_VIRTUAL_POSITION_CLOSE_ENABLED is True
    assert cfg.V62_LOG_PHASE3_OUTCOME_ENABLED is True


def test_research_log_profile_key_events_extended(monkeypatch):
    cfg = _reload_config_with_profile(monkeypatch, "RESEARCH_KEY_EVENTS")

    assert cfg.V62_LOG_SETTLED_ICEBERG_ENABLED is False
    assert cfg.V62_LOG_PHASE1_QUALITY_ENABLED is True
    assert cfg.V62_LOG_CANCEL_ICEBERG_ENABLED is False
    assert cfg.V62_LOG_A1_ZONE_STRESSED_ENABLED is False
    assert cfg.V62_LOG_VIRTUAL_POSITION_SKIP_ENABLED is True
    assert cfg.V62_LOG_PENDING_DROP_ENABLED is True


def test_research_log_profile_production_safe(monkeypatch):
    cfg = _reload_config_with_profile(monkeypatch, "PRODUCTION_SAFE")

    assert cfg.V62_LOG_PENDING_ICEBERG_ENABLED is False
    assert cfg.V62_LOG_IGNORE_ICEBERG_ENABLED is False
    assert cfg.V62_LOG_SPOOFING_WITHDRAWAL_ENABLED is False
    assert cfg.V62_LOG_A1_ZONE_NEW_ENABLED is False
    assert cfg.V62_LOG_A1_ZONE_FROZEN_ENABLED is True
    assert cfg.V62_LOG_PHASE2_STATE_ENABLED is False
    assert cfg.V62_LOG_PHASE2_CONFIRMED_ENABLED is True
    assert cfg.V62_LOG_PHASE3_CANDIDATE_ENABLED is True
    assert cfg.V62_LOG_VIRTUAL_POSITION_UPDATE_ENABLED is False
    assert cfg.V62_LOG_PHASE3_OUTCOME_ENABLED is True


def test_research_log_profile_production_safe_extended(monkeypatch):
    cfg = _reload_config_with_profile(monkeypatch, "PRODUCTION_SAFE")

    assert cfg.V62_LOG_SETTLED_ICEBERG_ENABLED is False
    assert cfg.V62_LOG_PHASE1_QUALITY_ENABLED is False
    assert cfg.V62_LOG_CANCEL_ICEBERG_ENABLED is False
    assert cfg.V62_LOG_A1_ZONE_STRESSED_ENABLED is False
    assert cfg.V62_LOG_VIRTUAL_POSITION_SKIP_ENABLED is True
    assert cfg.V62_LOG_PENDING_DROP_ENABLED is True


def test_log_to_console_false_does_not_add_stream_handler(monkeypatch, tmp_path):
    from src.utils import log as log_module

    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    monkeypatch.setenv("LOG_TO_CONSOLE", "false")
    monkeypatch.setenv("LOG_TO_FILE", "true")
    monkeypatch.setenv("LOG_DIR", str(tmp_path))
    monkeypatch.setenv("LOG_FILE_NAME", "test.log")

    log_module._setup_done = False
    try:
        log_module.setup_logging(logging.INFO)
        handlers = list(root_logger.handlers)
        assert not any(
            isinstance(handler, logging.StreamHandler)
            and getattr(handler, "stream", None) is sys.stdout
            for handler in handlers
        )
        assert any(isinstance(handler, logging.FileHandler) for handler in handlers)
    finally:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        for handler in original_handlers:
            root_logger.addHandler(handler)
        log_module._setup_done = True


def test_logging_switch_does_not_change_phase3_result(monkeypatch):
    _reload_config_with_profile(monkeypatch, "RESEARCH_DETAILED")
    detailed = Phase3CandidateEvaluator().evaluate_phase2_confirmed(_phase2_event("p3-detailed"))

    _reload_config_with_profile(monkeypatch, "RESEARCH_KEY_EVENTS")
    key_events = Phase3CandidateEvaluator().evaluate_phase2_confirmed(_phase2_event("p3-key"))

    for key in ("decision", "candidate_type", "suggested_stop", "risk_distance_u", "risk_distance_pct"):
        assert key_events[key] == detailed[key]


def test_logging_switch_does_not_change_virtual_position_behavior(monkeypatch):
    _reload_config_with_profile(monkeypatch, "RESEARCH_DETAILED")
    detailed_manager = VirtualPositionManager()
    detailed_open = detailed_manager.on_candidate(_candidate("vp-detailed"))
    detailed_close = detailed_manager.on_price(2998.0, ts=101.0)

    _reload_config_with_profile(monkeypatch, "RESEARCH_KEY_EVENTS")
    key_manager = VirtualPositionManager()
    key_open = key_manager.on_candidate(_candidate("vp-key"))
    key_close = key_manager.on_price(2998.0, ts=101.0)

    for key in ("direction", "open_price", "initial_stop", "dynamic_stop", "take_profit_price"):
        assert key_open[key] == detailed_open[key]
    for key in ("close_reason", "close_price", "realized_r_multiple", "virtual_equity_at_open"):
        assert key_close[key] == detailed_close[key]


def test_safety_check_failed_always_logs(monkeypatch, caplog):
    monkeypatch.setattr(research_config, "V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED", False)
    monkeypatch.setattr(research_config, "REAL_EXECUTION_ENABLED", True)
    monkeypatch.setattr(research_config, "PHASE3_REAL_TRADING_ENABLED", False)
    monkeypatch.setattr(research_config, "V62_REQUIRE_RESEARCH_ONLY_MODE", True)

    class _Engine:
        a1_reaction_evaluator = None
        candidate_risk_evaluator = None
        execution_outcome_evaluator = None
        virtual_position_manager = None
        zone_tracker = None
        outcome_evaluator = None

    monitor = ResearchRuntimeMonitor(_Engine(), label="test-fail-always")
    with caplog.at_level(logging.ERROR):
        monitor.run_startup_safety_check()

    assert any("[V62-SAFETY-CHECK-FAILED]" in r.message for r in caplog.records)


def test_heartbeat_includes_suppressed_log_counts(monkeypatch):
    monkeypatch.setattr(research_config, "V62_INTEGRATION_HEARTBEAT_ENABLED", True)
    monkeypatch.setattr(research_config, "V62_INTEGRATION_HEARTBEAT_INTERVAL_SEC", 300.0)
    suppressed_log_counter.snapshot_and_reset()
    suppressed_log_counter.inc("suppressed_pending_iceberg_count", 2)
    suppressed_log_counter.inc("suppressed_zone_new_count", 1)

    class _Engine:
        a1_reaction_evaluator = None
        candidate_risk_evaluator = None
        execution_outcome_evaluator = None
        virtual_position_manager = None
        zone_tracker = None
        outcome_evaluator = None

    summary = ResearchRuntimeMonitor(_Engine(), label="test-suppressed").maybe_log_heartbeat(1000.0)

    assert summary["suppressed_pending_iceberg_count"] == 2
    assert summary["suppressed_ignore_iceberg_count"] == 0
    assert summary["suppressed_spoofing_withdrawal_count"] == 0
    assert summary["suppressed_zone_new_count"] == 1
    assert summary["suppressed_virtual_update_count"] == 0


def test_suppressed_log_summary_extended_keys(monkeypatch):
    monkeypatch.setattr(research_config, "V62_INTEGRATION_HEARTBEAT_ENABLED", True)
    suppressed_log_counter.snapshot_and_reset()
    suppressed_log_counter.inc("suppressed_settled_iceberg_count", 2)
    suppressed_log_counter.inc("suppressed_phase1_quality_count", 3)
    suppressed_log_counter.inc("suppressed_cancel_iceberg_count", 4)
    suppressed_log_counter.inc("suppressed_zone_stressed_count", 5)
    suppressed_log_counter.inc("suppressed_virtual_skip_count", 6)
    suppressed_log_counter.inc("suppressed_pending_drop_count", 7)

    class _Engine:
        a1_reaction_evaluator = None
        candidate_risk_evaluator = None
        execution_outcome_evaluator = None
        virtual_position_manager = None
        zone_tracker = None
        outcome_evaluator = None

    summary = ResearchRuntimeMonitor(_Engine(), label="test-suppressed-extended").maybe_log_heartbeat(2000.0)

    assert summary["suppressed_settled_iceberg_count"] == 2
    assert summary["suppressed_phase1_quality_count"] == 3
    assert summary["suppressed_cancel_iceberg_count"] == 4
    assert summary["suppressed_zone_stressed_count"] == 5
    assert summary["suppressed_virtual_skip_count"] == 6
    assert summary["suppressed_pending_drop_count"] == 7


def test_virtual_skip_logging_can_be_suppressed_without_changing_counts(monkeypatch):
    monkeypatch.setattr(research_config, "V62_LOG_VIRTUAL_POSITION_SKIP_ENABLED", False)
    suppressed_log_counter.snapshot_and_reset()

    manager = VirtualPositionManager()
    assert manager.on_candidate(_candidate("vp-skip-open"))
    assert manager.on_candidate({**_candidate("vp-skip-opposite"), "direction": "SELL"}) is None

    assert manager.total_skipped == 1
    assert manager.get_active_position() is not None
    snapshot = suppressed_log_counter.snapshot_and_reset()
    assert snapshot["suppressed_virtual_skip_count"] == 1


def test_phase1_quality_logging_switch_does_not_change_signal(monkeypatch):
    monkeypatch.setattr(research_config, "V62_LOG_PHASE1_QUALITY_ENABLED", True)
    detailed = _phase1_engine_for_quality_signal().on_book_update({"ts": 0.3, "recv_ts": 0.3})

    monkeypatch.setattr(research_config, "V62_LOG_PHASE1_QUALITY_ENABLED", False)
    suppressed_log_counter.snapshot_and_reset()
    suppressed = _phase1_engine_for_quality_signal().on_book_update({"ts": 0.3, "recv_ts": 0.3})

    for key in ("event_type", "direction", "phase1_quality", "hidden_volume", "absorption_rate", "min_price"):
        assert suppressed[key] == detailed[key]
    snapshot = suppressed_log_counter.snapshot_and_reset()
    assert snapshot["suppressed_phase1_quality_count"] == 1
