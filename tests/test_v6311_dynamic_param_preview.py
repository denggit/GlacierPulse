#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from datetime import datetime
from zoneinfo import ZoneInfo

from src.detectors.iceberg_detector import IcebergDetector
from src.research.a1_dynamic_params.previewer import A1DynamicParamPreviewer
from src.research.a1_dynamic_params.session import get_session_tag
from src.strategy.a1_absorption.engine import A1AbsorptionEngine


def test_get_session_tag_beijing_2130_is_us_open():
    dt = datetime(2026, 5, 22, 21, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert get_session_tag(dt) == "US_OPEN"


def test_dynamic_preview_json_is_preview_only_and_static_active(tmp_path):
    path = tmp_path / "a1_dynamic_params.json"
    previewer = A1DynamicParamPreviewer(json_path=str(path))
    assert previewer.maybe_write(now_ts=1779466200, force=True)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["mode"] == "preview_only"
    assert payload["active_params"]["source"] == "static"
    assert payload["safety"]["dynamic_params_active"] is False


def test_dynamic_preview_does_not_modify_a1_active_params(tmp_path):
    class Ctx:
        bids = {}
        asks = {}
        current_price = 100.0

    engine = A1AbsorptionEngine(Ctx(), IcebergDetector(), phase1_truth_tracker=False)
    before = (
        engine.min_event_start_notional_usdt,
        engine.local_zone_width,
        engine.max_wait_ms,
    )
    engine.a1_dynamic_param_previewer = A1DynamicParamPreviewer(json_path=str(tmp_path / "preview.json"))
    engine._maybe_write_a1_dynamic_preview(1779466200)
    after = (
        engine.min_event_start_notional_usdt,
        engine.local_zone_width,
        engine.max_wait_ms,
    )
    assert after == before


def test_engine_iceberg_detector_logic_unchanged_with_disabled_truth_tracker():
    class Ctx:
        bids = {100.0: 5000.0}
        asks = {101.0: 5000.0}
        current_price = 100.0

    detector = IcebergDetector(min_hidden_notional_usdt=1_000_000, min_absorption_rate=0.7)
    engine = A1AbsorptionEngine(Ctx(), detector, phase1_truth_tracker=False)
    assert detector.detect_buy_iceberg(2_000_000, 500_000)["is_iceberg"] is True
    assert engine.min_event_start_notional_usdt == 300_000


def test_candidate_snapshot_contains_active_side_ratio_source():
    class Ctx:
        bids = {}
        asks = {}
        current_price = 100.0

    engine = A1AbsorptionEngine(Ctx(), IcebergDetector(), phase1_truth_tracker=False)
    snapshot = engine._build_phase1_truth_candidate_snapshot(
        event={
            "event_id": "pie-test",
            "direction": "BUY",
            "trigger_price": 100.0,
            "trigger_ts": 100.0,
            "trigger_recv_ts": 100.0,
            "active_notional": 1_000_000,
            "active_size": 10,
            "trade_count": 3,
            "zone_lower": 99.0,
            "zone_upper": 100.5,
            "start_thickness_usdt": 800_000,
            "min_trade_price": 99.8,
            "max_trade_price": 100.0,
        },
        result="ICEBERG",
        signal={"hidden_volume": 1_500_000, "absorption_rate": 0.8, "behavior": "ICEBERG_ABSORPTION"},
        current_price=100.0,
        book_reduction=100_000,
        wait_ms=300,
        settle_ts=101.0,
        settle_recv_ts=101.0,
    )
    assert snapshot["active_side_ratio_source"] == "constructed_same_side_pending_event"
