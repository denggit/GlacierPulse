#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.research.zone_truth.a2_accumulation_v2 import classify_a2_accumulation_path_v2


def _path(row):
    return classify_a2_accumulation_path_v2(row)["a2_accumulation_path_v2"]


def test_clean_hold():
    assert _path({"a2_pre_pool_eligible": True, "has_clean_hold": True}) == "A2_CLEAN_HOLD"


def test_sweep_reclaim_retest():
    assert _path({"a2_sweep_flag": True, "has_absorbed_after_sweep": True, "a2_reclaim_flag": True, "a2_retest_flag": True}) == "A2_SWEEP_RECLAIM_RETEST"


def test_sweep_reclaim_no_retest():
    assert _path({"a2_sweep_flag": True, "has_absorbed_after_sweep": True, "a2_reclaim_flag": True}) == "A2_SWEEP_RECLAIM_NO_RETEST"


def test_below_zone_absorption():
    assert _path({"phase2_type": "BELOW_ZONE_ABSORPTION"}) == "A2_BELOW_ZONE_ABSORPTION"


def test_ladder_hold():
    assert _path({"ladder_absorption_flag": True}) == "A2_LADDER_HOLD"


def test_visible_wall_hold():
    assert _path({"visible_wall_absorption_flag": True}) == "A2_VISIBLE_WALL_HOLD"


def test_failed_reclaim_priority():
    assert _path({"a2_failed_reclaim_flag": True, "has_clean_hold": True, "ladder_absorption_flag": True}) == "A2_FAILED_RECLAIM"


def test_no_accumulation_fallback():
    assert _path({"a2_pre_pool_eligible": True}) == "A2_NO_ACCUMULATION"
