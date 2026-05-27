#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
V6.2 Research Evaluator configuration.

These switches only enable research/simulation evaluators. Real trading for
Phase3 stays disabled by default and must not be used by V6.2 step1.
"""

import os
from typing import Any

from config.env_loader import load_env_config


_ENV_CONFIG = load_env_config()


def _raw_value(name: str, default: Any) -> Any:
    return os.getenv(name, _ENV_CONFIG.get(name, default))


def _bool_config(name: str, default: bool) -> bool:
    value = _raw_value(name, default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return bool(default)


def _int_config(name: str, default: int) -> int:
    value = _raw_value(name, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _float_config(name: str, default: float) -> float:
    value = _raw_value(name, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _str_config(name: str, default: str) -> str:
    value = _raw_value(name, default)
    if value is None:
        return str(default)
    return str(value)


def _int_list_config(name: str, default: list[int]) -> list[int]:
    value = _raw_value(name, ",".join(str(v) for v in default))
    if isinstance(value, (list, tuple)):
        raw_parts = value
    else:
        raw_parts = str(value).split(",")
    result: list[int] = []
    for part in raw_parts:
        try:
            result.append(int(str(part).strip()))
        except (TypeError, ValueError):
            continue
    return result or list(default)


def _float_list_config(name: str, default: list[float]) -> list[float]:
    value = _raw_value(name, ",".join(str(v) for v in default))
    if isinstance(value, (list, tuple)):
        raw_parts = value
    else:
        raw_parts = str(value).split(",")
    result: list[float] = []
    for part in raw_parts:
        try:
            result.append(float(str(part).strip()))
        except (TypeError, ValueError):
            continue
    return result or list(default)


V62_LOG_PROFILE = _str_config(
    "V62_LOG_PROFILE",
    "PRODUCTION_SAFE",
).upper()


# V7.0.0 shadow-only 3A research loop configuration.
A1_EVIDENCE_V2_ENABLED = _bool_config("A1_EVIDENCE_V2_ENABLED", True)
A1_EVIDENCE_V2_SHADOW_ONLY = _bool_config("A1_EVIDENCE_V2_SHADOW_ONLY", True)
A1_VISIBLE_WALL_MIN_START_DEPTH_USDT = _float_config("A1_VISIBLE_WALL_MIN_START_DEPTH_USDT", 500000.0)
A1_VISIBLE_WALL_MIN_ACTIVE_NOTIONAL_USDT = _float_config("A1_VISIBLE_WALL_MIN_ACTIVE_NOTIONAL_USDT", 300000.0)
A1_VISIBLE_WALL_MAX_WITHDRAWAL_EXCESS_RATIO = _float_config("A1_VISIBLE_WALL_MAX_WITHDRAWAL_EXCESS_RATIO", 0.35)
A1_VISIBLE_WALL_MIN_CONSUMPTION_RATIO = _float_config("A1_VISIBLE_WALL_MIN_CONSUMPTION_RATIO", 0.45)
A1_VISIBLE_WALL_MAX_CONSUMPTION_RATIO = _float_config("A1_VISIBLE_WALL_MAX_CONSUMPTION_RATIO", 1.35)
A1_VISIBLE_WALL_MIN_SURVIVAL_RATIO = _float_config("A1_VISIBLE_WALL_MIN_SURVIVAL_RATIO", 0.20)
A1_CLUSTER_WINDOWS_SEC = _int_list_config("A1_CLUSTER_WINDOWS_SEC", [3, 10, 30, 120])
A1_CLUSTER_MIN_ACTIVE_NOTIONAL_USDT = _float_config("A1_CLUSTER_MIN_ACTIVE_NOTIONAL_USDT", 1000000.0)
A1_CLUSTER_MAX_PRICE_EFFICIENCY = _float_config("A1_CLUSTER_MAX_PRICE_EFFICIENCY", 0.0008)
A1_CLUSTER_MIN_EVENT_COUNT = _int_config("A1_CLUSTER_MIN_EVENT_COUNT", 2)
A1_LADDER_BUCKET_SIZE_U = _float_config("A1_LADDER_BUCKET_SIZE_U", 0.5)
A1_LADDER_MIN_LEVEL_COUNT = _int_config("A1_LADDER_MIN_LEVEL_COUNT", 3)

ZONE_BOUNDARY_V2_ENABLED = _bool_config("ZONE_BOUNDARY_V2_ENABLED", True)
ZONE_BOUNDARY_V2_SHADOW_ONLY = _bool_config("ZONE_BOUNDARY_V2_SHADOW_ONLY", True)
ZONE_BOUNDARY_V2_SCAN_RANGE_U = _float_config("ZONE_BOUNDARY_V2_SCAN_RANGE_U", 8.0)
ZONE_BOUNDARY_V2_BUCKET_SIZE_U = _float_config("ZONE_BOUNDARY_V2_BUCKET_SIZE_U", 0.5)
ZONE_BOUNDARY_V2_MIN_TRADE_NOTIONAL_PER_BUCKET = _float_config("ZONE_BOUNDARY_V2_MIN_TRADE_NOTIONAL_PER_BUCKET", 20000.0)
ZONE_BOUNDARY_V2_MIN_RECOVERY_RATIO = _float_config("ZONE_BOUNDARY_V2_MIN_RECOVERY_RATIO", 0.70)
ZONE_BOUNDARY_V2_MIN_END_VS_START = _float_config("ZONE_BOUNDARY_V2_MIN_END_VS_START", 0.90)
ZONE_BOUNDARY_V2_OVER_RELOAD_RATIO = _float_config("ZONE_BOUNDARY_V2_OVER_RELOAD_RATIO", 1.05)
ZONE_BOUNDARY_V2_STRUCTURAL_STOP_BUFFER_U = _float_config("ZONE_BOUNDARY_V2_STRUCTURAL_STOP_BUFFER_U", 0.5)
ZONE_BOUNDARY_V2_WRITE_PROFILE_MAPS = _bool_config("ZONE_BOUNDARY_V2_WRITE_PROFILE_MAPS", False)

V7_3A_SIMULATOR_ENABLED = _bool_config("V7_3A_SIMULATOR_ENABLED", True)
V7_3A_ROUNDTRIP_FEE_PCT = _float_config("V7_3A_ROUNDTRIP_FEE_PCT", 0.001)
V7_3A_TARGET_R_LIST = _float_list_config("V7_3A_TARGET_R_LIST", [1.0, 1.5, 2.0])
V7_3A_MIN_SAMPLE = _int_config("V7_3A_MIN_SAMPLE", 10)
V7_3A_TOP_COMBO_LIMIT = _int_config("V7_3A_TOP_COMBO_LIMIT", 100)


def _log_default_for_profile(
    detailed_default: bool,
    key_events_default: bool,
    production_default: bool,
) -> bool:
    profile = V62_LOG_PROFILE.upper()
    if profile == "RESEARCH_KEY_EVENTS":
        return bool(key_events_default)
    if profile == "PRODUCTION_SAFE":
        return bool(production_default)
    return bool(detailed_default)


V62_LOG_PENDING_ICEBERG_ENABLED = _bool_config(
    "V62_LOG_PENDING_ICEBERG_ENABLED",
    False,
)
V62_LOG_IGNORE_ICEBERG_ENABLED = _bool_config(
    "V62_LOG_IGNORE_ICEBERG_ENABLED",
    False,
)
V62_LOG_SPOOFING_WITHDRAWAL_ENABLED = _bool_config(
    "V62_LOG_SPOOFING_WITHDRAWAL_ENABLED",
    False,
)
V62_LOG_SETTLED_ICEBERG_ENABLED = _bool_config(
    "V62_LOG_SETTLED_ICEBERG_ENABLED",
    False,
)
V62_LOG_PHASE1_QUALITY_ENABLED = _bool_config(
    "V62_LOG_PHASE1_QUALITY_ENABLED",
    False,
)
V62_LOG_CANCEL_ICEBERG_ENABLED = _bool_config(
    "V62_LOG_CANCEL_ICEBERG_ENABLED",
    False,
)
V62_LOG_PENDING_DROP_ENABLED = _bool_config(
    "V62_LOG_PENDING_DROP_ENABLED",
    _log_default_for_profile(True, True, True),
)
V62_LOG_A1_ICEBERG_EVENT_ENABLED = _bool_config(
    "V62_LOG_A1_ICEBERG_EVENT_ENABLED",
    _log_default_for_profile(True, True, False),
)
V62_LOG_A1_ZONE_NEW_ENABLED = _bool_config(
    "V62_LOG_A1_ZONE_NEW_ENABLED",
    False,
)
V62_LOG_A1_ZONE_UPDATE_ENABLED = _bool_config(
    "V62_LOG_A1_ZONE_UPDATE_ENABLED",
    False,
)
V62_LOG_A1_ZONE_BROKEN_ENABLED = _bool_config(
    "V62_LOG_A1_ZONE_BROKEN_ENABLED",
    _log_default_for_profile(True, True, True),
)
V62_LOG_A1_ZONE_EXPIRED_ENABLED = _bool_config(
    "V62_LOG_A1_ZONE_EXPIRED_ENABLED",
    False,
)
V62_LOG_A1_ZONE_STRESSED_ENABLED = _bool_config(
    "V62_LOG_A1_ZONE_STRESSED_ENABLED",
    False,
)
V62_LOG_A1_ZONE_FROZEN_ENABLED = _bool_config(
    "V62_LOG_A1_ZONE_FROZEN_ENABLED",
    False,
)
V62_LOG_A1_ZONE_OUTCOME_ENABLED = _bool_config(
    "V62_LOG_A1_ZONE_OUTCOME_ENABLED",
    False,
)
V62_LOG_PHASE2_REGISTERED_ENABLED = _bool_config(
    "V62_LOG_PHASE2_REGISTERED_ENABLED",
    False,
)
V62_LOG_PHASE2_STATE_ENABLED = _bool_config(
    "V62_LOG_PHASE2_STATE_ENABLED",
    False,
)
V62_LOG_PHASE2_CONFIRMED_ENABLED = _bool_config(
    "V62_LOG_PHASE2_CONFIRMED_ENABLED",
    _log_default_for_profile(True, True, True),
)
V62_LOG_PHASE3_CANDIDATE_ENABLED = _bool_config(
    "V62_LOG_PHASE3_CANDIDATE_ENABLED",
    _log_default_for_profile(True, True, True),
)
V62_LOG_VIRTUAL_POSITION_OPEN_ENABLED = _bool_config(
    "V62_LOG_VIRTUAL_POSITION_OPEN_ENABLED",
    _log_default_for_profile(True, True, True),
)
V62_LOG_VIRTUAL_POSITION_UPDATE_ENABLED = _bool_config(
    "V62_LOG_VIRTUAL_POSITION_UPDATE_ENABLED",
    _log_default_for_profile(True, False, False),
)
V62_LOG_VIRTUAL_POSITION_CLOSE_ENABLED = _bool_config(
    "V62_LOG_VIRTUAL_POSITION_CLOSE_ENABLED",
    _log_default_for_profile(True, True, True),
)
V62_LOG_VIRTUAL_POSITION_SKIP_ENABLED = _bool_config(
    "V62_LOG_VIRTUAL_POSITION_SKIP_ENABLED",
    _log_default_for_profile(True, True, True),
)
V62_LOG_VIRTUAL_POSITION_BLOCKED_ENABLED = _bool_config(
    "V62_LOG_VIRTUAL_POSITION_BLOCKED_ENABLED",
    _log_default_for_profile(True, True, True),
)
V62_LOG_VIRTUAL_SUPPORT_UPDATE_ENABLED = _bool_config(
    "V62_LOG_VIRTUAL_SUPPORT_UPDATE_ENABLED",
    _log_default_for_profile(True, True, False),
)
V62_LOG_VIRTUAL_STOP_UPDATE_ENABLED = _bool_config(
    "V62_LOG_VIRTUAL_STOP_UPDATE_ENABLED",
    _log_default_for_profile(True, True, True),
)
V62_LOG_PHASE3_OUTCOME_ENABLED = _bool_config(
    "V62_LOG_PHASE3_OUTCOME_ENABLED",
    _log_default_for_profile(True, True, True),
)
V62_LOG_PHASE3_OUTCOME_SUMMARY_ENABLED = _bool_config(
    "V62_LOG_PHASE3_OUTCOME_SUMMARY_ENABLED",
    _log_default_for_profile(True, True, True),
)
V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED = _bool_config(
    "V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED",
    _log_default_for_profile(True, True, True),
)


PHASE2_ORDERFLOW_EVALUATOR_ENABLED = _bool_config(
    "PHASE2_ORDERFLOW_EVALUATOR_ENABLED",
    True,
)
PHASE3_CANDIDATE_EVALUATOR_ENABLED = _bool_config(
    "PHASE3_CANDIDATE_EVALUATOR_ENABLED",
    True,
)
VIRTUAL_POSITION_MANAGER_ENABLED = _bool_config(
    "VIRTUAL_POSITION_MANAGER_ENABLED",
    True,
)
A1_REACTION_TO_VIRTUAL_POSITION_ENABLED = _bool_config(
    "A1_REACTION_TO_VIRTUAL_POSITION_ENABLED",
    False,
)
REAL_EXECUTION_ENABLED = _bool_config("REAL_EXECUTION_ENABLED", False)
VIRTUAL_SHADOW_MODE = _bool_config("VIRTUAL_SHADOW_MODE", False)
PHASE3_OUTCOME_EVALUATOR_ENABLED = _bool_config("PHASE3_OUTCOME_EVALUATOR_ENABLED", True)

VIRTUAL_INITIAL_EQUITY_USDT = _float_config("VIRTUAL_INITIAL_EQUITY_USDT", 1000.0)
VIRTUAL_MAX_CLOSED_POSITIONS = _int_config("VIRTUAL_MAX_CLOSED_POSITIONS", 1000)
PHASE3_OUTCOME_MAX_CLOSED_POSITIONS = _int_config("PHASE3_OUTCOME_MAX_CLOSED_POSITIONS", 5000)
PHASE3_OUTCOME_SUMMARY_LOG_INTERVAL_SEC = _float_config("PHASE3_OUTCOME_SUMMARY_LOG_INTERVAL_SEC", 300.0)
PHASE3_OUTCOME_MIN_GROUP_SAMPLE_SIZE = _int_config("PHASE3_OUTCOME_MIN_GROUP_SAMPLE_SIZE", 5)
PHASE3_OUTCOME_KEEP_RECENT_EVENTS = _bool_config("PHASE3_OUTCOME_KEEP_RECENT_EVENTS", True)
PHASE3_OUTCOME_WARN_NON_CANONICAL_CANDIDATE_TYPE = _bool_config(
    "PHASE3_OUTCOME_WARN_NON_CANONICAL_CANDIDATE_TYPE",
    True,
)
PHASE3_OUTCOME_UNKNOWN_CANDIDATE_TYPE = _str_config(
    "PHASE3_OUTCOME_UNKNOWN_CANDIDATE_TYPE",
    "UNKNOWN_CANDIDATE_TYPE",
)
PHASE3_OUTCOME_DEDUP_ENABLED = _bool_config("PHASE3_OUTCOME_DEDUP_ENABLED", True)
PHASE3_OUTCOME_DEDUP_MAX_IDS = _int_config("PHASE3_OUTCOME_DEDUP_MAX_IDS", 10000)
VIRTUAL_TAKE_PROFIT_R_MULTIPLE = _float_config("VIRTUAL_TAKE_PROFIT_R_MULTIPLE", 1.5)
VIRTUAL_BREAKEVEN_ENABLED = _bool_config("VIRTUAL_BREAKEVEN_ENABLED", True)
VIRTUAL_TRAILING_ENABLED = _bool_config("VIRTUAL_TRAILING_ENABLED", True)
VIRTUAL_BREAKEVEN_TRIGGER_R = _float_config("VIRTUAL_BREAKEVEN_TRIGGER_R", 1.0)
VIRTUAL_BREAKEVEN_OFFSET_R = _float_config("VIRTUAL_BREAKEVEN_OFFSET_R", 0.05)
VIRTUAL_TRAILING_TRIGGER_R = _float_config("VIRTUAL_TRAILING_TRIGGER_R", 1.5)
VIRTUAL_TRAILING_DISTANCE_R = _float_config("VIRTUAL_TRAILING_DISTANCE_R", 0.8)
VIRTUAL_SUPPORT_UPDATE_ENABLED = _bool_config("VIRTUAL_SUPPORT_UPDATE_ENABLED", True)
VIRTUAL_SUPPORT_MAX_ZONE_IDS = _int_config("VIRTUAL_SUPPORT_MAX_ZONE_IDS", 20)
VIRTUAL_SUPPORT_MIN_PHASE2_SCORE = _float_config("VIRTUAL_SUPPORT_MIN_PHASE2_SCORE", 0.65)
VIRTUAL_SUPPORT_REQUIRE_SAME_DIRECTION = _bool_config("VIRTUAL_SUPPORT_REQUIRE_SAME_DIRECTION", True)
VIRTUAL_STOP_UPDATE_MIN_IMPROVEMENT_U = _float_config("VIRTUAL_STOP_UPDATE_MIN_IMPROVEMENT_U", 0.1)
VIRTUAL_UPDATE_LOG_MIN_INTERVAL_SEC = _float_config("VIRTUAL_UPDATE_LOG_MIN_INTERVAL_SEC", 30.0)
VIRTUAL_ALLOW_REPLACE_POSITION = _bool_config("VIRTUAL_ALLOW_REPLACE_POSITION", False)
PHASE3_REAL_TRADING_ENABLED = _bool_config(
    "PHASE3_REAL_TRADING_ENABLED",
    False,
)

PHASE3_MAX_ACCOUNT_LOSS_PCT = _float_config("PHASE3_MAX_ACCOUNT_LOSS_PCT", 0.03)
PHASE3_LEVERAGE = _float_config("PHASE3_LEVERAGE", 50.0)
PHASE3_ROUNDTRIP_FEE_PCT = _float_config("PHASE3_ROUNDTRIP_FEE_PCT", 0.001)
PHASE3_SLIPPAGE_BUFFER_PCT = _float_config("PHASE3_SLIPPAGE_BUFFER_PCT", 0.0002)
PHASE3_MAX_MARGIN_USAGE_PCT = _float_config("PHASE3_MAX_MARGIN_USAGE_PCT", 0.95)
PHASE3_MIN_MARGIN_USAGE_PCT = _float_config("PHASE3_MIN_MARGIN_USAGE_PCT", 0.02)

PHASE3_STOP_BUFFER_USDT = _float_config("PHASE3_STOP_BUFFER_USDT", 0.5)
PHASE3_CLEAN_HOLD_STOP_BUFFER_USDT = _float_config("PHASE3_CLEAN_HOLD_STOP_BUFFER_USDT", 0.8)
PHASE3_BELOW_ZONE_STOP_BUFFER_USDT = _float_config("PHASE3_BELOW_ZONE_STOP_BUFFER_USDT", 0.5)

PHASE3_MAX_RISK_DISTANCE_PCT = _float_config("PHASE3_MAX_RISK_DISTANCE_PCT", 0.006)
# Reserved for future target/outcome-aware reward-risk evaluation.
# V6.2.5 has no target price, virtual position, or outcome yet, so this is not used.
PHASE3_MIN_REWARD_RISK_PROXY = _float_config("PHASE3_MIN_REWARD_RISK_PROXY", 1.0)

PHASE3_MIN_SWEEP_RECLAIM_PHASE2_SCORE = _float_config("PHASE3_MIN_SWEEP_RECLAIM_PHASE2_SCORE", 0.65)
PHASE3_MIN_CLEAN_HOLD_PHASE2_SCORE = _float_config("PHASE3_MIN_CLEAN_HOLD_PHASE2_SCORE", 0.72)
PHASE3_MIN_BELOW_ZONE_PHASE2_SCORE = _float_config("PHASE3_MIN_BELOW_ZONE_PHASE2_SCORE", 0.70)
PHASE3_MIN_BELOW_ZONE_ABSORPTION_SCORE = _float_config("PHASE3_MIN_BELOW_ZONE_ABSORPTION_SCORE", 0.65)

PHASE3_ALLOW_BELOW_ZONE_WITHOUT_BOOK_DEPTH = _bool_config(
    "PHASE3_ALLOW_BELOW_ZONE_WITHOUT_BOOK_DEPTH",
    False,
)

MAX_ACTIVE_PHASE2_ZONES = _int_config("MAX_ACTIVE_PHASE2_ZONES", 20)
PHASE2_ZONE_TTL_SECONDS = _float_config("PHASE2_ZONE_TTL_SECONDS", 1800.0)

BOOK_NEAR_ZONE_RANGE_USDT = _float_config("BOOK_NEAR_ZONE_RANGE_USDT", 1.0)
BOOK_NEAR_SWEEP_RANGE_USDT = _float_config("BOOK_NEAR_SWEEP_RANGE_USDT", 1.0)

PHASE2_TEST_ZONE_BUFFER_USDT = _float_config("PHASE2_TEST_ZONE_BUFFER_USDT", 0.5)
PHASE2_RECLAIM_BUFFER_USDT = _float_config("PHASE2_RECLAIM_BUFFER_USDT", 0.2)
PHASE2_RETEST_BUFFER_USDT = _float_config("PHASE2_RETEST_BUFFER_USDT", 0.5)

PHASE2_MIN_ACTIVE_NOTIONAL_3S = _float_config("PHASE2_MIN_ACTIVE_NOTIONAL_3S", 100000.0)
PHASE2_MIN_ABSORPTION_SCORE = _float_config("PHASE2_MIN_ABSORPTION_SCORE", 0.55)
PHASE2_MIN_RELOAD_SCORE = _float_config("PHASE2_MIN_RELOAD_SCORE", 0.2)
PHASE2_MIN_RECLAIM_SCORE = _float_config("PHASE2_MIN_RECLAIM_SCORE", 0.5)
PHASE2_MIN_RETEST_SCORE = _float_config("PHASE2_MIN_RETEST_SCORE", 0.5)
PHASE2_MIN_TOTAL_SCORE = _float_config("PHASE2_MIN_TOTAL_SCORE", 0.65)
PHASE2_MIN_BELOW_ZONE_TOTAL_SCORE = _float_config("PHASE2_MIN_BELOW_ZONE_TOTAL_SCORE", 0.60)

PHASE2_MAX_SWEEP_DEPTH_PCT_SOFT = _float_config("PHASE2_MAX_SWEEP_DEPTH_PCT_SOFT", 0.003)
PHASE2_MAX_SWEEP_DEPTH_PCT_HARD = _float_config("PHASE2_MAX_SWEEP_DEPTH_PCT_HARD", 0.008)
PHASE2_MAX_TIME_BELOW_MS = _float_config("PHASE2_MAX_TIME_BELOW_MS", 180000.0)
PHASE2_MAX_TIME_ABOVE_MS = _float_config("PHASE2_MAX_TIME_ABOVE_MS", 180000.0)

# Reserved for future non-transition summary logs.
# Current [PHASE2-STATE] is transition-only and does not require throttling.
PHASE2_STATE_LOG_MIN_INTERVAL_SEC = _float_config("PHASE2_STATE_LOG_MIN_INTERVAL_SEC", 1.0)

V62_INTEGRATION_HEARTBEAT_ENABLED = _bool_config(
    "V62_INTEGRATION_HEARTBEAT_ENABLED",
    True,
)
V62_INTEGRATION_HEARTBEAT_INTERVAL_SEC = _float_config(
    "V62_INTEGRATION_HEARTBEAT_INTERVAL_SEC",
    300.0,
)
V62_STARTUP_SAFETY_CHECK_ENABLED = _bool_config(
    "V62_STARTUP_SAFETY_CHECK_ENABLED",
    True,
)
V62_REQUIRE_RESEARCH_ONLY_MODE = _bool_config(
    "V62_REQUIRE_RESEARCH_ONLY_MODE",
    True,
)
V62_SHADOW_RUN_LABEL = _str_config(
    "V62_SHADOW_RUN_LABEL",
    "V6.2-research-shadow",
)
V62_ENABLE_FINAL_RUN_SUMMARY = _bool_config(
    "V62_ENABLE_FINAL_RUN_SUMMARY",
    True,
)
V62_LOG_COMPONENT_STATUS_ON_START = _bool_config(
    "V62_LOG_COMPONENT_STATUS_ON_START",
    True,
)
V62_LOG_CONFIG_SNAPSHOT_ON_START = _bool_config(
    "V62_LOG_CONFIG_SNAPSHOT_ON_START",
    True,
)
V62_HEARTBEAT_INCLUDE_GROUP_SUMMARY = _bool_config(
    "V62_HEARTBEAT_INCLUDE_GROUP_SUMMARY",
    True,
)


A1_REACTION_RESEARCH_COVERAGE_ENABLED = _bool_config("A1_REACTION_RESEARCH_COVERAGE_ENABLED", True)
A1_REACTION_FAST_MOVE_ENABLED = _bool_config("A1_REACTION_FAST_MOVE_ENABLED", True)
A1_REACTION_FAST_MOVE_WINDOW_SEC = _float_config("A1_REACTION_FAST_MOVE_WINDOW_SEC", 3.0)
A1_REACTION_FAST_MOVE_MIN_DISTANCE_U = _float_config("A1_REACTION_FAST_MOVE_MIN_DISTANCE_U", 1.0)
A1_REACTION_FAST_MOVE_MIN_DISTANCE_PCT = _float_config("A1_REACTION_FAST_MOVE_MIN_DISTANCE_PCT", 0.0003)
A1_REACTION_FAST_MOVE_MIN_ACTIVE_NOTIONAL_3S = _float_config("A1_REACTION_FAST_MOVE_MIN_ACTIVE_NOTIONAL_3S", 0.0)

A1_REACTION_EVENT_RECORDER_ENABLED = _bool_config("A1_REACTION_EVENT_RECORDER_ENABLED", True)
A1_REACTION_EVENT_RECORDER_WRITE_JSONL = _bool_config("A1_REACTION_EVENT_RECORDER_WRITE_JSONL", False)
A1_REACTION_EVENT_RECORDER_JSONL_PATH = _str_config(
    "A1_REACTION_EVENT_RECORDER_JSONL_PATH",
    "logs/research/a1_reaction_events.jsonl",
)
A1_REACTION_EVENT_RECORDER_MAX_RECENT_EVENTS = _int_config("A1_REACTION_EVENT_RECORDER_MAX_RECENT_EVENTS", 5000)

V62_LOG_A1_REACTION_RESEARCH_EVENT_ENABLED = _bool_config("V62_LOG_A1_REACTION_RESEARCH_EVENT_ENABLED", False)


PHASE1_TRUTH_SHADOW_ENABLED = _bool_config("PHASE1_TRUTH_SHADOW_ENABLED", True)
PHASE1_CANDIDATE_RECORDER_ENABLED = _bool_config("PHASE1_CANDIDATE_RECORDER_ENABLED", True)
PHASE1_CANDIDATE_RECORDER_WRITE_JSONL = _bool_config("PHASE1_CANDIDATE_RECORDER_WRITE_JSONL", True)
PHASE1_CANDIDATE_RECORDER_JSONL_PATH = _str_config(
    "PHASE1_CANDIDATE_RECORDER_JSONL_PATH",
    "logs/research/phase1_candidates.jsonl",
)
PHASE1_TRUTH_POST_WINDOWS_SEC = _int_list_config("PHASE1_TRUTH_POST_WINDOWS_SEC", [1, 5, 30, 120])
PHASE1_TRUTH_MAX_ACTIVE_OBSERVATIONS = _int_config("PHASE1_TRUTH_MAX_ACTIVE_OBSERVATIONS", 500)
PHASE1_TRUTH_FINALIZE_AFTER_SEC = _float_config("PHASE1_TRUTH_FINALIZE_AFTER_SEC", 120.0)

A1_DYNAMIC_PARAM_PREVIEW_ENABLED = _bool_config("A1_DYNAMIC_PARAM_PREVIEW_ENABLED", True)
A1_DYNAMIC_PARAM_PREVIEW_JSON_PATH = _str_config(
    "A1_DYNAMIC_PARAM_PREVIEW_JSON_PATH",
    "runtime_state/a1_dynamic_params.json",
)
A1_DYNAMIC_PARAM_PREVIEW_INTERVAL_SEC = _float_config("A1_DYNAMIC_PARAM_PREVIEW_INTERVAL_SEC", 300.0)
A1_DYNAMIC_PARAM_MODE = _str_config("A1_DYNAMIC_PARAM_MODE", "preview_only")
