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
REAL_EXECUTION_ENABLED = _bool_config("REAL_EXECUTION_ENABLED", False)
VIRTUAL_SHADOW_MODE = _bool_config("VIRTUAL_SHADOW_MODE", False)

VIRTUAL_INITIAL_EQUITY_USDT = _float_config("VIRTUAL_INITIAL_EQUITY_USDT", 1000.0)
VIRTUAL_MAX_CLOSED_POSITIONS = _int_config("VIRTUAL_MAX_CLOSED_POSITIONS", 1000)
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
