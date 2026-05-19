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
PHASE3_REAL_TRADING_ENABLED = _bool_config(
    "PHASE3_REAL_TRADING_ENABLED",
    False,
)

MAX_ACTIVE_PHASE2_ZONES = _int_config("MAX_ACTIVE_PHASE2_ZONES", 20)
PHASE2_ZONE_TTL_SECONDS = _float_config("PHASE2_ZONE_TTL_SECONDS", 1800.0)
