#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


SCHEMA_VERSION = "v6.3.11.phase1_truth.1"


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


@dataclass
class Phase1Observation:
    candidate: Dict[str, Any]
    settle_ts: float
    settle_recv_ts: float
    created_recv_ts: float
    post_trade_count: int = 0
    post_total_notional: float = 0.0
    post_buy_notional: float = 0.0
    post_sell_notional: float = 0.0
    post_cvd_delta: float = 0.0
    post_min_price: float = 0.0
    post_max_price: float = 0.0
    post_last_price: float = 0.0
    post_5s_cvd_delta: float = 0.0
    post_30s_cvd_delta: float = 0.0
    post_5s_min_price: float = 0.0
    post_5s_max_price: float = 0.0
    post_30s_min_price: float = 0.0
    post_30s_max_price: float = 0.0
    time_outside_zone: float = 0.0
    time_outside_zone_30s: float = 0.0
    reclaim_time_sec: float | None = None
    accepted_beyond_zone: bool = False
    depth_recovery_ratio_1s: float = 0.0
    depth_recovery_ratio_5s: float = 0.0
    depth_recovery_ratio_30s: float = 0.0
    replenish_count: int = 0
    reload_interval_ms: float = 0.0
    local_depth_min: float = 0.0
    local_depth_max: float = 0.0
    local_depth_last: float = 0.0
    _last_trade_ts: float = 0.0
    _last_book_ts: float = 0.0
    _last_outside_state: bool = False
    _first_replenish_ts: float = 0.0
    _last_replenish_ts: float = 0.0
    _seen_sweep: bool = False
    checkpoints: Dict[int, Dict[str, Any]] = field(default_factory=dict)

    @property
    def event_key(self) -> str:
        return str(self.candidate.get("event_key") or self.candidate.get("event_id") or "")
