#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Iterable, Mapping

from .runtime_engine import RuntimeThreeABacktestEngine, default_runtime_engine_config


def build_runtime_strategy_reports(
    rows: Iterable[Mapping[str, Any]],
    bars: Iterable[Mapping[str, Any]] | None = None,
    *,
    trade_events: Iterable[Mapping[str, Any]] | None = None,
    expiry_secs: Iterable[int] | None = None,
    stop_model: str | None = None,
    target_model: str | None = None,
    next_tick_entry: bool | None = None,
    enable_audit: bool | None = None,
    a2_rt_min_quiet_sec: float | None = None,
    a2_rt_min_tick_count: int | None = None,
) -> dict[str, list[dict[str, Any]] | dict[str, Any]]:
    overrides: dict[str, Any] = {}
    if expiry_secs is not None:
        overrides["expiry_secs"] = list(expiry_secs)
    if stop_model is not None:
        overrides["stop_model"] = stop_model
    if target_model is not None:
        overrides["target_model"] = target_model
    if next_tick_entry is not None:
        overrides["next_tick_entry"] = next_tick_entry
    if enable_audit is not None:
        overrides["enable_audit"] = enable_audit
    if a2_rt_min_quiet_sec is not None:
        overrides["a2_rt_min_quiet_sec"] = a2_rt_min_quiet_sec
    if a2_rt_min_tick_count is not None:
        overrides["a2_rt_min_tick_count"] = a2_rt_min_tick_count
    config = default_runtime_engine_config(**overrides)
    return RuntimeThreeABacktestEngine(config).run(rows, trade_events, bars)
