#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import statistics
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from config import research_evaluator as cfg
from src.research.a1_edge.schema import parse_bool, parse_float
from src.research.no_future_audit import validate_entry_conditions, validate_trade_row
from src.research.runtime_three_a.runtime_event_source import RuntimeEventSource, runtime_performance_warning

from .a2_runtime_state import (
    A2RuntimeConfig,
    A2RuntimeStateMachine,
    A2_A3_TRIGGERED,
    A2_EXPIRED,
    A2_INVALIDATED,
    A2_READY_FOR_A3,
)
from .a3_runtime_entry import A3RuntimeConfig, ENTRY_CONDITION_FIELDS, evaluate_a3_runtime_entry
from .stop_models import build_stop
from .target_models import build_target_candidates


RUNTIME_STATUS_SKIPPED_NO_TRADE_EVENTS = "SKIPPED_NO_TRADE_EVENTS"
RUNTIME_STATUS_NO_RT_A3_SIGNALS = "NO_RT_A3_SIGNALS"
RUNTIME_STATUS_OK = "OK"


VARIANTS = {
    "A_CORE_NO_VP": lambda row: True,
    "B_A1_VP_ADVANTAGE": lambda row: _vp_setup(row) in _advantage_setups(),
    "C_NO_A1_HVN_DANGER": lambda row: _vp_setup(row) not in {"BUY_NEAR_HVN_DANGER", "SELL_NEAR_HVN_DANGER"},
    "D_DIRECTIONAL_LVN_HVN": lambda row: _vp_setup(row) in {"BUY_LVN_BELOW_HVN_ABSORB", "SELL_LVN_ABOVE_HVN_ABSORB"},
    "E_VALUE_EDGE": lambda row: _vp_setup(row) in {"BUY_BELOW_VAL_ABSORB", "BUY_NEAR_VAL_ABSORB", "SELL_ABOVE_VAH_ABSORB", "SELL_NEAR_VAH_ABSORB"},
    "F_STRICT": lambda row: _vp_setup(row) in _advantage_setups()
    and _vp_setup(row) not in {"BUY_NEAR_HVN_DANGER", "SELL_NEAR_HVN_DANGER"}
    and parse_float(row.get("fee_share_r")) <= 1.0
    and str(row.get("a2_context_alignment") or "").upper() != "COUNTER_TREND"
    and str(row.get("strong_a1_tier") or "").upper() in {"STRONG_A1_RAW", "STRONG_A1"},
}


@dataclass(frozen=True)
class RuntimeThreeAEngineConfig:
    expiry_secs: list[int] = field(default_factory=lambda: [180, 300, 600, 900, 1200, 1800])
    a2: A2RuntimeConfig = field(default_factory=A2RuntimeConfig)
    a3: A3RuntimeConfig = field(default_factory=A3RuntimeConfig)
    stop_model: str = "STOP_STRUCTURAL_ZONE_V2"
    target_model: str = "TARGET_FIXED_2R"
    next_tick_entry: bool = False
    enable_audit: bool = True
    max_fee_share_r: float = 1.0
    outcome_window_sec: int = 3600
    default_expiry_sec: int = 900


class RuntimeThreeABacktestEngine:
    def __init__(self, config: RuntimeThreeAEngineConfig | None = None) -> None:
        self.config = config or default_runtime_engine_config()
        validate_entry_conditions(ENTRY_CONDITION_FIELDS)

    def run(
        self,
        zones: Iterable[Mapping[str, Any]],
        trade_events: Iterable[Mapping[str, Any]] | None,
        bars: Iterable[Mapping[str, Any]] | None = None,
    ) -> dict[str, list[dict[str, Any]] | dict[str, Any]]:
        expiry_values = sorted({int(x) for x in self.config.expiry_secs})
        event_source, ticks, tick_ts = _prepare_runtime_events(trade_events)
        if (event_source is None and not ticks) or (isinstance(event_source, RuntimeEventSource) and not event_source.files):
            return _empty_reports(expiry_values, RUNTIME_STATUS_SKIPPED_NO_TRADE_EVENTS, self.config)

        bars_norm = normalize_runtime_bars(bars or [])
        zones_sorted = sorted([dict(row) for row in zones or []], key=_zone_start_ts)
        all_signals: list[dict[str, Any]] = []
        all_trades: list[dict[str, Any]] = []
        expiry_counters: dict[int, dict[str, int]] = {
            expiry: {"expired_count": 0, "invalidated_count": 0, "a3_triggered_count": 0}
            for expiry in expiry_values
        }
        max_expiry = max(expiry_values) if expiry_values else 0

        for zone in zones_sorted:
            start_ts = _zone_start_ts(zone)
            end_ts = start_ts + float(max_expiry) + float(self.config.outcome_window_sec)
            symbol = str(zone.get("symbol") or "")
            tick_iter = _zone_window_ticks(
                event_source=event_source,
                ticks=ticks,
                tick_ts=tick_ts,
                start_ts=start_ts,
                end_ts=end_ts,
                symbol=symbol,
            )
            outcomes = self._run_zone_for_expiries(zone, tick_iter, bars_norm, expiry_values, max_expiry)
            for expiry, outcome in outcomes.items():
                status = str(outcome.get("status") or "")
                if status == A2_EXPIRED:
                    expiry_counters[expiry]["expired_count"] += 1
                elif status == A2_INVALIDATED:
                    expiry_counters[expiry]["invalidated_count"] += 1
                elif status == A2_A3_TRIGGERED:
                    expiry_counters[expiry]["a3_triggered_count"] += 1
                    signal = outcome.get("signal")
                    trade = outcome.get("trade")
                    if isinstance(signal, dict):
                        all_signals.append(signal)
                    if isinstance(trade, dict):
                        all_trades.append(trade)

        runtime_status = RUNTIME_STATUS_OK if all_signals or all_trades else RUNTIME_STATUS_NO_RT_A3_SIGNALS
        return _reports(
            all_signals,
            all_trades,
            expiry_values,
            expiry_counters,
            runtime_status,
            self.config,
            _runtime_memory_profile(event_source, ticks),
        )

    def _run_zone_for_expiries(
        self,
        zone: Mapping[str, Any],
        ticks: Iterable[Mapping[str, Any]],
        bars: list[dict[str, float]],
        expiry_values: list[int],
        max_expiry: int,
    ) -> dict[int, dict[str, Any]]:
        start_ts = _zone_start_ts(zone)
        machine = A2RuntimeStateMachine(zone, expiry_sec=max_expiry, config=_a2_with_expiry(self.config.a2, max_expiry))
        max_seen_ts = 0.0
        invalidated_ts = 0.0
        trigger_a2: dict[str, Any] | None = None
        trigger_a3: dict[str, Any] | None = None
        pending_next_a2: Mapping[str, Any] | None = None

        for tick in ticks:
            ts = float(tick["ts"])
            max_seen_ts = max(max_seen_ts, ts)
            if pending_next_a2 is not None:
                a3 = evaluate_a3_runtime_entry(
                    pending_next_a2,
                    tick,
                    direction=str(zone.get("direction") or ""),
                    inherited_a1_vp_setup=_vp_setup(zone),
                    config=self.config.a3,
                )
                if parse_bool(a3.get("a3_entry_rt_flag")):
                    trigger_a2 = machine.mark_a3_triggered()
                    trigger_a3 = a3
                    break
                pending_next_a2 = None
            if ts - start_ts > float(max_expiry):
                machine.update(tick)
                break
            pre = machine.snapshot()
            if pre.get("a2_rt_state") == A2_READY_FOR_A3 or parse_bool(pre.get("a2_rt_ready_for_a3_flag")):
                if self.config.next_tick_entry:
                    pending_next_a2 = pre
                    continue
                a3 = evaluate_a3_runtime_entry(
                    pre,
                    tick,
                    direction=str(zone.get("direction") or ""),
                    inherited_a1_vp_setup=_vp_setup(zone),
                    config=self.config.a3,
                )
                if parse_bool(a3.get("a3_entry_rt_flag")):
                    trigger_a2 = machine.mark_a3_triggered()
                    trigger_a3 = a3
                    break
            snap = machine.update(tick)
            if snap.get("a2_rt_state") == A2_INVALIDATED:
                invalidated_ts = parse_float(snap.get("a2_rt_last_update_ts"))
                break
            if snap.get("a2_rt_state") == A2_EXPIRED:
                break

        outcomes: dict[int, dict[str, Any]] = {}
        trigger_ts = parse_float(trigger_a3.get("a3_entry_rt_ts")) if trigger_a3 else 0.0
        for expiry in expiry_values:
            if trigger_a2 is not None and trigger_a3 is not None and trigger_ts - start_ts <= float(expiry):
                a2 = _a2_snapshot_for_expiry(trigger_a2, expiry)
                a3 = dict(trigger_a3)
                signal = _signal_row(zone, a2, a3, expiry)
                trade = _trade_row(zone, a2, a3, expiry, bars, self.config)
                outcomes[expiry] = {"status": A2_A3_TRIGGERED, "signal": signal, "trade": trade}
            elif invalidated_ts > 0 and invalidated_ts - start_ts <= float(expiry):
                outcomes[expiry] = {"status": A2_INVALIDATED}
            elif max_seen_ts > 0 and max_seen_ts - start_ts > float(expiry):
                outcomes[expiry] = {"status": A2_EXPIRED}
            else:
                outcomes[expiry] = {"status": ""}
        return outcomes


def default_runtime_engine_config(**overrides: Any) -> RuntimeThreeAEngineConfig:
    a2 = A2RuntimeConfig(
        min_quiet_sec=float(overrides.get("a2_rt_min_quiet_sec", getattr(cfg, "A2_RT_MIN_QUIET_SEC", 3.0))),
        min_tick_count=int(overrides.get("a2_rt_min_tick_count", getattr(cfg, "A2_RT_MIN_TICK_COUNT", 20))),
        max_age_sec=float(overrides.get("a2_rt_max_age_sec", getattr(cfg, "A2_RT_MAX_AGE_SEC", 900.0))),
        max_box_width_u=float(overrides.get("a2_rt_max_box_width_u", getattr(cfg, "A2_RT_MAX_BOX_WIDTH_U", 3.0))),
        max_box_width_multiplier=float(overrides.get("a2_rt_max_box_width_multiplier", getattr(cfg, "A2_RT_MAX_BOX_WIDTH_MULTIPLIER", 2.0))),
        quiet_volume_ratio_max=float(overrides.get("a2_rt_quiet_volume_ratio_max", getattr(cfg, "A2_RT_QUIET_VOLUME_RATIO_MAX", 0.45))),
        cvd_stall_ratio_max=float(overrides.get("a2_rt_cvd_stall_ratio_max", getattr(cfg, "A2_RT_CVD_STALL_RATIO_MAX", 0.35))),
        invalidation_buffer_u=float(overrides.get("a2_rt_invalidation_buffer_u", getattr(cfg, "A2_RT_INVALIDATION_BUFFER_U", 0.5))),
    )
    a3 = A3RuntimeConfig(
        breakout_buffer_u=float(overrides.get("a3_rt_breakout_buffer_u", getattr(cfg, "A3_RT_BREAKOUT_BUFFER_U", 0.5))),
        active_flow_ratio=float(overrides.get("a3_rt_active_flow_ratio", getattr(cfg, "A3_RT_ACTIVE_FLOW_RATIO", 1.5))),
        min_delta_notional=float(overrides.get("a3_rt_min_delta_notional", getattr(cfg, "A3_RT_MIN_DELTA_NOTIONAL", 100000.0))),
        min_burst_notional=float(overrides.get("a3_rt_min_burst_notional", getattr(cfg, "A3_RT_MIN_BURST_NOTIONAL", 150000.0))),
        burst_multiplier=float(overrides.get("a3_rt_burst_multiplier", getattr(cfg, "A3_RT_BURST_MULTIPLIER", 2.0))),
        price_velocity_min_u_per_sec=float(overrides.get("a3_rt_price_velocity_min_u_per_sec", getattr(cfg, "A3_RT_PRICE_VELOCITY_MIN_U_PER_SEC", 0.2))),
    )
    expiry_secs = [int(x) for x in overrides.get("expiry_secs", getattr(cfg, "A2_RT_EXPIRY_SWEEP_SECS", [180, 300, 600, 900, 1200, 1800]))]
    max_age = int(overrides.get("a2_rt_max_age_sec", getattr(cfg, "A2_RT_MAX_AGE_SEC", 900.0)))
    if max_age not in expiry_secs:
        expiry_secs.append(max_age)
    return RuntimeThreeAEngineConfig(
        expiry_secs=expiry_secs,
        a2=a2,
        a3=a3,
        stop_model=str(overrides.get("stop_model", getattr(cfg, "V7_3A_RT_STOP_MODEL", "STOP_STRUCTURAL_ZONE_V2"))),
        target_model=str(overrides.get("target_model", getattr(cfg, "V7_3A_RT_TARGET_MODEL", "TARGET_FIXED_2R"))),
        next_tick_entry=bool(overrides.get("next_tick_entry", getattr(cfg, "V7_3A_RT_NEXT_TICK_ENTRY", False))),
        enable_audit=bool(overrides.get("enable_audit", getattr(cfg, "V7_3A_RT_ENABLE_NO_FUTURE_AUDIT", True))),
        max_fee_share_r=float(overrides.get("max_fee_share_r", getattr(cfg, "V7_3A_RT_MAX_FEE_SHARE_R", 1.0))),
        default_expiry_sec=int(overrides.get("default_expiry_sec", max_age)),
    )


def simulate_runtime_trade_exit(
    *,
    entry_ts: float,
    entry_price: float,
    stop_price: float,
    target_price: float,
    direction: str,
    future_bars: Iterable[Mapping[str, Any]],
    fee_share_r: float,
    risk_u: float,
    window_sec: int = 3600,
) -> dict[str, Any]:
    bars = [
        bar for bar in normalize_runtime_bars(future_bars)
        if float(bar["timestamp"]) >= float(entry_ts) and float(bar["timestamp"]) <= float(entry_ts) + float(window_sec)
    ]
    side = str(direction or "").upper()
    if risk_u <= 0 or entry_price <= 0 or side not in {"BUY", "SELL"}:
        return _exit_result(0.0, 0.0, "INVALID_RISK", 0.0, 0.0, complete_flag=False)
    if not bars:
        return _exit_result(entry_ts, entry_price, "NO_FUTURE_BARS", -fee_share_r, 0.0, complete_flag=False)
    if side == "BUY":
        mfe_r = (max(float(b["high"]) for b in bars) - entry_price) / risk_u
        mae_r = (min(float(b["low"]) for b in bars) - entry_price) / risk_u
    else:
        mfe_r = (entry_price - min(float(b["low"]) for b in bars)) / risk_u
        mae_r = (entry_price - max(float(b["high"]) for b in bars)) / risk_u
    target_r = abs(target_price - entry_price) / risk_u if target_price > 0 else 0.0
    for bar in bars:
        high = float(bar["high"])
        low = float(bar["low"])
        if side == "BUY":
            hit_stop = low <= stop_price
            hit_target = high >= target_price
        else:
            hit_stop = high >= stop_price
            hit_target = low <= target_price
        if hit_stop and hit_target:
            return _exit_result(bar["timestamp"], stop_price, "AMBIGUOUS_BOTH_HIT", -1.0 - fee_share_r, mfe_r, mae_r)
        if hit_target:
            return _exit_result(bar["timestamp"], target_price, "TARGET_FIRST", target_r - fee_share_r, mfe_r, mae_r)
        if hit_stop:
            return _exit_result(bar["timestamp"], stop_price, "STOP_FIRST", -1.0 - fee_share_r, mfe_r, mae_r)
    close = float(bars[-1]["close"])
    raw_r = (close - entry_price) / risk_u if side == "BUY" else (entry_price - close) / risk_u
    complete = float(bars[-1]["timestamp"]) >= float(entry_ts) + float(window_sec)
    return _exit_result(bars[-1]["timestamp"], close, "CLOSE_EXIT", raw_r - fee_share_r, mfe_r, mae_r, complete_flag=complete)


def normalize_runtime_ticks(events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    prev_price = 0.0
    prev_ts = 0.0
    for event in events or []:
        tick = normalize_runtime_tick(event, prev_price=prev_price, prev_ts=prev_ts)
        if tick is None:
            continue
        out.append(tick)
        prev_price = float(tick["last_price"])
        prev_ts = float(tick["ts"])
    out.sort(key=lambda row: float(row["ts"]))
    return out


def normalize_runtime_tick(event: Mapping[str, Any], *, prev_price: float = 0.0, prev_ts: float = 0.0) -> dict[str, Any] | None:
    ts = _first_positive(event, "ts", "timestamp", "event_ts", "recv_ts")
    if ts > 10_000_000_000:
        ts /= 1000.0
    price = _first_positive(event, "last_price", "price", "trade_price", "close", "px")
    if ts <= 0 or price <= 0:
        return None
    side = str(event.get("side") or event.get("direction") or "").upper()
    notional = _first_positive(event, "notional", "trade_notional", "sz_usdt", "amount_usdt")
    buy = _first_positive(event, "active_buy_notional_3s", "active_buy_notional_1s", "buy_notional")
    sell = _first_positive(event, "active_sell_notional_3s", "active_sell_notional_1s", "sell_notional")
    if buy <= 0 and side in {"BUY", "BID"}:
        buy = notional
    if sell <= 0 and side in {"SELL", "ASK"}:
        sell = notional
    velocity = parse_float(event.get("price_velocity_u_per_sec"))
    if velocity == 0 and prev_price > 0 and ts > prev_ts:
        velocity = (price - prev_price) / (ts - prev_ts)
    return {
        **dict(event),
        "ts": ts,
        "last_price": price,
        "active_buy_notional_3s": buy,
        "active_sell_notional_3s": sell,
        "cvd_delta_3s": parse_float(event.get("cvd_delta_3s") or event.get("cvd_delta_1s") or event.get("cvd_delta")),
        "price_velocity_u_per_sec": velocity,
        "condition_available_ts": _first_positive(event, "condition_available_ts", "field_available_ts") or ts,
        "condition_source": str(event.get("condition_source") or "tick_at_entry"),
    }


def normalize_runtime_bars(bars: Iterable[Mapping[str, Any]]) -> list[dict[str, float]]:
    out: list[dict[str, float]] = []
    for row in bars or []:
        ts = _first_positive(row, "timestamp", "ts", "event_ts")
        if ts > 10_000_000_000:
            ts /= 1000.0
        close = parse_float(row.get("close") or row.get("last_price") or row.get("price"))
        high = parse_float(row.get("high")) or close
        low = parse_float(row.get("low")) or close
        open_ = parse_float(row.get("open")) or close
        if ts <= 0 or close <= 0:
            continue
        out.append({"timestamp": ts, "open": open_, "high": high, "low": low, "close": close, "volume": parse_float(row.get("volume") or row.get("vol"))})
    out.sort(key=lambda row: float(row["timestamp"]))
    return out


def _a2_with_expiry(config: A2RuntimeConfig, expiry: int) -> A2RuntimeConfig:
    return A2RuntimeConfig(
        min_quiet_sec=config.min_quiet_sec,
        min_tick_count=config.min_tick_count,
        max_age_sec=float(expiry),
        max_box_width_u=config.max_box_width_u,
        max_box_width_multiplier=config.max_box_width_multiplier,
        quiet_volume_ratio_max=config.quiet_volume_ratio_max,
        cvd_stall_ratio_max=config.cvd_stall_ratio_max,
        invalidation_buffer_u=config.invalidation_buffer_u,
    )


def _prepare_runtime_events(
    trade_events: Iterable[Mapping[str, Any]] | None,
) -> tuple[Any | None, list[dict[str, Any]], list[float]]:
    if trade_events is None:
        return None, [], []
    if hasattr(trade_events, "get_window"):
        return trade_events, [], []
    ticks = normalize_runtime_ticks(trade_events)
    return None, ticks, [float(tick["ts"]) for tick in ticks]


def _zone_window_ticks(
    *,
    event_source: Any | None,
    ticks: list[dict[str, Any]],
    tick_ts: list[float],
    start_ts: float,
    end_ts: float,
    symbol: str,
) -> Iterable[dict[str, Any]]:
    if event_source is not None:
        return _iter_normalized_window(event_source.get_window(start_ts, end_ts, symbol))
    left = bisect_left(tick_ts, float(start_ts))
    right = bisect_right(tick_ts, float(end_ts))
    return (tick for tick in _iter_index_range_ticks(ticks, left, right) if _symbol_matches(symbol, tick))


def _iter_index_range_ticks(ticks: list[dict[str, Any]], left: int, right: int) -> Iterable[dict[str, Any]]:
    for idx in range(left, right):
        yield ticks[idx]


def _iter_normalized_window(events: Iterable[Mapping[str, Any]]) -> Iterable[dict[str, Any]]:
    prev_price = 0.0
    prev_ts = 0.0
    for event in events:
        tick = normalize_runtime_tick(event, prev_price=prev_price, prev_ts=prev_ts)
        if tick is None:
            continue
        prev_price = float(tick["last_price"])
        prev_ts = float(tick["ts"])
        yield tick


def _a2_snapshot_for_expiry(a2: Mapping[str, Any], expiry: int) -> dict[str, Any]:
    out = dict(a2)
    out["a2_rt_expiry_sec"] = expiry
    out["a2_rt_expiry_bucket"] = f"{int(round(float(expiry) / 60.0))}m"
    return out


def _runtime_memory_profile(event_source: Any | None, ticks: list[dict[str, Any]] | None) -> dict[str, Any]:
    if event_source is not None and hasattr(event_source, "memory_profile"):
        profile = dict(event_source.memory_profile())
        warning = runtime_performance_warning(profile)
        if warning:
            profile["runtime_performance_warning"] = warning
        return profile
    count = len(ticks or [])
    profile = {
        "runtime_event_source_mode": "materialized_iterable" if count else "empty",
        "runtime_ticks_materialized_count": count,
        "runtime_window_reads": 0,
        "runtime_max_window_ticks": 0,
        "runtime_candidate_file_scans": 0,
        "runtime_event_source_manifest_used": False,
    }
    warning = runtime_performance_warning(profile)
    if warning:
        profile["runtime_performance_warning"] = warning
    return profile


def _signal_row(zone: Mapping[str, Any], a2: Mapping[str, Any], a3: Mapping[str, Any], expiry_sec: int) -> dict[str, Any]:
    return {
        "zone_id": zone.get("zone_id", ""),
        "direction": a3.get("a3_entry_rt_direction") or zone.get("direction", ""),
        "a1_ts": _zone_start_ts(zone),
        "a1_price": zone.get("iceberg_context_price") or zone.get("zone_mid") or 0.0,
        "a1_vp_setup_rt": _vp_setup(zone),
        "a2_rt_ready_ts": a2.get("a2_rt_last_update_ts") or 0.0,
        "a2_rt_expiry_sec": expiry_sec,
        "entry_ts": a3.get("a3_entry_rt_ts") or 0.0,
        "entry_price": a3.get("a3_entry_rt_price") or 0.0,
        "entry_reason": a3.get("a3_entry_rt_reason") or "",
        "condition_available_ts_max": a3.get("a3_entry_rt_condition_available_ts_max") or a3.get("a3_entry_rt_ts") or 0.0,
        "uses_future_field_flag": parse_bool(a3.get("a3_entry_rt_uses_future_field_flag")),
        "future_field_names": "",
    }


def _trade_row(
    zone: Mapping[str, Any],
    a2: Mapping[str, Any],
    a3: Mapping[str, Any],
    expiry_sec: int,
    bars: list[dict[str, float]],
    config: RuntimeThreeAEngineConfig,
) -> dict[str, Any]:
    row = {**dict(zone), **dict(a2), **dict(a3)}
    direction = str(a3.get("a3_entry_rt_direction") or zone.get("direction") or "").upper()
    entry = parse_float(a3.get("a3_entry_rt_price"))
    stop = build_stop(row, entry, direction, config.stop_model)
    risk = parse_float(stop.get("risk_u"))
    targets = build_target_candidates(row, entry, direction, risk)
    target_price = targets["target_fixed_2r_price_sim"] if config.target_model.upper() == "TARGET_FIXED_2R" else targets.get("target_hybrid_min_2r_price_rt", 0.0)
    target_r = abs(parse_float(target_price) - entry) / risk if risk > 0 else 0.0
    exit_result = simulate_runtime_trade_exit(
        entry_ts=parse_float(a3.get("a3_entry_rt_ts")),
        entry_price=entry,
        stop_price=parse_float(stop.get("stop_price")),
        target_price=parse_float(target_price),
        direction=direction,
        future_bars=bars,
        fee_share_r=parse_float(stop.get("fee_share_r")),
        risk_u=risk,
        window_sec=config.outcome_window_sec,
    )
    trade = {
        "trade_id": f"{zone.get('zone_id', '')}:{expiry_sec}:{int(parse_float(a3.get('a3_entry_rt_ts')))}",
        "zone_id": zone.get("zone_id", ""),
        "direction": direction,
        "a1_ts": _zone_start_ts(zone),
        "a1_price": zone.get("iceberg_context_price") or zone.get("zone_mid") or 0.0,
        "a1_vp_setup_rt": _vp_setup(zone),
        "a1_vp_context_prefix": _vp_prefix(zone),
        "a2_rt_start_ts": a2.get("a2_rt_start_ts") or 0.0,
        "a2_rt_ready_ts": a2.get("a2_rt_last_update_ts") or 0.0,
        "a2_rt_expiry_sec": expiry_sec,
        "a2_rt_state": a2.get("a2_rt_state") or "",
        "entry_ts": a3.get("a3_entry_rt_ts") or 0.0,
        "entry_price": entry,
        "entry_reason": a3.get("a3_entry_rt_reason") or "",
        "condition_available_ts_max": a3.get("a3_entry_rt_condition_available_ts_max") or a3.get("a3_entry_rt_ts") or 0.0,
        "condition_source": a3.get("a3_entry_rt_condition_source") or "tick_at_entry",
        "uses_future_field_flag": parse_bool(a3.get("a3_entry_rt_uses_future_field_flag")),
        "future_field_names": "",
        "trade_blocked_flag": False,
        "trade_blocked_reason": "",
        "entry_condition_fields": a3.get("a3_entry_rt_condition_fields") or "|".join(ENTRY_CONDITION_FIELDS),
        **stop,
        "target_model": config.target_model,
        "target_price": round(parse_float(target_price), 8),
        "target_r": round(target_r, 8),
        **targets,
        **exit_result,
        "a3_quality_future_type_v2": zone.get("a3_quality_future_type_v2") or "NO_AGGRESSION",
        "a3_quality_future_score_v2": zone.get("a3_quality_future_score_v2") or 0.0,
    }
    if risk <= 0 or parse_float(stop.get("fee_share_r")) > config.max_fee_share_r:
        trade["trade_blocked_flag"] = True
        trade["trade_blocked_reason"] = "INVALID_RISK_OR_FEE"
    if config.enable_audit:
        trade = validate_trade_row(trade)
    return trade


def _exit_result(
    exit_ts: float,
    exit_price: float,
    exit_reason: str,
    realized_r: float,
    mfe_r: float,
    mae_r: float | None = None,
    *,
    complete_flag: bool = True,
) -> dict[str, Any]:
    return {
        "exit_ts": round(exit_ts, 8),
        "exit_price": round(exit_price, 8),
        "exit_reason": exit_reason,
        "realized_r_sim": round(realized_r, 8),
        "mfe_r_future": round(mfe_r, 8),
        "mae_r_future": round(mae_r if mae_r is not None else 0.0, 8),
        "target_first_flag_sim": exit_reason == "TARGET_FIRST",
        "stop_first_flag_sim": exit_reason == "STOP_FIRST",
        "ambiguous_flag_sim": exit_reason == "AMBIGUOUS_BOTH_HIT",
        "complete_flag_sim": bool(complete_flag),
    }


def _reports(
    signals: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    expiry_values: list[int],
    expiry_counters: dict[int, dict[str, int]],
    runtime_status: str,
    config: RuntimeThreeAEngineConfig,
    memory_profile: dict[str, Any] | None = None,
) -> dict[str, list[dict[str, Any]] | dict[str, Any]]:
    stats_trades = [
        trade for trade in trades
        if not parse_bool(trade.get("uses_future_field_flag")) and not parse_bool(trade.get("trade_blocked_flag"))
    ]
    default_expiry = int(config.default_expiry_sec)
    default_expiry_trades = [
        trade for trade in stats_trades
        if int(parse_float(trade.get("a2_rt_expiry_sec"))) == default_expiry
    ]
    unique_signal_keys = {
        (str(signal.get("zone_id") or ""), float(parse_float(signal.get("entry_ts"))))
        for signal in signals
    }
    return {
        "signals": signals,
        "trades": trades,
        "by_strategy": [_summary_row(name, [t for t in default_expiry_trades if fn(t)], "strategy_variant") for name, fn in VARIANTS.items()],
        "by_strategy_all_expiry_variants": [_summary_row(name, [t for t in stats_trades if fn(t)], "strategy_variant") for name, fn in VARIANTS.items()],
        "by_strategy_default_expiry": [_summary_row(name, [t for t in default_expiry_trades if fn(t)], "strategy_variant") for name, fn in VARIANTS.items()],
        "by_vp_setup": _group_summary(stats_trades, "a1_vp_setup_rt"),
        "by_expiry": _expiry_summary(stats_trades, expiry_values, expiry_counters),
        "by_target_candidate": _group_summary(stats_trades, "target_model"),
        "summary": {
            "runtime_3a_strategy_version": "v7.3.0",
            "runtime_3a_status": runtime_status,
            "signal_count": len(signals),
            "trade_count": len(stats_trades),
            "trade_count_all_expiry_variants": len(stats_trades),
            "default_expiry_sec": default_expiry,
            "default_expiry_trade_count": len(default_expiry_trades),
            "unique_signal_count": len(unique_signal_keys),
            "trade_candidate_count": len(trades),
            "trade_blocked_count": sum(1 for trade in trades if parse_bool(trade.get("trade_blocked_flag"))),
            "expiry_secs": expiry_values,
            "stop_model": config.stop_model,
            "target_model": config.target_model,
            "runtime_3a_memory_profile": dict(memory_profile or {}),
        },
    }


def _empty_reports(expiry_values: list[int], status: str, config: RuntimeThreeAEngineConfig) -> dict[str, list[dict[str, Any]] | dict[str, Any]]:
    counters = {expiry: {"expired_count": 0, "invalidated_count": 0, "a3_triggered_count": 0} for expiry in expiry_values}
    return _reports([], [], expiry_values, counters, status, config, _runtime_memory_profile(None, []))


def _summary_row(name: str, trades: list[Mapping[str, Any]], label: str | None = None) -> dict[str, Any]:
    values = [parse_float(t.get("realized_r_sim")) for t in trades]
    wins = [v for v in values if v > 0]
    losses = [abs(v) for v in values if v < 0]
    return {
        (label or "strategy_variant"): name,
        "trade_count": len(trades),
        "avg_realized_r_sim": round(sum(values) / len(values), 8) if values else 0.0,
        "median_realized_r_sim": round(statistics.median(values), 8) if values else 0.0,
        "win_rate": round(len(wins) / len(values), 8) if values else 0.0,
        "profit_factor": round(sum(wins) / sum(losses), 8) if losses else (round(sum(wins), 8) if wins else 0.0),
        "max_drawdown_r": _max_drawdown(values),
        "max_consecutive_losses": _max_consecutive_losses(values),
        "trades_per_day": _trades_per_day(trades),
        "fee_share_r_avg": round(sum(parse_float(t.get("fee_share_r")) for t in trades) / len(trades), 8) if trades else 0.0,
        "long_short_split": dict(Counter(str(t.get("direction") or "") for t in trades)),
        "a1_vp_setup_split": dict(Counter(str(t.get("a1_vp_setup_rt") or "") for t in trades)),
        "a2_expiry_split": dict(Counter(str(t.get("a2_rt_expiry_sec") or "") for t in trades)),
        "target_candidate_split": dict(Counter(str(t.get("target_model") or "") for t in trades)),
    }


def _group_summary(trades: list[Mapping[str, Any]], field: str) -> list[dict[str, Any]]:
    groups: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for trade in trades:
        groups[str(trade.get(field) or "UNKNOWN")].append(trade)
    return [_summary_row(key, value, field) for key, value in sorted(groups.items())]


def _expiry_summary(trades: list[Mapping[str, Any]], expiry_values: list[int], counters: dict[int, dict[str, int]]) -> list[dict[str, Any]]:
    rows = []
    for expiry in sorted(set(expiry_values)):
        group = [t for t in trades if int(parse_float(t.get("a2_rt_expiry_sec"))) == int(expiry)]
        row = _summary_row(str(expiry), group, "expiry_sec")
        row.update(counters.get(expiry, {"expired_count": 0, "invalidated_count": 0, "a3_triggered_count": len(group)}))
        rows.append(row)
    return rows


def _max_drawdown(values: list[float]) -> float:
    peak = 0.0
    equity = 0.0
    drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = min(drawdown, equity - peak)
    return round(drawdown, 8)


def _max_consecutive_losses(values: list[float]) -> int:
    best = current = 0
    for value in values:
        current = current + 1 if value < 0 else 0
        best = max(best, current)
    return best


def _trades_per_day(trades: list[Mapping[str, Any]]) -> float:
    times = [parse_float(t.get("entry_ts")) for t in trades if parse_float(t.get("entry_ts")) > 0]
    if not times:
        return 0.0
    days = max((max(times) - min(times)) / 86400.0, 1.0)
    return round(len(times) / days, 8)


def _advantage_setups() -> set[str]:
    return {
        "BUY_BELOW_VAL_ABSORB",
        "BUY_NEAR_VAL_ABSORB",
        "BUY_LVN_BELOW_HVN_ABSORB",
        "BUY_INSIDE_VALUE_BELOW_POC_ABSORB",
        "SELL_ABOVE_VAH_ABSORB",
        "SELL_NEAR_VAH_ABSORB",
        "SELL_LVN_ABOVE_HVN_ABSORB",
        "SELL_INSIDE_VALUE_ABOVE_POC_ABSORB",
    }


def _vp_setup(row: Mapping[str, Any]) -> str:
    for name in ("a1_vp_setup_rt", "vp24h_a1_vp_setup_rt", "vpsession_a1_vp_setup_rt", "vp4h_a1_vp_setup_rt", "vp1h_a1_vp_setup_rt"):
        value = str(row.get(name) or "").strip()
        if value:
            return value
    return "VP_UNAVAILABLE"


def _vp_prefix(row: Mapping[str, Any]) -> str:
    for prefix in ("vp24h", "vpsession", "vp4h", "vp1h"):
        if row.get(f"{prefix}_a1_vp_setup_rt"):
            return prefix
    return ""


def _zone_start_ts(zone: Mapping[str, Any]) -> float:
    return _first_positive(zone, "reaction_event_ts", "first_iceberg_pie_ts", "best_pie_ts", "first_seen_ts", "frozen_ts")


def _symbol_matches(symbol: str, tick: Mapping[str, Any]) -> bool:
    tick_symbol = str(tick.get("symbol") or tick.get("instId") or "")
    return not symbol or not tick_symbol or symbol == tick_symbol


def _first_positive(row: Mapping[str, Any], *names: str) -> float:
    for name in names:
        value = parse_float(row.get(name))
        if value > 0:
            return value
    return 0.0
