#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import statistics
from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping

from config import research_evaluator as cfg
from src.research.a1_edge.schema import parse_bool, parse_float
from src.research.no_future_audit import validate_entry_conditions, validate_trade_row

from .stop_models import build_stop
from .target_models import build_target_candidates


RT_ENTRY_FIELDS = [
    "a2_rt_ready_for_a3_flag",
    "a2_rt_state",
    "a2_rt_box_high",
    "a2_rt_box_low",
    "a3_entry_rt_flag",
    "a3_entry_rt_ts",
    "a3_entry_rt_price",
    "a3_entry_rt_direction",
]

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


def build_runtime_strategy_reports(
    rows: Iterable[Mapping[str, Any]],
    bars: Iterable[Mapping[str, Any]] | None = None,
    *,
    expiry_secs: Iterable[int] | None = None,
    stop_model: str | None = None,
    target_model: str | None = None,
    enable_audit: bool | None = None,
) -> dict[str, list[dict[str, Any]] | dict[str, Any]]:
    validate_entry_conditions(RT_ENTRY_FIELDS)
    expiry_values = [int(x) for x in (expiry_secs or getattr(cfg, "A2_RT_EXPIRY_SWEEP_SECS", [180, 300, 600, 900, 1200, 1800]))]
    stop_name = str(stop_model or getattr(cfg, "V7_3A_RT_STOP_MODEL", "STOP_STRUCTURAL_ZONE_V2"))
    target_name = str(target_model or getattr(cfg, "V7_3A_RT_TARGET_MODEL", "TARGET_FIXED_2R"))
    audit = bool(getattr(cfg, "V7_3A_RT_ENABLE_NO_FUTURE_AUDIT", True)) if enable_audit is None else bool(enable_audit)
    signals: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    for row in rows or []:
        if not parse_bool(row.get("a3_entry_rt_flag")):
            continue
        if str(row.get("a2_rt_state") or "").upper() != "A2_READY_FOR_A3" and not parse_bool(row.get("a2_rt_ready_for_a3_flag")):
            continue
        expiry = int(parse_float(row.get("a2_rt_expiry_sec")) or getattr(cfg, "A2_RT_MAX_AGE_SEC", 900))
        if expiry not in expiry_values:
            expiry_values.append(expiry)
        signal = _signal_row(row, expiry)
        signals.append(signal)
        trade = _trade_row(row, expiry, stop_name, target_name)
        if audit:
            trade = validate_trade_row(trade)
        if not parse_bool(trade.get("uses_future_field_flag")):
            trades.append(trade)
    return {
        "signals": signals,
        "trades": trades,
        "by_strategy": [_summary_row(name, [t for t in trades if fn(t)], "strategy_variant") for name, fn in VARIANTS.items()],
        "by_vp_setup": _group_summary(trades, "a1_vp_setup_rt"),
        "by_expiry": _expiry_summary(trades, expiry_values),
        "by_target_candidate": _group_summary(trades, "target_model"),
        "summary": {
            "runtime_3a_strategy_version": "v7.3.0",
            "signal_count": len(signals),
            "trade_count": len(trades),
            "expiry_secs": expiry_values,
            "stop_model": stop_name,
            "target_model": target_name,
        },
    }


def _signal_row(row: Mapping[str, Any], expiry_sec: int) -> dict[str, Any]:
    return {
        "zone_id": row.get("zone_id", ""),
        "direction": row.get("a3_entry_rt_direction") or row.get("direction", ""),
        "a1_ts": row.get("first_iceberg_pie_ts") or row.get("best_pie_ts") or row.get("first_seen_ts") or 0.0,
        "a1_price": row.get("iceberg_context_price") or row.get("zone_mid") or 0.0,
        "a1_vp_setup_rt": _vp_setup(row),
        "a2_rt_ready_ts": row.get("a2_rt_last_update_ts") or 0.0,
        "a2_rt_expiry_sec": expiry_sec,
        "entry_ts": row.get("a3_entry_rt_ts") or 0.0,
        "entry_price": row.get("a3_entry_rt_price") or 0.0,
        "entry_reason": row.get("a3_entry_rt_reason") or "",
        "condition_available_ts_max": row.get("a3_entry_rt_ts") or 0.0,
        "uses_future_field_flag": False,
        "future_field_names": "",
    }


def _trade_row(row: Mapping[str, Any], expiry_sec: int, stop_model: str, target_model: str) -> dict[str, Any]:
    direction = str(row.get("a3_entry_rt_direction") or row.get("direction") or "").upper()
    entry = parse_float(row.get("a3_entry_rt_price"))
    stop = build_stop(row, entry, direction, stop_model)
    targets = build_target_candidates(row, entry, direction, parse_float(stop.get("risk_u")))
    target = targets["target_fixed_2r_price_sim"] if str(target_model).upper() == "TARGET_FIXED_2R" else targets.get("target_hybrid_min_2r_price_rt", 0.0)
    realized = _future_realized_proxy(row)
    trade = {
        "trade_id": f"{row.get('zone_id', '')}:{expiry_sec}:{int(parse_float(row.get('a3_entry_rt_ts')))}",
        "zone_id": row.get("zone_id", ""),
        "direction": direction,
        "a1_ts": row.get("first_iceberg_pie_ts") or row.get("best_pie_ts") or row.get("first_seen_ts") or 0.0,
        "a1_price": row.get("iceberg_context_price") or row.get("zone_mid") or 0.0,
        "a1_vp_setup_rt": _vp_setup(row),
        "a1_vp_context_prefix": _vp_prefix(row),
        "a2_rt_start_ts": row.get("a2_rt_start_ts") or 0.0,
        "a2_rt_ready_ts": row.get("a2_rt_last_update_ts") or 0.0,
        "a2_rt_expiry_sec": expiry_sec,
        "a2_rt_state": row.get("a2_rt_state") or "",
        "entry_ts": row.get("a3_entry_rt_ts") or 0.0,
        "entry_price": entry,
        "entry_reason": row.get("a3_entry_rt_reason") or "",
        "condition_available_ts_max": row.get("a3_entry_rt_ts") or 0.0,
        "uses_future_field_flag": False,
        "future_field_names": "",
        "entry_condition_fields": "|".join(RT_ENTRY_FIELDS),
        **stop,
        "target_model": target_model,
        "target_price": target,
        "target_r": 2.0,
        **targets,
        "exit_ts": 0.0,
        "exit_price": 0.0,
        "exit_reason": "OUTCOME_PROXY",
        "realized_r_sim": realized,
        "mfe_r_future": parse_float(row.get("a3_future_net_mfe_1h_r") or row.get("a3_structural_net_mfe_1h_r_future")),
        "mae_r_future": parse_float(row.get("a3_future_net_mae_1h_r") or row.get("a3_structural_net_mae_1h_r_future")),
        "a3_quality_future_type_v2": row.get("a3_quality_future_type_v2") or "NO_AGGRESSION",
        "a3_quality_future_score_v2": row.get("a3_quality_future_score_v2") or 0.0,
    }
    if parse_float(stop.get("risk_u")) <= 0 or parse_float(stop.get("fee_share_r")) > parse_float(getattr(cfg, "V7_3A_RT_MAX_FEE_SHARE_R", 1.0)):
        trade["uses_future_field_flag"] = True
        trade["future_field_names"] = "BLOCKED_INVALID_RISK_OR_FEE"
    return trade


def _future_realized_proxy(row: Mapping[str, Any]) -> float:
    return parse_float(row.get("a3_future_realized_r_proxy_1h") or row.get("a3_structural_realized_r_proxy_1h_future"))


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
        "trades_per_day": 0.0,
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


def _expiry_summary(trades: list[Mapping[str, Any]], expiry_values: list[int]) -> list[dict[str, Any]]:
    rows = []
    for expiry in sorted(set(expiry_values)):
        group = [t for t in trades if int(parse_float(t.get("a2_rt_expiry_sec"))) == int(expiry)]
        row = _summary_row(str(expiry), group, "expiry_sec")
        row.update({"expired_count": 0, "invalidated_count": 0, "a3_triggered_count": len(group)})
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
