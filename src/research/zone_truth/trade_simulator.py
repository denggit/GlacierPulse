#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from bisect import bisect_left, bisect_right
from typing import Any, Iterable, Mapping

from config import research_evaluator as cfg
from src.research.a1_edge.schema import parse_bool, parse_float


ENTRY_MODELS = (
    "BREAKOUT",
    "RECLAIM_CLOSE",
    "RETEST_HOLD",
    "AGGRESSION_FLIP",
    "NO_QUICK_RETURN_CONFIRM",
)
STOP_MODELS = ("V1_ZONE_WIDTH", "STRUCTURAL_PROXY", "ZONE_BOUNDARY_V2")


def simulate_3a_proxy_trades(
    rows: Iterable[Mapping[str, Any]],
    bars: Iterable[Mapping[str, Any]],
    *,
    entry_models: Iterable[str] = ENTRY_MODELS,
    stop_models: Iterable[str] = STOP_MODELS,
    target_r_list: Iterable[float] | None = None,
    window_sec: int = 3600,
    fee_pct: float | None = None,
) -> list[dict[str, Any]]:
    normalized_bars = _normalize_bars(bars)
    targets = [float(x) for x in (target_r_list or getattr(cfg, "V7_3A_TARGET_R_LIST", [1.0, 1.5, 2.0])) if float(x) >= 1.0]
    fee = float(fee_pct if fee_pct is not None else getattr(cfg, "V7_3A_ROUNDTRIP_FEE_PCT", 0.001))
    trades: list[dict[str, Any]] = []
    for row in rows or []:
        for entry_model in entry_models:
            entry = resolve_entry(row, normalized_bars, entry_model)
            if not entry["available"]:
                trades.append(_unavailable(row, entry_model, "NO_ENTRY", entry.get("reason", "NO_ENTRY")))
                continue
            for stop_model in stop_models:
                stop = resolve_stop(row, entry, stop_model)
                if not stop["available"]:
                    trades.append(
                        _unavailable(
                            row,
                            entry_model,
                            stop_model,
                            stop.get("reason", "INVALID_STOP"),
                            stop.get("stop_basis_reason", ""),
                        )
                    )
                    continue
                for target_r in targets:
                    trades.append(
                        simulate_single_trade(
                            row,
                            normalized_bars,
                            entry_model=entry_model,
                            stop_model=stop_model,
                            target_r=target_r,
                            entry_ts=entry["entry_ts"],
                            entry_price=entry["entry_price"],
                            entry_bar_ts=entry.get("entry_bar_ts", 0.0),
                            entry_price_source=entry.get("entry_price_source", ""),
                            stop_price=stop["stop_price"],
                            risk_u=stop["risk_u"],
                            stop_basis_reason=stop.get("stop_basis_reason", ""),
                            window_sec=window_sec,
                            fee_pct=fee,
                        )
                    )
    return trades


def resolve_entry(row: Mapping[str, Any], bars: list[dict[str, float]], entry_model: str) -> dict[str, Any]:
    model = str(entry_model).upper()
    if model == "BREAKOUT":
        ts = parse_float(row.get("a3_preview_entry_ts"))
        price = parse_float(row.get("a3_preview_entry_price"))
        if parse_bool(row.get("a3_preview_breakout_raw_flag")) and ts > 0 and price > 0:
            return {
                "available": True,
                "entry_ts": ts,
                "entry_price": price,
                "entry_bar_ts": 0.0,
                "entry_price_source": "A3_PREVIEW_ENTRY",
            }
        return {"available": False, "reason": "NO_BREAKOUT_ENTRY"}
    if model == "RECLAIM_CLOSE":
        ts = _first_ts(row, "sweep_reclaimed_ts", "a1_reaction_confirmed_ts", "reaction_event_ts")
        return _entry_from_bar_close(bars, ts, "NO_RECLAIM_TS")
    if model == "RETEST_HOLD":
        if not parse_bool(row.get("has_retested_inside_zone")) and not parse_bool(row.get("a2_retest_flag")):
            return {"available": False, "reason": "NO_RETEST_HOLD"}
        ts = _first_ts(row, "retest_confirmed_ts", "retest_started_ts", "reaction_event_ts")
        return _entry_from_bar_close(bars, ts, "NO_RETEST_TS")
    if model == "AGGRESSION_FLIP":
        if not parse_bool(row.get("a3_orderflow_aggression_flag")):
            return {"available": False, "reason": "NO_ORDERFLOW_AGGRESSION"}
        ts = _first_ts(row, "a3_aggression_ts", "confirmed_ts", "a3_preview_entry_ts", "reaction_event_ts")
        return _entry_from_bar_close(bars, ts, "NO_AGGRESSION_TS")
    if model == "NO_QUICK_RETURN_CONFIRM":
        if not (
            parse_bool(row.get("a3_no_quick_return_flag"))
            or parse_bool(row.get("a3_preview_no_quick_return_3m_flag"))
            or parse_bool(row.get("a3_preview_no_quick_return_5m_flag"))
        ):
            return {"available": False, "reason": "NO_QUICK_RETURN_NOT_CONFIRMED"}
        base = parse_float(row.get("a3_preview_entry_ts"))
        offset = 300.0 if parse_bool(row.get("a3_preview_no_quick_return_5m_flag")) else 180.0
        return _entry_from_bar_close(bars, base + offset if base > 0 else 0.0, "NO_QUICK_RETURN_TS")
    return {"available": False, "reason": "UNKNOWN_ENTRY_MODEL"}


def resolve_stop(row: Mapping[str, Any], entry: Mapping[str, Any], stop_model: str) -> dict[str, Any]:
    model = str(stop_model).upper()
    direction = str(row.get("direction") or "").upper()
    entry_price = parse_float(entry.get("entry_price"))
    if direction not in {"BUY", "SELL"} or entry_price <= 0:
        return {"available": False, "reason": "INVALID_DIRECTION_OR_ENTRY"}
    if model == "V1_ZONE_WIDTH":
        risk = max(
            parse_float(row.get("zone_width")),
            abs(parse_float(row.get("zone_upper")) - parse_float(row.get("zone_lower"))),
            1.0,
        )
        stop = entry_price - risk if direction == "BUY" else entry_price + risk
        return {"available": True, "stop_price": stop, "risk_u": risk, "stop_basis_reason": "V1_ZONE_WIDTH"}
    if model == "STRUCTURAL_PROXY":
        stop = _structural_proxy_stop(row, direction)
        return _validate_stop(entry_price, stop, direction, "STRUCTURAL_PROXY_UNAVAILABLE", "STRUCTURAL_PROXY")
    if model == "ZONE_BOUNDARY_V2":
        for field in (
            "first_event_zone_v2_structural_stop_price",
            "first_iceberg_event_zone_v2_structural_stop_price",
        ):
            stop = parse_float(row.get(field))
            if stop <= 0:
                continue
            event_stop = _validate_stop(
                entry_price,
                stop,
                direction,
                "ZONE_BOUNDARY_V2_EVENT_LEVEL_INVALID",
                "ZONE_BOUNDARY_V2_EVENT_LEVEL",
            )
            if event_stop["available"]:
                return event_stop
        stop = _structural_proxy_stop(row, direction)
        return _validate_stop(
            entry_price,
            stop,
            direction,
            "ZONE_BOUNDARY_V2_FALLBACK_STRUCTURAL_PROXY_UNAVAILABLE",
            "ZONE_BOUNDARY_V2_FALLBACK_STRUCTURAL_PROXY_NO_FUTURE_BASIS",
        )
    return {"available": False, "reason": "UNKNOWN_STOP_MODEL"}


def simulate_single_trade(
    row: Mapping[str, Any],
    bars: list[dict[str, float]],
    *,
    entry_model: str,
    stop_model: str,
    target_r: float,
    entry_ts: float,
    entry_price: float,
    stop_price: float,
    risk_u: float,
    entry_bar_ts: float = 0.0,
    entry_price_source: str = "",
    stop_basis_reason: str = "",
    window_sec: int = 3600,
    fee_pct: float = 0.001,
) -> dict[str, Any]:
    direction = str(row.get("direction") or "").upper()
    target_r = max(float(target_r), 1.0)
    fee_u = entry_price * fee_pct
    fee_share = fee_u / risk_u if risk_u > 0 else 0.0
    if direction == "BUY":
        target_price = entry_price + target_r * risk_u
    else:
        target_price = entry_price - target_r * risk_u
    future = _future_bars(bars, entry_ts, window_sec, entry_bar_ts=entry_bar_ts, entry_price_source=entry_price_source)
    realized, outcome, flags, mfe_r, mae_r, complete = _first_hit(direction, future, entry_price, stop_price, target_price, risk_u, target_r, fee_share, window_sec)
    return {
        **_row_identity(row),
        "entry_model": entry_model,
        "stop_model": stop_model,
        "target_r": round(target_r, 8),
        "entry_ts": round(entry_ts, 8),
        "entry_bar_ts": round(entry_bar_ts, 8),
        "entry_price_source": entry_price_source,
        "entry_price": round(entry_price, 8),
        "stop_price": round(stop_price, 8),
        "stop_basis_reason": stop_basis_reason or str(stop_model),
        "target_price": round(target_price, 8),
        "risk_u": round(risk_u, 8),
        "fee_share_r": round(fee_share, 8),
        "realized_r_1h": round(realized, 8),
        "realized_outcome_1h": outcome,
        "target_first_flag": flags["target"],
        "stop_first_flag": flags["stop"],
        "ambiguous_flag": flags["ambiguous"],
        "complete_flag": complete,
        "mfe_r_1h": round(mfe_r - fee_share, 8),
        "mae_r_1h": round(mae_r - fee_share, 8),
    }


def _first_hit(
    direction: str,
    bars: list[dict[str, float]],
    entry: float,
    stop: float,
    target: float,
    risk: float,
    target_r: float,
    fee_share: float,
    window_sec: int,
) -> tuple[float, str, dict[str, bool], float, float, bool]:
    if risk <= 0 or direction not in {"BUY", "SELL"}:
        return 0.0, "INVALID_STOP", {"target": False, "stop": False, "ambiguous": False}, 0.0, 0.0, False
    if not bars:
        return -fee_share, "CLOSE_EXIT", {"target": False, "stop": False, "ambiguous": False}, 0.0, 0.0, False
    complete = (float(bars[-1]["timestamp"]) - float(bars[0]["timestamp"])) >= max(0, window_sec - 60)
    if direction == "BUY":
        mfe_r = (max(float(b["high"]) for b in bars) - entry) / risk
        mae_r = (min(float(b["low"]) for b in bars) - entry) / risk
    else:
        mfe_r = (entry - min(float(b["low"]) for b in bars)) / risk
        mae_r = (entry - max(float(b["high"]) for b in bars)) / risk
    for bar in bars:
        high = float(bar["high"])
        low = float(bar["low"])
        if direction == "BUY":
            hit_stop = low <= stop
            hit_target = high >= target
        else:
            hit_stop = high >= stop
            hit_target = low <= target
        if hit_stop and hit_target:
            return -1.0 - fee_share, "AMBIGUOUS_BOTH_HIT", {"target": False, "stop": True, "ambiguous": True}, mfe_r, mae_r, complete
        if hit_target:
            return target_r - fee_share, f"TARGET_{target_r:g}R_FIRST", {"target": True, "stop": False, "ambiguous": False}, mfe_r, mae_r, complete
        if hit_stop:
            return -1.0 - fee_share, "STOP_FIRST", {"target": False, "stop": True, "ambiguous": False}, mfe_r, mae_r, complete
    close = float(bars[-1]["close"])
    raw_r = (close - entry) / risk if direction == "BUY" else (entry - close) / risk
    return raw_r - fee_share, "CLOSE_EXIT", {"target": False, "stop": False, "ambiguous": False}, mfe_r, mae_r, complete


def _normalize_bars(bars: Iterable[Mapping[str, Any]]) -> list[dict[str, float]]:
    result = []
    for row in bars or []:
        ts = parse_float(row.get("timestamp"), parse_float(row.get("ts")))
        if ts > 10_000_000_000:
            ts /= 1000.0
        if ts <= 0:
            continue
        result.append(
            {
                "timestamp": ts,
                "open": parse_float(row.get("open")),
                "high": parse_float(row.get("high")),
                "low": parse_float(row.get("low")),
                "close": parse_float(row.get("close")),
                "volume": parse_float(row.get("volume"), parse_float(row.get("vol"))),
            }
        )
    result.sort(key=lambda bar: bar["timestamp"])
    return result


def _entry_from_bar_close(bars: list[dict[str, float]], ts: float, reason: str) -> dict[str, Any]:
    if ts <= 0:
        return {"available": False, "reason": reason}
    bar = _bar_at_or_after(bars, ts)
    if not bar:
        return {"available": False, "reason": "ENTRY_BAR_UNAVAILABLE"}
    return {
        "available": True,
        "entry_ts": float(bar["timestamp"]),
        "entry_price": float(bar["close"]),
        "entry_bar_ts": float(bar["timestamp"]),
        "entry_price_source": "BAR_CLOSE",
    }


def _bar_at_or_after(bars: list[dict[str, float]], ts: float) -> dict[str, float] | None:
    timestamps = [float(b["timestamp"]) for b in bars]
    idx = bisect_left(timestamps, ts)
    return bars[idx] if idx < len(bars) else None


def _future_bars(
    bars: list[dict[str, float]],
    entry_ts: float,
    window_sec: int,
    entry_bar_ts: float = 0.0,
    entry_price_source: str = "",
) -> list[dict[str, float]]:
    timestamps = [float(b["timestamp"]) for b in bars]
    close_based_entry = str(entry_price_source or "").upper() == "BAR_CLOSE" and entry_bar_ts > 0
    anchor_ts = entry_bar_ts if close_based_entry else entry_ts
    start = bisect_right(timestamps, entry_bar_ts) if close_based_entry else bisect_left(timestamps, entry_ts)
    end = bisect_right(timestamps, anchor_ts + float(window_sec))
    return bars[start:end]


def _first_ts(row: Mapping[str, Any], *names: str) -> float:
    for name in names:
        value = parse_float(row.get(name))
        if value > 0:
            return value
    return 0.0


def _structural_proxy_stop(row: Mapping[str, Any], direction: str) -> float:
    buffer_u = 0.5
    if direction == "BUY":
        low = parse_float(row.get("first_iceberg_pie_min_trade_price")) or parse_float(row.get("first_pie_min_trade_price"))
        return low - buffer_u if low > 0 else 0.0
    high = parse_float(row.get("first_iceberg_pie_max_trade_price")) or parse_float(row.get("first_pie_max_trade_price"))
    return high + buffer_u if high > 0 else 0.0


def _validate_stop(entry: float, stop: float, direction: str, reason: str, stop_basis_reason: str = "") -> dict[str, Any]:
    if stop <= 0:
        return {"available": False, "reason": reason, "stop_basis_reason": stop_basis_reason}
    risk = entry - stop if direction == "BUY" else stop - entry
    if risk <= 0:
        return {"available": False, "reason": "INVALID_STOP", "stop_basis_reason": stop_basis_reason}
    return {"available": True, "stop_price": stop, "risk_u": risk, "stop_basis_reason": stop_basis_reason}


def _row_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "zone_id": str(row.get("zone_id") or ""),
        "symbol": str(row.get("symbol") or ""),
        "direction": str(row.get("direction") or "UNKNOWN"),
        "a1_primary_evidence_type": str(row.get("a1_primary_evidence_type") or "UNKNOWN"),
        "a1_evidence_types": str(row.get("a1_evidence_types") or ""),
        "a1_strength_tier": str(row.get("strong_a1_tier") or row.get("a1_strength_tier") or "UNKNOWN"),
        "a1_best_horizon": str(row.get("a1_best_horizon") or "UNKNOWN"),
        "a2_accumulation_path_v2": str(row.get("a2_accumulation_path_v2") or "A2_UNKNOWN"),
        "a3_aggression_type_v2": str(row.get("a3_aggression_type_v2") or "NO_AGGRESSION"),
        "market_context_bucket": _market_context(row),
    }


def _market_context(row: Mapping[str, Any]) -> str:
    for name in ("trend_alignment", "trend_regime_enhanced_1h", "a2_context_alignment"):
        value = str(row.get(name) or "").strip()
        if value:
            return value
    return "UNKNOWN"


def _unavailable(
    row: Mapping[str, Any],
    entry_model: str,
    stop_model: str,
    reason: str,
    stop_basis_reason: str = "",
) -> dict[str, Any]:
    return {
        **_row_identity(row),
        "entry_model": entry_model,
        "stop_model": stop_model,
        "target_r": 0.0,
        "entry_ts": 0.0,
        "entry_bar_ts": 0.0,
        "entry_price_source": "",
        "entry_price": 0.0,
        "stop_price": 0.0,
        "stop_basis_reason": stop_basis_reason,
        "target_price": 0.0,
        "risk_u": 0.0,
        "fee_share_r": 0.0,
        "realized_r_1h": 0.0,
        "realized_outcome_1h": reason,
        "target_first_flag": False,
        "stop_first_flag": False,
        "ambiguous_flag": False,
        "complete_flag": False,
        "mfe_r_1h": 0.0,
        "mae_r_1h": 0.0,
    }
