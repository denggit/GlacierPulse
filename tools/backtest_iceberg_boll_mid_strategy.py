#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.research.a1_edge.io_utils import read_jsonl, read_kline_csv
from src.research.a1_edge.schema import parse_bool, parse_float, parse_timestamp


TRADE_FIELDS = [
    "trade_id",
    "candidate_event_key",
    "direction",
    "candidate_ts",
    "entry_ts",
    "exit_ts",
    "entry_price",
    "exit_price",
    "stop_price",
    "stop_basis_price",
    "stop_basis_type",
    "initial_boll_mid",
    "exit_boll_mid",
    "target_price_at_exit",
    "risk_u",
    "target_r_at_entry",
    "contracts",
    "notional",
    "required_margin",
    "leverage",
    "entry_fee",
    "exit_fee",
    "gross_pnl",
    "net_pnl",
    "realized_r",
    "equity_before",
    "equity_after",
    "exit_reason",
    "holding_minutes",
    "zone_lower",
    "zone_upper",
    "trigger_price",
    "settle_price",
    "min_trade_price",
    "max_trade_price",
    "first_iceberg_pie_min_trade_price",
    "first_iceberg_pie_max_trade_price",
    "truth_score",
    "absorption_rate",
    "hidden_volume",
    "active_notional",
    "boll_lower_at_signal",
    "boll_mid_at_signal",
    "boll_upper_at_signal",
    "boll_touch_flag",
    "boll_touch_type",
    "boll_touch_price",
    "zone_contains_boll_band_flag",
    "boll_distance_to_band",
    "boll_proximity_ratio_used",
]

REJECTION_FIELDS = [
    "candidate_event_key",
    "direction",
    "candidate_ts",
    "reason",
    "candidate_price",
    "zone_lower",
    "zone_upper",
    "boll_lower",
    "boll_mid",
    "boll_upper",
    "boll_lower_at_signal",
    "boll_mid_at_signal",
    "boll_upper_at_signal",
    "boll_touch_flag",
    "boll_touch_type",
    "boll_touch_price",
    "zone_contains_boll_band_flag",
    "entry_trigger",
    "stop_price",
    "stop_basis_price",
    "stop_basis_type",
    "risk_u",
    "target_r",
    "details",
]

EQUITY_FIELDS = ["sequence", "timestamp", "trade_id", "equity", "net_pnl", "drawdown_pct"]

EXIT_REASONS = [
    "STOP",
    "BOLL_MID_TARGET",
    "AMBIGUOUS_STOP_FIRST",
    "TARGET_INVALIDATED_BY_MIDLINE",
    "END_OF_DATA_CLOSE",
]


@dataclass(frozen=True)
class BacktestParams:
    initial_equity: float = 100.0
    risk_pct: float = 0.02
    leverage: float = 50.0
    contract_size_eth: float = 0.1
    min_contracts: float = 0.01
    contract_step: float = 0.01
    fee_rate_per_side: float = 0.0005
    boll_timeframe: str = "15m"
    boll_period: int = 20
    boll_std: float = 2.0
    boll_proximity_ratio: float = 0.20
    stop_buffer_u: float = 1.5
    entry_breakout_buffer_u: float = 0.0
    entry_timeout_min: int = 60
    min_target_r: float = 3.0
    one_position_at_a_time: bool = True
    boll_touch_required: bool = True
    timezone: str = "Asia/Shanghai"
    start_date: str | None = None
    end_date: str | None = None

    @property
    def timeframe_sec(self) -> int:
        text = str(self.boll_timeframe).strip().lower()
        if text.endswith("m"):
            return int(float(text[:-1]) * 60)
        if text.endswith("h"):
            return int(float(text[:-1]) * 3600)
        return int(float(text))


def load_finalized_iceberg_candidates(path: Path | str) -> list[dict[str, Any]]:
    rows = []
    for row in read_jsonl(path):
        direction = str(row.get("direction") or "").upper()
        if (
            row.get("record_type") == "candidate_finalized"
            and str(row.get("result") or "").upper() == "ICEBERG"
            and direction in {"BUY", "SELL"}
        ):
            normalized = dict(row)
            normalized["direction"] = direction
            normalized["_candidate_ts"] = candidate_ts(normalized)
            rows.append(normalized)
    return sorted(rows, key=lambda x: x["_candidate_ts"])


def candidate_ts(row: Mapping[str, Any]) -> float:
    for field in ("settle_ts", "candidate_ts", "event_ts", "reaction_event_ts", "ts", "timestamp"):
        ts = parse_timestamp(row.get(field))
        if ts > 0:
            return ts
    return 0.0


def candidate_event_key(row: Mapping[str, Any]) -> str:
    for field in ("event_key", "candidate_event_key", "zone_id"):
        value = row.get(field)
        if value not in (None, ""):
            return str(value)
    return f"{row.get('direction', 'UNKNOWN')}|{candidate_ts(row):.3f}|{parse_float(row.get('zone_lower')):.4f}|{parse_float(row.get('zone_upper')):.4f}"


def candidate_price(row: Mapping[str, Any]) -> float:
    for field in ("settle_price", "trigger_price"):
        value = parse_float(row.get(field))
        if value > 0:
            return value
    lower = parse_float(row.get("zone_lower"))
    upper = parse_float(row.get("zone_upper"))
    if lower > 0 and upper > 0:
        return (lower + upper) / 2.0
    return 0.0


def truth_score_value(row: Mapping[str, Any]) -> float:
    score = row.get("truth_score")
    if isinstance(score, Mapping):
        return parse_float(score.get("truth_score_total"))
    return parse_float(row.get("truth_score_total", score))


def date_boundaries(start_date: str | None, end_date: str | None, tz_name: str) -> tuple[float | None, float | None]:
    tz = ZoneInfo(tz_name)
    start_ts = None
    end_ts = None
    if start_date:
        start_ts = datetime.combine(datetime.fromisoformat(start_date).date(), time.min, tzinfo=tz).timestamp()
    if end_date:
        end_ts = datetime.combine(datetime.fromisoformat(end_date).date(), time.max, tzinfo=tz).timestamp()
    return start_ts, end_ts


def filter_by_date(rows: Iterable[Mapping[str, Any]], params: BacktestParams) -> list[dict[str, Any]]:
    start_ts, end_ts = date_boundaries(params.start_date, params.end_date, params.timezone)
    out = []
    for row in rows:
        ts = parse_float(row.get("_candidate_ts"), candidate_ts(row))
        if start_ts is not None and ts < start_ts:
            continue
        if end_ts is not None and ts > end_ts:
            continue
        out.append(dict(row))
    return out


def load_1m_klines(path: Path | str, timezone: str) -> list[dict[str, float]]:
    rows = read_kline_csv(path, kline_timezone=timezone)
    return [row for row in rows if row["open"] > 0 and row["high"] > 0 and row["low"] > 0 and row["close"] > 0]


def build_bollinger_bars(klines: list[Mapping[str, float]], params: BacktestParams) -> list[dict[str, float]]:
    if not klines:
        return []
    df = pd.DataFrame(klines).sort_values("timestamp")
    df["dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    rule = f"{params.timeframe_sec}s"
    grouped = (
        df.set_index("dt")
        .resample(rule, label="left", closed="left")
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            count=("close", "count"),
        )
        .dropna(subset=["open", "high", "low", "close"])
    )
    grouped = grouped[grouped["count"] >= max(1, params.timeframe_sec // 60)]
    grouped["middle"] = grouped["close"].rolling(params.boll_period).mean()
    grouped["std"] = grouped["close"].rolling(params.boll_period).std(ddof=0)
    grouped["upper"] = grouped["middle"] + params.boll_std * grouped["std"]
    grouped["lower"] = grouped["middle"] - params.boll_std * grouped["std"]
    rows: list[dict[str, float]] = []
    for dt, row in grouped.dropna(subset=["middle", "upper", "lower"]).iterrows():
        start_ts = float(dt.timestamp())
        rows.append(
            {
                "timestamp": start_ts,
                "close_ts": start_ts + params.timeframe_sec,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
                "middle": float(row["middle"]),
                "upper": float(row["upper"]),
                "lower": float(row["lower"]),
            }
        )
    return rows


def last_closed_boll(boll_bars: list[Mapping[str, float]], ts: float) -> dict[str, float] | None:
    selected = None
    for bar in boll_bars:
        if parse_float(bar.get("close_ts")) <= ts:
            selected = dict(bar)
        else:
            break
    return selected


def boll_bars_after(boll_bars: list[Mapping[str, float]], close_ts: float) -> list[dict[str, float]]:
    return [dict(bar) for bar in boll_bars if parse_float(bar.get("close_ts")) > close_ts]


def is_near_required_band(direction: str, price: float, boll: Mapping[str, float], ratio: float) -> tuple[bool, float]:
    lower = parse_float(boll.get("lower"))
    middle = parse_float(boll.get("middle"))
    upper = parse_float(boll.get("upper"))
    if direction == "BUY":
        threshold = lower + ratio * (middle - lower)
        return price <= threshold, price - lower
    threshold = upper - ratio * (upper - middle)
    return price >= threshold, upper - price


def boll_touch_info(row: Mapping[str, Any], boll: Mapping[str, float]) -> dict[str, Any]:
    direction = str(row.get("direction") or "").upper()
    zone_lower = parse_float(row.get("zone_lower"))
    zone_upper = parse_float(row.get("zone_upper"))
    boll_lower = parse_float(boll.get("lower"))
    boll_upper = parse_float(boll.get("upper"))

    if direction == "BUY":
        zone_contains = zone_lower > 0 and zone_upper > 0 and zone_lower <= boll_lower <= zone_upper
        pie_lows = [
            parse_float(row.get("first_iceberg_pie_min_trade_price")),
            parse_float(row.get("first_pie_min_trade_price")),
        ]
        valid_pie_lows = [value for value in pie_lows if value > 0]
        sweep_low = parse_float(row.get("min_trade_price"))
        candidates = [*valid_pie_lows, sweep_low, zone_lower]
        valid = [value for value in candidates if value > 0]
        touch_low = min(valid) if valid else 0.0
        pie_touched = any(value <= boll_lower for value in valid_pie_lows)
        sweep_touched = (sweep_low > 0 and sweep_low <= boll_lower) or (touch_low > 0 and touch_low <= boll_lower)
        if zone_contains:
            touch_type = "ZONE_CONTAINS_LOWER"
            touch_price = boll_lower
        elif pie_touched:
            touch_type = "PIE_LOW_TOUCH_LOWER"
            touch_price = min(value for value in valid_pie_lows if value <= boll_lower)
        elif sweep_touched:
            touch_type = "SWEEP_LOW_TOUCH_LOWER"
            touch_price = touch_low
        else:
            touch_type = "NO_TOUCH"
            touch_price = touch_low
        flag = bool(touch_low > 0 and touch_low <= boll_lower) or zone_contains
        return {
            "boll_touch_flag": flag,
            "boll_touch_type": touch_type,
            "boll_touch_price": touch_price,
            "zone_contains_boll_band_flag": zone_contains,
            "boll_distance_to_band": touch_low - boll_lower if touch_low > 0 else 0.0,
        }

    zone_contains = zone_lower > 0 and zone_upper > 0 and zone_lower <= boll_upper <= zone_upper
    pie_highs = [
        parse_float(row.get("first_iceberg_pie_max_trade_price")),
        parse_float(row.get("first_pie_max_trade_price")),
    ]
    valid_pie_highs = [value for value in pie_highs if value > 0]
    sweep_high = parse_float(row.get("max_trade_price"))
    candidates = [*valid_pie_highs, sweep_high, zone_upper]
    valid = [value for value in candidates if value > 0]
    touch_high = max(valid) if valid else 0.0
    pie_touched = any(value >= boll_upper for value in valid_pie_highs)
    sweep_touched = (sweep_high > 0 and sweep_high >= boll_upper) or (touch_high > 0 and touch_high >= boll_upper)
    if zone_contains:
        touch_type = "ZONE_CONTAINS_UPPER"
        touch_price = boll_upper
    elif pie_touched:
        touch_type = "PIE_HIGH_TOUCH_UPPER"
        touch_price = max(value for value in valid_pie_highs if value >= boll_upper)
    elif sweep_touched:
        touch_type = "SWEEP_HIGH_TOUCH_UPPER"
        touch_price = touch_high
    else:
        touch_type = "NO_TOUCH"
        touch_price = touch_high
    flag = bool(touch_high > 0 and touch_high >= boll_upper) or zone_contains
    return {
        "boll_touch_flag": flag,
        "boll_touch_type": touch_type,
        "boll_touch_price": touch_price,
        "zone_contains_boll_band_flag": zone_contains,
        "boll_distance_to_band": boll_upper - touch_high if touch_high > 0 else 0.0,
    }


def find_a3_entry(
    row: Mapping[str, Any],
    klines: list[Mapping[str, float]],
    params: BacktestParams,
) -> tuple[dict[str, Any] | None, float]:
    direction = str(row.get("direction") or "").upper()
    settle_ts = candidate_ts(row)
    timeout_ts = settle_ts + params.entry_timeout_min * 60.0
    if direction == "BUY":
        trigger = parse_float(row.get("zone_upper")) + params.entry_breakout_buffer_u
        for idx, bar in enumerate(klines):
            ts = parse_float(bar.get("timestamp"))
            if ts <= settle_ts:
                continue
            if ts > timeout_ts:
                break
            if parse_float(bar.get("high")) >= trigger:
                return {"idx": idx, "entry_ts": ts, "entry_price": trigger, "entry_trigger": trigger}, trigger
    elif direction == "SELL":
        trigger = parse_float(row.get("zone_lower")) - params.entry_breakout_buffer_u
        for idx, bar in enumerate(klines):
            ts = parse_float(bar.get("timestamp"))
            if ts <= settle_ts:
                continue
            if ts > timeout_ts:
                break
            if parse_float(bar.get("low")) <= trigger:
                return {"idx": idx, "entry_ts": ts, "entry_price": trigger, "entry_trigger": trigger}, trigger
    return None, parse_float(row.get("zone_upper" if direction == "BUY" else "zone_lower"))


def structural_stop(row: Mapping[str, Any], entry_price: float, params: BacktestParams) -> tuple[float, float, float, str]:
    direction = str(row.get("direction") or "").upper()
    if direction == "BUY":
        basis, basis_type = extreme_positive_value(
            row,
            "min",
            [
                ("first_iceberg_pie_min_trade_price", "FIRST_ICEBERG_PIE_MIN_TRADE_PRICE"),
                ("first_pie_min_trade_price", "FIRST_PIE_MIN_TRADE_PRICE"),
                ("min_trade_price", "MIN_TRADE_PRICE"),
                ("zone_lower", "ZONE_LOWER"),
            ],
        )
        stop = basis - params.stop_buffer_u if basis > 0 else 0.0
        return stop, entry_price - stop, basis, basis_type
    basis, basis_type = extreme_positive_value(
        row,
        "max",
        [
            ("first_iceberg_pie_max_trade_price", "FIRST_ICEBERG_PIE_MAX_TRADE_PRICE"),
            ("first_pie_max_trade_price", "FIRST_PIE_MAX_TRADE_PRICE"),
            ("max_trade_price", "MAX_TRADE_PRICE"),
            ("zone_upper", "ZONE_UPPER"),
        ],
    )
    stop = basis + params.stop_buffer_u if basis > 0 else 0.0
    return stop, stop - entry_price, basis, basis_type


def extreme_positive_value(row: Mapping[str, Any], mode: str, fields: list[tuple[str, str]]) -> tuple[float, str]:
    valid: list[tuple[float, str]] = []
    for field, label in fields:
        value = parse_float(row.get(field))
        if value > 0:
            valid.append((value, label))
    if not valid:
        return 0.0, "UNAVAILABLE"
    if mode == "max":
        return max(valid, key=lambda x: x[0])
    return min(valid, key=lambda x: x[0])


def floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    decimals = max(0, len(f"{step:.12f}".rstrip("0").split(".")[-1]))
    return round(math.floor((value + 1e-12) / step) * step, decimals)


def calculate_position_size(
    equity: float,
    entry_price: float,
    risk_u: float,
    params: BacktestParams,
) -> tuple[dict[str, float] | None, str | None]:
    risk_budget = equity * params.risk_pct
    contract_stop_loss = risk_u * params.contract_size_eth
    estimated_contract_fee = entry_price * params.contract_size_eth * params.fee_rate_per_side * 2.0
    contract_total_risk = contract_stop_loss + estimated_contract_fee
    if contract_total_risk <= 0:
        return None, "INVALID_STOP"
    contracts = floor_to_step(risk_budget / contract_total_risk, params.contract_step)
    if contracts < params.min_contracts:
        return None, "SIZE_BELOW_MIN_CONTRACTS"
    notional = contracts * params.contract_size_eth * entry_price
    required_margin = notional / params.leverage
    if required_margin > equity:
        max_by_margin = floor_to_step(
            equity * params.leverage / (entry_price * params.contract_size_eth),
            params.contract_step,
        )
        contracts = min(contracts, max_by_margin)
    if contracts < params.min_contracts:
        return None, "INSUFFICIENT_MARGIN"

    actual_stop_loss = contracts * params.contract_size_eth * risk_u
    estimated_fee = contracts * params.contract_size_eth * entry_price * params.fee_rate_per_side * 2.0
    actual_total_risk = actual_stop_loss + estimated_fee
    if actual_total_risk > risk_budget * 1.02:
        max_by_risk = floor_to_step(risk_budget / contract_total_risk, params.contract_step)
        contracts = min(contracts, max_by_risk)
    if contracts < params.min_contracts:
        return None, "SIZE_BELOW_MIN_CONTRACTS"

    notional = contracts * params.contract_size_eth * entry_price
    required_margin = notional / params.leverage
    actual_stop_loss = contracts * params.contract_size_eth * risk_u
    estimated_fee = contracts * params.contract_size_eth * entry_price * params.fee_rate_per_side * 2.0
    actual_total_risk = actual_stop_loss + estimated_fee
    return (
        {
            "contracts": contracts,
            "notional": notional,
            "required_margin": required_margin,
            "actual_stop_loss": actual_stop_loss,
            "estimated_fee": estimated_fee,
            "actual_total_risk": actual_total_risk,
            "risk_budget": risk_budget,
        },
        None,
    )


def simulate_exit(
    direction: str,
    klines: list[Mapping[str, float]],
    entry_idx: int,
    entry_ts: float,
    entry_price: float,
    stop_price: float,
    initial_target: float,
    entry_boll: Mapping[str, float],
    boll_bars: list[Mapping[str, float]],
) -> dict[str, float | str]:
    current_target = initial_target
    current_mid = parse_float(entry_boll.get("middle"))
    last_boll_close_ts = parse_float(entry_boll.get("close_ts"))
    pending_boll = boll_bars_after(boll_bars, last_boll_close_ts)
    next_boll_idx = 0
    last_bar = dict(klines[entry_idx])

    for bar in klines[entry_idx:]:
        bar_ts = parse_float(bar.get("timestamp"))
        last_bar = dict(bar)
        while next_boll_idx < len(pending_boll) and parse_float(pending_boll[next_boll_idx].get("close_ts")) <= bar_ts:
            boll = pending_boll[next_boll_idx]
            next_boll_idx += 1
            current_mid = parse_float(boll.get("middle"))
            current_target = current_mid
            if (direction == "BUY" and current_target <= entry_price) or (
                direction == "SELL" and current_target >= entry_price
            ):
                return {
                    "exit_ts": parse_float(boll.get("close_ts")),
                    "exit_price": parse_float(boll.get("close")),
                    "exit_reason": "TARGET_INVALIDATED_BY_MIDLINE",
                    "exit_boll_mid": current_mid,
                    "target_price_at_exit": current_target,
                }

        if direction == "BUY":
            hit_stop = parse_float(bar.get("low")) <= stop_price
            hit_target = parse_float(bar.get("high")) >= current_target
        else:
            hit_stop = parse_float(bar.get("high")) >= stop_price
            hit_target = parse_float(bar.get("low")) <= current_target
        if hit_stop and hit_target:
            return {
                "exit_ts": bar_ts,
                "exit_price": stop_price,
                "exit_reason": "AMBIGUOUS_STOP_FIRST",
                "exit_boll_mid": current_mid,
                "target_price_at_exit": current_target,
            }
        if hit_stop:
            return {
                "exit_ts": bar_ts,
                "exit_price": stop_price,
                "exit_reason": "STOP",
                "exit_boll_mid": current_mid,
                "target_price_at_exit": current_target,
            }
        if hit_target:
            return {
                "exit_ts": bar_ts,
                "exit_price": current_target,
                "exit_reason": "BOLL_MID_TARGET",
                "exit_boll_mid": current_mid,
                "target_price_at_exit": current_target,
            }

    return {
        "exit_ts": parse_float(last_bar.get("timestamp")),
        "exit_price": parse_float(last_bar.get("close")),
        "exit_reason": "END_OF_DATA_CLOSE",
        "exit_boll_mid": current_mid,
        "target_price_at_exit": current_target,
    }


def make_rejection(
    row: Mapping[str, Any],
    reason: str,
    *,
    candidate_price_value: float = 0.0,
    boll: Mapping[str, float] | None = None,
    touch: Mapping[str, Any] | None = None,
    entry_trigger: float = 0.0,
    stop_price: float = 0.0,
    stop_basis_price: float = 0.0,
    stop_basis_type: str = "",
    risk_u: float = 0.0,
    target_r: float = 0.0,
    details: str = "",
) -> dict[str, Any]:
    boll = boll or {}
    touch = touch or {}
    return {
        "candidate_event_key": candidate_event_key(row),
        "direction": row.get("direction", ""),
        "candidate_ts": round(candidate_ts(row), 8),
        "reason": reason,
        "candidate_price": round(candidate_price_value, 8) if candidate_price_value else "",
        "zone_lower": round(parse_float(row.get("zone_lower")), 8),
        "zone_upper": round(parse_float(row.get("zone_upper")), 8),
        "boll_lower": round(parse_float(boll.get("lower")), 8) if boll else "",
        "boll_mid": round(parse_float(boll.get("middle")), 8) if boll else "",
        "boll_upper": round(parse_float(boll.get("upper")), 8) if boll else "",
        "boll_lower_at_signal": round(parse_float(boll.get("lower")), 8) if boll else "",
        "boll_mid_at_signal": round(parse_float(boll.get("middle")), 8) if boll else "",
        "boll_upper_at_signal": round(parse_float(boll.get("upper")), 8) if boll else "",
        "boll_touch_flag": bool(touch.get("boll_touch_flag", False)) if touch else "",
        "boll_touch_type": touch.get("boll_touch_type", "") if touch else "",
        "boll_touch_price": round(parse_float(touch.get("boll_touch_price")), 8) if touch else "",
        "zone_contains_boll_band_flag": bool(touch.get("zone_contains_boll_band_flag", False)) if touch else "",
        "entry_trigger": round(entry_trigger, 8) if entry_trigger else "",
        "stop_price": round(stop_price, 8) if stop_price else "",
        "stop_basis_price": round(stop_basis_price, 8) if stop_basis_price else "",
        "stop_basis_type": stop_basis_type,
        "risk_u": round(risk_u, 8) if risk_u else "",
        "target_r": round(target_r, 8) if target_r else "",
        "details": details,
    }


def simulate_backtest(
    candidates: list[Mapping[str, Any]],
    klines: list[Mapping[str, float]],
    params: BacktestParams,
) -> dict[str, Any]:
    candidates = filter_by_date(candidates, params)
    boll_bars = build_bollinger_bars(klines, params)
    equity = params.initial_equity
    high_water = equity
    closed_positions: list[tuple[float, float]] = []
    trades: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    proposals: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = [
        {"sequence": 0, "timestamp": "", "trade_id": "", "equity": round(equity, 8), "net_pnl": 0.0, "drawdown_pct": 0.0}
    ]

    # Phase 1: static candidate validation and no-equity trade proposal generation.
    for row in candidates:
        direction = str(row.get("direction") or "").upper()
        c_ts = candidate_ts(row)
        c_price = candidate_price(row)
        if not klines or c_ts <= 0 or c_price <= 0:
            rejections.append(make_rejection(row, "DATA_UNAVAILABLE", candidate_price_value=c_price))
            continue

        signal_boll = last_closed_boll(boll_bars, c_ts)
        if not signal_boll:
            rejections.append(make_rejection(row, "BOLL_UNAVAILABLE", candidate_price_value=c_price))
            continue
        touch = boll_touch_info(row, signal_boll)
        if params.boll_touch_required:
            if not bool(touch.get("boll_touch_flag")):
                reason = "NOT_TOUCH_BOLL_LOWER" if direction == "BUY" else "NOT_TOUCH_BOLL_UPPER"
                rejections.append(make_rejection(row, reason, candidate_price_value=c_price, boll=signal_boll, touch=touch))
                continue
            distance = parse_float(touch.get("boll_distance_to_band"))
        else:
            near, distance = is_near_required_band(direction, c_price, signal_boll, params.boll_proximity_ratio)
            if not near:
                reason = "NOT_NEAR_BOLL_LOWER" if direction == "BUY" else "NOT_NEAR_BOLL_UPPER"
                rejections.append(make_rejection(row, reason, candidate_price_value=c_price, boll=signal_boll, touch=touch))
                continue

        entry, entry_trigger = find_a3_entry(row, klines, params)
        if not entry:
            rejections.append(
                make_rejection(
                    row,
                    "NO_A3_BREAKOUT",
                    candidate_price_value=c_price,
                    boll=signal_boll,
                    touch=touch,
                    entry_trigger=entry_trigger,
                )
            )
            continue

        entry_price = parse_float(entry["entry_price"])
        entry_ts = parse_float(entry["entry_ts"])
        stop_price, risk_u, stop_basis_price, stop_basis_type = structural_stop(row, entry_price, params)
        if risk_u <= 0:
            rejections.append(
                make_rejection(
                    row,
                    "INVALID_STOP",
                    candidate_price_value=c_price,
                    boll=signal_boll,
                    touch=touch,
                    entry_trigger=entry_trigger,
                    stop_price=stop_price,
                    stop_basis_price=stop_basis_price,
                    stop_basis_type=stop_basis_type,
                    risk_u=risk_u,
                )
            )
            continue

        entry_boll = last_closed_boll(boll_bars, entry_ts)
        if not entry_boll:
            rejections.append(
                make_rejection(row, "BOLL_UNAVAILABLE", candidate_price_value=c_price, boll=signal_boll, touch=touch, entry_trigger=entry_trigger)
            )
            continue
        target_price = parse_float(entry_boll.get("middle"))
        if direction == "BUY":
            target_r = (target_price - entry_price) / risk_u
            target_valid = target_price > entry_price and target_r >= params.min_target_r
        else:
            target_r = (entry_price - target_price) / risk_u
            target_valid = target_price < entry_price and target_r >= params.min_target_r
        if not target_valid:
            rejections.append(
                make_rejection(
                    row,
                    "TARGET_LT_MIN_R",
                    candidate_price_value=c_price,
                    boll=signal_boll,
                    touch=touch,
                    entry_trigger=entry_trigger,
                    stop_price=stop_price,
                    stop_basis_price=stop_basis_price,
                    stop_basis_type=stop_basis_type,
                    risk_u=risk_u,
                    target_r=target_r,
                )
            )
            continue

        exit_info = simulate_exit(
            direction,
            klines,
            int(entry["idx"]),
            entry_ts,
            entry_price,
            stop_price,
            target_price,
            entry_boll,
            boll_bars,
        )
        proposals.append(
            {
                "row": row,
                "direction": direction,
                "candidate_ts": c_ts,
                "candidate_price": c_price,
                "signal_boll": signal_boll,
                "touch": touch,
                "distance": distance,
                "entry": entry,
                "entry_trigger": entry_trigger,
                "entry_price": entry_price,
                "entry_ts": entry_ts,
                "stop_price": stop_price,
                "risk_u": risk_u,
                "stop_basis_price": stop_basis_price,
                "stop_basis_type": stop_basis_type,
                "entry_boll": entry_boll,
                "target_price": target_price,
                "target_r": target_r,
                "exit_info": exit_info,
                "exit_ts": parse_float(exit_info["exit_ts"]),
            }
        )

    # Phase 2: execute accepted proposals by real entry time so sizing/equity/order are chronological.
    proposals.sort(
        key=lambda p: (
            parse_float(p.get("entry_ts")),
            parse_float(p.get("candidate_ts")),
            candidate_event_key(p.get("row", {})),
        )
    )

    for proposal in proposals:
        row = proposal["row"]
        direction = str(proposal["direction"])
        c_ts = parse_float(proposal["candidate_ts"])
        c_price = parse_float(proposal["candidate_price"])
        signal_boll = proposal["signal_boll"]
        touch = proposal["touch"]
        distance = parse_float(proposal["distance"])
        entry_trigger = parse_float(proposal["entry_trigger"])
        entry_price = parse_float(proposal["entry_price"])
        entry_ts = parse_float(proposal["entry_ts"])
        exit_info = proposal["exit_info"]
        exit_ts = parse_float(proposal["exit_ts"])
        stop_price = parse_float(proposal["stop_price"])
        risk_u = parse_float(proposal["risk_u"])
        stop_basis_price = parse_float(proposal["stop_basis_price"])
        stop_basis_type = str(proposal["stop_basis_type"])
        entry_boll = proposal["entry_boll"]
        target_r = parse_float(proposal["target_r"])

        if params.one_position_at_a_time and any(intervals_overlap(entry_ts, exit_ts, start, end) for start, end in closed_positions):
            rejections.append(
                make_rejection(
                    row,
                    "POSITION_ALREADY_OPEN",
                    candidate_price_value=c_price,
                    boll=signal_boll,
                    touch=touch,
                    entry_trigger=entry_trigger,
                    stop_price=stop_price,
                    stop_basis_price=stop_basis_price,
                    stop_basis_type=stop_basis_type,
                    risk_u=risk_u,
                    target_r=target_r,
                )
            )
            continue

        size, size_reason = calculate_position_size(equity, entry_price, risk_u, params)
        if not size:
            rejections.append(
                make_rejection(
                    row,
                    str(size_reason),
                    candidate_price_value=c_price,
                    boll=signal_boll,
                    touch=touch,
                    entry_trigger=entry_trigger,
                    stop_price=stop_price,
                    stop_basis_price=stop_basis_price,
                    stop_basis_type=stop_basis_type,
                    risk_u=risk_u,
                    target_r=target_r,
                )
            )
            continue

        exit_price = parse_float(exit_info["exit_price"])
        contracts = parse_float(size["contracts"])
        if direction == "BUY":
            gross_pnl = contracts * params.contract_size_eth * (exit_price - entry_price)
        else:
            gross_pnl = contracts * params.contract_size_eth * (entry_price - exit_price)
        entry_fee = contracts * params.contract_size_eth * entry_price * params.fee_rate_per_side
        exit_fee = contracts * params.contract_size_eth * exit_price * params.fee_rate_per_side
        net_pnl = gross_pnl - entry_fee - exit_fee
        equity_before = equity
        equity = equity + net_pnl
        planned_risk = parse_float(size["actual_total_risk"])
        realized_r = net_pnl / planned_risk if planned_risk > 0 else 0.0
        trade_id = f"T{len(trades) + 1:05d}"
        trade = {
            "trade_id": trade_id,
            "candidate_event_key": candidate_event_key(row),
            "direction": direction,
            "candidate_ts": round(c_ts, 8),
            "entry_ts": round(entry_ts, 8),
            "exit_ts": round(exit_ts, 8),
            "entry_price": round(entry_price, 8),
            "exit_price": round(exit_price, 8),
            "stop_price": round(stop_price, 8),
            "stop_basis_price": round(stop_basis_price, 8),
            "stop_basis_type": stop_basis_type,
            "initial_boll_mid": round(parse_float(entry_boll.get("middle")), 8),
            "exit_boll_mid": round(parse_float(exit_info["exit_boll_mid"]), 8),
            "target_price_at_exit": round(parse_float(exit_info["target_price_at_exit"]), 8),
            "risk_u": round(risk_u, 8),
            "target_r_at_entry": round(target_r, 8),
            "contracts": round(contracts, 8),
            "notional": round(parse_float(size["notional"]), 8),
            "required_margin": round(parse_float(size["required_margin"]), 8),
            "leverage": params.leverage,
            "entry_fee": round(entry_fee, 8),
            "exit_fee": round(exit_fee, 8),
            "gross_pnl": round(gross_pnl, 8),
            "net_pnl": round(net_pnl, 8),
            "realized_r": round(realized_r, 8),
            "equity_before": round(equity_before, 8),
            "equity_after": round(equity, 8),
            "exit_reason": exit_info["exit_reason"],
            "holding_minutes": round((parse_float(exit_info["exit_ts"]) - entry_ts) / 60.0, 8),
            "zone_lower": round(parse_float(row.get("zone_lower")), 8),
            "zone_upper": round(parse_float(row.get("zone_upper")), 8),
            "trigger_price": round(parse_float(row.get("trigger_price")), 8),
            "settle_price": round(parse_float(row.get("settle_price")), 8),
            "min_trade_price": round(parse_float(row.get("min_trade_price")), 8),
            "max_trade_price": round(parse_float(row.get("max_trade_price")), 8),
            "first_iceberg_pie_min_trade_price": round(parse_float(row.get("first_iceberg_pie_min_trade_price")), 8),
            "first_iceberg_pie_max_trade_price": round(parse_float(row.get("first_iceberg_pie_max_trade_price")), 8),
            "truth_score": round(truth_score_value(row), 8),
            "absorption_rate": round(parse_float(row.get("absorption_rate")), 8),
            "hidden_volume": round(parse_float(row.get("hidden_volume")), 8),
            "active_notional": round(parse_float(row.get("active_notional")), 8),
            "boll_lower_at_signal": round(parse_float(signal_boll.get("lower")), 8),
            "boll_mid_at_signal": round(parse_float(signal_boll.get("middle")), 8),
            "boll_upper_at_signal": round(parse_float(signal_boll.get("upper")), 8),
            "boll_touch_flag": bool(touch.get("boll_touch_flag")),
            "boll_touch_type": touch.get("boll_touch_type", ""),
            "boll_touch_price": round(parse_float(touch.get("boll_touch_price")), 8),
            "zone_contains_boll_band_flag": bool(touch.get("zone_contains_boll_band_flag")),
            "boll_distance_to_band": round(distance, 8),
            "boll_proximity_ratio_used": params.boll_proximity_ratio,
        }
        trades.append(trade)
        closed_positions.append((entry_ts, exit_ts))
        high_water = max(high_water, equity)
        drawdown = (equity / high_water - 1.0) * 100.0 if high_water > 0 else 0.0
        equity_curve.append(
            {
                "sequence": len(equity_curve),
                "timestamp": trade["exit_ts"],
                "trade_id": trade_id,
                "equity": round(equity, 8),
                "net_pnl": round(net_pnl, 8),
                "drawdown_pct": round(drawdown, 8),
            }
        )

    summary = build_summary(trades, rejections, candidates, equity_curve, params)
    return {
        "trades": trades,
        "rejections": rejections,
        "equity_curve": equity_curve,
        "summary": summary,
        "boll_bars": boll_bars,
    }


def intervals_overlap(a_start: float, a_end: float, b_start: float, b_end: float) -> bool:
    return a_start < b_end and b_start < a_end


def build_summary(
    trades: list[Mapping[str, Any]],
    rejections: list[Mapping[str, Any]],
    candidates: list[Mapping[str, Any]],
    equity_curve: list[Mapping[str, Any]],
    params: BacktestParams,
) -> dict[str, Any]:
    final_equity = parse_float(equity_curve[-1].get("equity"), params.initial_equity) if equity_curve else params.initial_equity
    net_pnl_values = [parse_float(t.get("net_pnl")) for t in trades]
    wins = [t for t in trades if parse_float(t.get("net_pnl")) > 0]
    losses = [t for t in trades if parse_float(t.get("net_pnl")) < 0]
    realized = [parse_float(t.get("realized_r")) for t in trades]
    fees = sum(parse_float(t.get("entry_fee")) + parse_float(t.get("exit_fee")) for t in trades)
    gross_wins = sum(parse_float(t.get("net_pnl")) for t in wins)
    gross_losses = abs(sum(parse_float(t.get("net_pnl")) for t in losses))
    max_drawdown_pct = min([parse_float(x.get("drawdown_pct")) for x in equity_curve] or [0.0])
    rejection_count_by_reason: dict[str, int] = {}
    for row in rejections:
        reason = str(row.get("reason") or "UNKNOWN")
        rejection_count_by_reason[reason] = rejection_count_by_reason.get(reason, 0) + 1

    direction_stats = {side: side_stats(trades, side) for side in ("BUY", "SELL")}
    exit_reason_stats = {reason: sum(1 for t in trades if t.get("exit_reason") == reason) for reason in EXIT_REASONS}
    by_month = build_monthly_stats(trades, params)
    span_days = backtest_span_days(candidates, params)

    return {
        "version": "V7.1.0",
        "strategy_name": "ICEBERG Bollinger Midline Strategy Backtest",
        "initial_equity": round(params.initial_equity, 8),
        "final_equity": round(final_equity, 8),
        "net_profit": round(final_equity - params.initial_equity, 8),
        "total_return_pct": round((final_equity / params.initial_equity - 1.0) * 100.0, 8) if params.initial_equity else 0.0,
        "max_drawdown_pct": round(max_drawdown_pct, 8),
        "total_trades": len(trades),
        "win_rate": round(len(wins) / len(trades), 8) if trades else 0.0,
        "profit_factor": round(gross_wins / gross_losses, 8) if gross_losses > 0 else (None if gross_wins <= 0 else "INF"),
        "avg_realized_r": round(sum(realized) / len(realized), 8) if realized else 0.0,
        "median_realized_r": round(statistics.median(realized), 8) if realized else 0.0,
        "avg_win_r": round(sum(parse_float(t.get("realized_r")) for t in wins) / len(wins), 8) if wins else 0.0,
        "avg_loss_r": round(sum(parse_float(t.get("realized_r")) for t in losses) / len(losses), 8) if losses else 0.0,
        "max_win_r": round(max(realized), 8) if realized else 0.0,
        "max_loss_r": round(min(realized), 8) if realized else 0.0,
        "total_fees": round(fees, 8),
        "avg_holding_minutes": round(sum(parse_float(t.get("holding_minutes")) for t in trades) / len(trades), 8) if trades else 0.0,
        "trades_per_day": round(len(trades) / span_days, 8) if span_days > 0 else 0.0,
        "candidates_total": len(candidates),
        "iceberg_candidates": len(candidates),
        "rejection_count_by_reason": rejection_count_by_reason,
        "direction": direction_stats,
        "exit_reason": exit_reason_stats,
        "boll_side": {
            "near_lower_buy": sum(1 for t in trades if t.get("direction") == "BUY"),
            "near_upper_sell": sum(1 for t in trades if t.get("direction") == "SELL"),
        },
        "by_month": by_month,
        "parameters": params.__dict__,
    }


def side_stats(trades: list[Mapping[str, Any]], direction: str) -> dict[str, Any]:
    rows = [t for t in trades if t.get("direction") == direction]
    wins = [t for t in rows if parse_float(t.get("net_pnl")) > 0]
    pnl = sum(parse_float(t.get("net_pnl")) for t in rows)
    return {
        "trades": len(rows),
        "pnl": round(pnl, 8),
        "win_rate": round(len(wins) / len(rows), 8) if rows else 0.0,
        "avg_r": round(sum(parse_float(t.get("realized_r")) for t in rows) / len(rows), 8) if rows else 0.0,
    }


def build_monthly_stats(trades: list[Mapping[str, Any]], params: BacktestParams) -> dict[str, dict[str, Any]]:
    tz = ZoneInfo(params.timezone)
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for trade in trades:
        ts = parse_float(trade.get("exit_ts"))
        key = datetime.fromtimestamp(ts, tz).strftime("%Y-%m") if ts > 0 else "UNKNOWN"
        grouped.setdefault(key, []).append(trade)
    out: dict[str, dict[str, Any]] = {}
    for key, rows in sorted(grouped.items()):
        wins = [t for t in rows if parse_float(t.get("net_pnl")) > 0]
        out[key] = {
            "trades": len(rows),
            "pnl": round(sum(parse_float(t.get("net_pnl")) for t in rows), 8),
            "win_rate": round(len(wins) / len(rows), 8) if rows else 0.0,
            "avg_r": round(sum(parse_float(t.get("realized_r")) for t in rows) / len(rows), 8) if rows else 0.0,
        }
    return out


def backtest_span_days(candidates: list[Mapping[str, Any]], params: BacktestParams) -> float:
    start_ts, end_ts = date_boundaries(params.start_date, params.end_date, params.timezone)
    if start_ts is not None and end_ts is not None:
        return max((end_ts - start_ts) / 86400.0, 1.0)
    ts_values = [candidate_ts(c) for c in candidates if candidate_ts(c) > 0]
    if len(ts_values) < 2:
        return 1.0
    return max((max(ts_values) - min(ts_values)) / 86400.0, 1.0)


def write_csv_file(path: Path, rows: Iterable[Mapping[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_outputs(result: Mapping[str, Any], out_dir: Path | str) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    write_csv_file(out / "iceberg_boll_trades.csv", result.get("trades", []), TRADE_FIELDS)
    write_csv_file(out / "iceberg_boll_equity_curve.csv", result.get("equity_curve", []), EQUITY_FIELDS)
    write_csv_file(out / "iceberg_boll_rejections.csv", result.get("rejections", []), REJECTION_FIELDS)
    summary = dict(result.get("summary", {}))
    with (out / "iceberg_boll_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    (out / "iceberg_boll_summary.md").write_text(render_summary_md(summary), encoding="utf-8")


def render_summary_md(summary: Mapping[str, Any]) -> str:
    lines = [
        "# ICEBERG Bollinger Midline Strategy Backtest",
        "",
        f"- Version: {summary.get('version', '')}",
        f"- Initial equity: {summary.get('initial_equity', 0)}",
        f"- Final equity: {summary.get('final_equity', 0)}",
        f"- Net profit: {summary.get('net_profit', 0)}",
        f"- Total return pct: {summary.get('total_return_pct', 0)}",
        f"- Max drawdown pct: {summary.get('max_drawdown_pct', 0)}",
        f"- Total trades: {summary.get('total_trades', 0)}",
        f"- Win rate: {summary.get('win_rate', 0)}",
        f"- Profit factor: {summary.get('profit_factor', 0)}",
        f"- Total fees: {summary.get('total_fees', 0)}",
        "",
        "## Rejections",
    ]
    for reason, count in dict(summary.get("rejection_count_by_reason", {})).items():
        lines.append(f"- {reason}: {count}")
    lines.extend(["", "## Direction"])
    for direction, stats in dict(summary.get("direction", {})).items():
        lines.append(f"- {direction}: trades={stats.get('trades', 0)} pnl={stats.get('pnl', 0)} win_rate={stats.get('win_rate', 0)} avg_r={stats.get('avg_r', 0)}")
    lines.extend(["", "## Exit Reason"])
    for reason, count in dict(summary.get("exit_reason", {})).items():
        lines.append(f"- {reason}: {count}")
    lines.append("")
    return "\n".join(lines)


def run_from_files(
    phase1_candidates: Path | str,
    kline: Path | str,
    out: Path | str,
    params: BacktestParams,
) -> dict[str, Any]:
    candidates = load_finalized_iceberg_candidates(phase1_candidates)
    klines = load_1m_klines(kline, params.timezone)
    result = simulate_backtest(candidates, klines, params)
    write_outputs(result, out)
    return result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="V7.1.0 ICEBERG Bollinger midline offline strategy backtest")
    parser.add_argument("--phase1-candidates", required=True)
    parser.add_argument("--kline", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--timezone", default="Asia/Shanghai")
    parser.add_argument("--initial-equity", type=float, default=100.0)
    parser.add_argument("--risk-pct", type=float, default=0.02)
    parser.add_argument("--leverage", type=float, default=50.0)
    parser.add_argument("--contract-size-eth", type=float, default=0.1)
    parser.add_argument("--min-contracts", type=float, default=0.01)
    parser.add_argument("--contract-step", type=float, default=0.01)
    parser.add_argument("--fee-rate-per-side", type=float, default=0.0005)
    parser.add_argument("--boll-timeframe", default="15m")
    parser.add_argument("--boll-period", type=int, default=20)
    parser.add_argument("--boll-std", type=float, default=2.0)
    parser.add_argument("--boll-proximity-ratio", type=float, default=0.20)
    parser.add_argument("--stop-buffer-u", type=float, default=1.5)
    parser.add_argument("--entry-breakout-buffer-u", type=float, default=0.0)
    parser.add_argument("--entry-timeout-min", type=int, default=60)
    parser.add_argument("--min-target-r", type=float, default=3.0)
    parser.add_argument("--one-position-at-a-time", default="true")
    parser.add_argument("--boll-touch-required", default="true")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    return parser


def params_from_args(args: argparse.Namespace) -> BacktestParams:
    return BacktestParams(
        initial_equity=args.initial_equity,
        risk_pct=args.risk_pct,
        leverage=args.leverage,
        contract_size_eth=args.contract_size_eth,
        min_contracts=args.min_contracts,
        contract_step=args.contract_step,
        fee_rate_per_side=args.fee_rate_per_side,
        boll_timeframe=args.boll_timeframe,
        boll_period=args.boll_period,
        boll_std=args.boll_std,
        boll_proximity_ratio=args.boll_proximity_ratio,
        stop_buffer_u=args.stop_buffer_u,
        entry_breakout_buffer_u=args.entry_breakout_buffer_u,
        entry_timeout_min=args.entry_timeout_min,
        min_target_r=args.min_target_r,
        one_position_at_a_time=parse_bool(args.one_position_at_a_time, True),
        boll_touch_required=parse_bool(args.boll_touch_required, True),
        timezone=args.timezone,
        start_date=args.start_date,
        end_date=args.end_date,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    params = params_from_args(args)
    run_from_files(args.phase1_candidates, args.kline, args.out, params)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
