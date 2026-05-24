#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from src.research.a1_edge.io_utils import normalize_klines
from src.research.a1_edge.schema import parse_float

from .forward import infer_bar_interval_sec
from .models import local_session


WINDOWS = ((900, "15m"), (3600, "1h"), (14400, "4h"))


class ZoneMarketContextCalculator:
    def __init__(self, kline_timezone: str = "Asia/Shanghai") -> None:
        self.kline_timezone = kline_timezone

    def attach_market_context(
        self,
        zone_rows: Iterable[Mapping[str, Any]],
        kline_rows: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        bars = normalize_klines(kline_rows, kline_timezone=self.kline_timezone)
        rolling_1h_volumes = _rolling_volumes(bars, 3600)
        return [self.attach_to_row(row, bars, rolling_1h_volumes) for row in zone_rows or []]

    def attach_to_row(
        self,
        row: Mapping[str, Any],
        bars: list[dict[str, float]],
        rolling_1h_volumes: list[float] | None = None,
    ) -> dict[str, Any]:
        result = dict(row)
        if rolling_1h_volumes is None:
            rolling_1h_volumes = _rolling_volumes(bars, 3600)
        anchor_ts = _resolve_anchor_ts(result)
        result.update(_empty_context(anchor_ts, self.kline_timezone))
        if not bars or anchor_ts <= 0:
            return result

        interval_sec = infer_bar_interval_sec(bars)
        anchor_idx = _last_completed_bar_index(bars, anchor_ts, interval_sec)
        if anchor_idx < 0:
            return result
        anchor_price = parse_float(bars[anchor_idx].get("close"))
        if anchor_price <= 0:
            return result

        window_stats: dict[str, dict[str, Any]] = {}
        for window_sec, label in WINDOWS:
            stats = _pre_window_stats(bars, anchor_idx, window_sec, interval_sec)
            window_stats[label] = stats
            result[f"pre_{label}_return_u"] = stats["return_u"]
            result[f"pre_{label}_return_pct"] = stats["return_pct"]
            result[f"is_complete_pre_{label}"] = stats["is_complete"]
            result[f"pre_{label}_range_u"] = stats["range_u"]
            result[f"pre_{label}_volume"] = stats["volume"]

        pre_1h = window_stats["1h"]
        pre_4h = window_stats["4h"]
        result["pre_1h_direction"] = _direction(pre_1h["return_pct"], up_threshold=0.30, down_threshold=-0.30, known=pre_1h["known"])
        result["pre_4h_direction"] = _direction(pre_4h["return_pct"], up_threshold=0.60, down_threshold=-0.60, known=pre_4h["known"])
        result["trend_regime_1h"] = result["pre_1h_direction"]
        result["trend_regime_4h"] = result["pre_4h_direction"]
        result["volatility_regime_1h"] = _volatility_regime(pre_1h["range_u"], anchor_price, known=pre_1h["known"])
        result["volume_regime_1h"] = _volume_regime(pre_1h["volume"], rolling_1h_volumes or [], known=pre_1h["known"])
        result["distance_to_pre_1h_high_u"] = round(anchor_price - pre_1h["high"], 8) if pre_1h["known"] else 0.0
        result["distance_to_pre_1h_low_u"] = round(anchor_price - pre_1h["low"], 8) if pre_1h["known"] else 0.0

        enhanced_trend = _enhanced_trend_context(bars, anchor_idx, anchor_price, window_stats, interval_sec)
        result.update(enhanced_trend)

        session_stats = _session_stats(bars, anchor_idx, anchor_ts, self.kline_timezone, str(result.get("session_tag") or "UNKNOWN"))
        result["session_open_price"] = session_stats["open"]
        result["session_high"] = session_stats["high"]
        result["session_low"] = session_stats["low"]
        result["distance_to_session_high_u"] = (
            round(anchor_price - session_stats["high"], 8) if session_stats["known"] else 0.0
        )
        result["distance_to_session_low_u"] = (
            round(anchor_price - session_stats["low"], 8) if session_stats["known"] else 0.0
        )
        return result


def _resolve_anchor_ts(row: Mapping[str, Any]) -> float:
    for name in ("forward_anchor_ts", "reaction_event_ts", "frozen_ts", "best_pie_ts", "first_seen_ts"):
        value = parse_float(row.get(name))
        if value > 0:
            return value
    return 0.0


def _empty_context(anchor_ts: float, timezone: str) -> dict[str, Any]:
    return {
        "market_context_anchor_ts": anchor_ts,
        "market_context_anchor_local_time": _local_time(anchor_ts, timezone),
        "pre_15m_return_u": 0.0,
        "pre_15m_return_pct": 0.0,
        "pre_1h_return_u": 0.0,
        "pre_1h_return_pct": 0.0,
        "pre_4h_return_u": 0.0,
        "pre_4h_return_pct": 0.0,
        "is_complete_pre_15m": False,
        "is_complete_pre_1h": False,
        "is_complete_pre_4h": False,
        "pre_15m_range_u": 0.0,
        "pre_1h_range_u": 0.0,
        "pre_4h_range_u": 0.0,
        "pre_15m_volume": 0.0,
        "pre_1h_volume": 0.0,
        "pre_4h_volume": 0.0,
        "pre_1h_direction": "UNKNOWN",
        "pre_4h_direction": "UNKNOWN",
        "trend_regime_1h": "UNKNOWN",
        "trend_regime_4h": "UNKNOWN",
        "ema20_1m": 0.0,
        "ema60_1m": 0.0,
        "ema20_slope_15m_pct": 0.0,
        "ema60_slope_15m_pct": 0.0,
        "trend_efficiency_1h": 0.0,
        "trend_efficiency_4h": 0.0,
        "higher_high_count_1h": 0,
        "higher_low_count_1h": 0,
        "lower_high_count_1h": 0,
        "lower_low_count_1h": 0,
        "price_location_1h": 0.5,
        "price_location_4h": 0.5,
        "trend_score_1h": 0,
        "trend_score_4h": 0,
        "trend_confidence_1h": 0.0,
        "trend_confidence_4h": 0.0,
        "trend_regime_enhanced_1h": "UNKNOWN",
        "trend_regime_enhanced_4h": "UNKNOWN",
        "trend_alignment": "MIXED_OR_UNKNOWN",
        "trend_alignment_score": 0,
        "volatility_regime_1h": "UNKNOWN",
        "volume_regime_1h": "UNKNOWN",
        "distance_to_pre_1h_high_u": 0.0,
        "distance_to_pre_1h_low_u": 0.0,
        "distance_to_session_high_u": 0.0,
        "distance_to_session_low_u": 0.0,
        "session_open_price": 0.0,
        "session_high": 0.0,
        "session_low": 0.0,
    }


def _enhanced_trend_context(
    bars: list[dict[str, float]],
    anchor_idx: int,
    anchor_price: float,
    window_stats: Mapping[str, dict[str, Any]],
    interval_sec: float,
) -> dict[str, Any]:
    completed_bars = bars[: anchor_idx + 1] if anchor_idx >= 0 else []
    closes = [parse_float(bar.get("close")) for bar in completed_bars]
    ema20_series = _ema_series(closes, 20)
    ema60_series = _ema_series(closes, 60)
    ema20 = ema20_series[-1] if len(closes) >= 20 and ema20_series else 0.0
    ema60 = ema60_series[-1] if len(closes) >= 60 and ema60_series else 0.0
    ema_known = ema20 > 0 and ema60 > 0

    result = {
        "ema20_1m": round(ema20, 8),
        "ema60_1m": round(ema60, 8),
        "ema20_slope_15m_pct": _ema_slope_pct(ema20_series, 20, bars_ago=15),
        "ema60_slope_15m_pct": _ema_slope_pct(ema60_series, 60, bars_ago=15),
        "trend_efficiency_1h": _trend_efficiency(bars, anchor_idx, 3600, interval_sec),
        "trend_efficiency_4h": _trend_efficiency(bars, anchor_idx, 14400, interval_sec),
        "higher_high_count_1h": 0,
        "higher_low_count_1h": 0,
        "lower_high_count_1h": 0,
        "lower_low_count_1h": 0,
        "price_location_1h": _price_location(anchor_price, window_stats.get("1h") or {}),
        "price_location_4h": _price_location(anchor_price, window_stats.get("4h") or {}),
        "trend_score_1h": 0,
        "trend_score_4h": 0,
        "trend_confidence_1h": 0.0,
        "trend_confidence_4h": 0.0,
        "trend_regime_enhanced_1h": "UNKNOWN",
        "trend_regime_enhanced_4h": "UNKNOWN",
        "trend_alignment": "MIXED_OR_UNKNOWN",
        "trend_alignment_score": 0,
    }

    structure = _structure_counts_1h(bars, anchor_idx, interval_sec)
    result.update(structure)

    score_1h = _trend_score_1h(result, window_stats)
    score_4h = _trend_score_4h(result, window_stats)
    result["trend_score_1h"] = score_1h
    result["trend_score_4h"] = score_4h

    pre_1h = window_stats.get("1h") or {}
    pre_4h = window_stats.get("4h") or {}
    regime_1h = _enhanced_regime(score_1h, threshold=4, known=bool(pre_1h.get("is_complete")) and ema_known)
    regime_4h = _enhanced_regime(score_4h, threshold=3, known=bool(pre_4h.get("is_complete")) and ema_known)
    result["trend_regime_enhanced_1h"] = regime_1h
    result["trend_regime_enhanced_4h"] = regime_4h
    result["trend_confidence_1h"] = _trend_confidence(score_1h, max_score=8, known=regime_1h != "UNKNOWN")
    result["trend_confidence_4h"] = _trend_confidence(score_4h, max_score=5, known=regime_4h != "UNKNOWN")
    alignment, alignment_score = _trend_alignment(regime_1h, regime_4h)
    result["trend_alignment"] = alignment
    result["trend_alignment_score"] = alignment_score
    return result


def _pre_window_stats(
    bars: list[dict[str, float]],
    anchor_idx: int,
    window_sec: int,
    interval_sec: float,
) -> dict[str, Any]:
    expected_count = max(1, int(round(float(window_sec) / max(interval_sec, 1.0))))
    reference_idx = anchor_idx - expected_count
    if reference_idx < 0:
        return _unknown_window()
    start_close = parse_float(bars[reference_idx].get("close"))
    anchor_close = parse_float(bars[anchor_idx].get("close"))
    if start_close <= 0 or anchor_close <= 0:
        return _unknown_window()
    window = bars[reference_idx + 1 : anchor_idx + 1]
    if len(window) < expected_count:
        return _unknown_window()
    high = max(parse_float(bar.get("high")) for bar in window)
    low = min(parse_float(bar.get("low")) for bar in window)
    volume = sum(parse_float(bar.get("volume")) for bar in window)
    ret_u = anchor_close - start_close
    ret_pct = (ret_u / start_close * 100.0) if start_close else 0.0
    return {
        "known": True,
        "is_complete": True,
        "return_u": round(ret_u, 8),
        "return_pct": round(ret_pct, 8),
        "range_u": round(high - low, 8),
        "volume": round(volume, 8),
        "high": high,
        "low": low,
    }


def _unknown_window() -> dict[str, Any]:
    return {
        "known": False,
        "is_complete": False,
        "return_u": 0.0,
        "return_pct": 0.0,
        "range_u": 0.0,
        "volume": 0.0,
        "high": 0.0,
        "low": 0.0,
    }


def _ema_series(values: list[float], period: int) -> list[float]:
    clean_values = [parse_float(value) for value in values]
    if not clean_values or period <= 0:
        return []
    alpha = 2.0 / (float(period) + 1.0)
    ema = clean_values[0]
    result = []
    for value in clean_values:
        ema = (value * alpha) + (ema * (1.0 - alpha))
        result.append(ema)
    return result


def _ema_slope_pct(series: list[float], period: int, bars_ago: int = 15) -> float:
    if len(series) < period or len(series) <= bars_ago:
        return 0.0
    current = parse_float(series[-1])
    previous = parse_float(series[-1 - bars_ago])
    if previous <= 0:
        return 0.0
    return round((current - previous) / previous * 100.0, 8)


def _trend_efficiency(
    bars: list[dict[str, float]],
    anchor_idx: int,
    window_sec: int,
    interval_sec: float,
) -> float:
    expected_count = max(1, int(round(float(window_sec) / max(interval_sec, 1.0))))
    reference_idx = anchor_idx - expected_count
    if reference_idx < 0:
        return 0.0
    closes = [parse_float(bar.get("close")) for bar in bars[reference_idx : anchor_idx + 1]]
    if len(closes) < expected_count + 1 or any(close <= 0 for close in closes):
        return 0.0
    path = sum(abs(closes[idx] - closes[idx - 1]) for idx in range(1, len(closes)))
    if path <= 0:
        return 0.0
    return round(abs(closes[-1] - closes[0]) / path, 8)


def _structure_counts_1h(
    bars: list[dict[str, float]],
    anchor_idx: int,
    interval_sec: float,
) -> dict[str, int]:
    expected_count = max(1, int(round(3600.0 / max(interval_sec, 1.0))))
    segment_count = 4
    segment_size = expected_count // segment_count
    if segment_size <= 0:
        return _empty_structure_counts()
    start_idx = anchor_idx - expected_count + 1
    if start_idx < 0:
        return _empty_structure_counts()
    window = bars[start_idx : anchor_idx + 1]
    if len(window) < expected_count:
        return _empty_structure_counts()

    segments = []
    for idx in range(segment_count):
        segment = window[idx * segment_size : (idx + 1) * segment_size]
        if len(segment) < segment_size:
            return _empty_structure_counts()
        segments.append(
            {
                "high": max(parse_float(bar.get("high")) for bar in segment),
                "low": min(parse_float(bar.get("low")) for bar in segment),
            }
        )

    counts = _empty_structure_counts()
    for idx in range(1, len(segments)):
        current = segments[idx]
        previous = segments[idx - 1]
        if current["high"] > previous["high"]:
            counts["higher_high_count_1h"] += 1
        if current["low"] > previous["low"]:
            counts["higher_low_count_1h"] += 1
        if current["high"] < previous["high"]:
            counts["lower_high_count_1h"] += 1
        if current["low"] < previous["low"]:
            counts["lower_low_count_1h"] += 1
    return counts


def _empty_structure_counts() -> dict[str, int]:
    return {
        "higher_high_count_1h": 0,
        "higher_low_count_1h": 0,
        "lower_high_count_1h": 0,
        "lower_low_count_1h": 0,
    }


def _price_location(anchor_price: float, stats: Mapping[str, Any]) -> float:
    if not stats.get("known"):
        return 0.5
    high = parse_float(stats.get("high"))
    low = parse_float(stats.get("low"))
    if high <= low:
        return 0.5
    location = (anchor_price - low) / (high - low)
    return round(min(1.0, max(0.0, location)), 8)


def _trend_score_1h(context: Mapping[str, Any], window_stats: Mapping[str, dict[str, Any]]) -> int:
    pre_1h = window_stats.get("1h") or {}
    pre_4h = window_stats.get("4h") or {}
    score = 0
    score += _point(parse_float(pre_1h.get("return_pct")) >= 0.30)
    score += _point(parse_float(pre_4h.get("return_pct")) >= 0.60)
    score += _point(parse_float(context.get("ema20_1m")) > parse_float(context.get("ema60_1m")))
    score += _point(parse_float(context.get("ema20_slope_15m_pct")) > 0.03)
    score += _point(parse_float(context.get("ema60_slope_15m_pct")) > 0.02)
    score += _point(parse_float(context.get("higher_high_count_1h")) >= 2 and parse_float(context.get("higher_low_count_1h")) >= 2)
    score += _point(parse_float(context.get("trend_efficiency_1h")) >= 0.35 and parse_float(pre_1h.get("return_pct")) > 0)
    score += _point(parse_float(context.get("price_location_1h")) >= 0.60)

    score -= _point(parse_float(pre_1h.get("return_pct")) <= -0.30)
    score -= _point(parse_float(pre_4h.get("return_pct")) <= -0.60)
    score -= _point(parse_float(context.get("ema20_1m")) < parse_float(context.get("ema60_1m")))
    score -= _point(parse_float(context.get("ema20_slope_15m_pct")) < -0.03)
    score -= _point(parse_float(context.get("ema60_slope_15m_pct")) < -0.02)
    score -= _point(parse_float(context.get("lower_high_count_1h")) >= 2 and parse_float(context.get("lower_low_count_1h")) >= 2)
    score -= _point(parse_float(context.get("trend_efficiency_1h")) >= 0.35 and parse_float(pre_1h.get("return_pct")) < 0)
    score -= _point(parse_float(context.get("price_location_1h")) <= 0.40)
    return int(score)


def _trend_score_4h(context: Mapping[str, Any], window_stats: Mapping[str, dict[str, Any]]) -> int:
    pre_4h = window_stats.get("4h") or {}
    score = 0
    score += _point(parse_float(pre_4h.get("return_pct")) >= 0.60)
    score += _point(parse_float(context.get("ema20_1m")) > parse_float(context.get("ema60_1m")))
    score += _point(parse_float(context.get("ema60_slope_15m_pct")) > 0.02)
    score += _point(parse_float(context.get("trend_efficiency_4h")) >= 0.25 and parse_float(pre_4h.get("return_pct")) > 0)
    score += _point(parse_float(context.get("price_location_4h")) >= 0.60)

    score -= _point(parse_float(pre_4h.get("return_pct")) <= -0.60)
    score -= _point(parse_float(context.get("ema20_1m")) < parse_float(context.get("ema60_1m")))
    score -= _point(parse_float(context.get("ema60_slope_15m_pct")) < -0.02)
    score -= _point(parse_float(context.get("trend_efficiency_4h")) >= 0.25 and parse_float(pre_4h.get("return_pct")) < 0)
    score -= _point(parse_float(context.get("price_location_4h")) <= 0.40)
    return int(score)


def _point(condition: bool) -> int:
    return 1 if condition else 0


def _enhanced_regime(score: int, threshold: int, known: bool) -> str:
    if not known:
        return "UNKNOWN"
    if score >= threshold:
        return "UP"
    if score <= -threshold:
        return "DOWN"
    return "RANGE"


def _trend_confidence(score: int, max_score: int, known: bool) -> float:
    if not known or max_score <= 0:
        return 0.0
    return round(min(100.0, abs(float(score)) / float(max_score) * 100.0), 8)


def _trend_alignment(regime_1h: str, regime_4h: str) -> tuple[str, int]:
    if regime_1h == regime_4h and regime_1h in {"UP", "DOWN"}:
        return f"ALIGNED_{regime_1h}", 2
    if regime_1h in {"UP", "DOWN"} and regime_4h in {"UP", "DOWN"} and regime_1h != regime_4h:
        return "CONFLICT", -2
    if regime_1h == "RANGE" and regime_4h == "RANGE":
        return "RANGE_ALIGNED", 1
    return "MIXED_OR_UNKNOWN", 0


def _direction(value: float, up_threshold: float, down_threshold: float, known: bool = True) -> str:
    if not known:
        return "UNKNOWN"
    if value >= up_threshold:
        return "UP"
    if value <= down_threshold:
        return "DOWN"
    return "RANGE"


def _volatility_regime(range_u: float, anchor_price: float, known: bool = True) -> str:
    if not known or anchor_price <= 0:
        return "UNKNOWN"
    pct = range_u / anchor_price * 100.0
    if pct >= 1.50:
        return "HIGH_VOL"
    if pct >= 0.70:
        return "MID_VOL"
    return "LOW_VOL"


def _volume_regime(volume: float, rolling_volumes: list[float], known: bool = True) -> str:
    values = sorted(v for v in rolling_volumes if v > 0)
    if not known or not values:
        return "UNKNOWN"
    percentile = sum(1 for value in values if value <= volume) / len(values) * 100.0
    if percentile >= 80.0:
        return "HIGH_VOLUME"
    if percentile >= 40.0:
        return "MID_VOLUME"
    return "LOW_VOLUME"


def _rolling_volumes(bars: list[dict[str, float]], window_sec: int) -> list[float]:
    if not bars:
        return []
    interval = infer_bar_interval_sec(bars)
    expected_count = max(1, int(round(float(window_sec) / max(interval, 1.0))))
    if len(bars) < expected_count:
        return []
    volumes = []
    for idx in range(expected_count - 1, len(bars)):
        window = bars[idx - expected_count + 1 : idx + 1]
        volumes.append(round(sum(parse_float(bar.get("volume")) for bar in window), 8))
    return volumes


def _last_completed_bar_index(bars: list[dict[str, float]], anchor_ts: float, interval_sec: float) -> int:
    if not bars or anchor_ts <= 0:
        return -1
    best_idx = -1
    interval = max(float(interval_sec), 1.0)
    for idx, bar in enumerate(bars):
        if parse_float(bar.get("timestamp")) + interval <= anchor_ts:
            best_idx = idx
        else:
            break
    return best_idx


def _session_stats(
    bars: list[dict[str, float]],
    anchor_idx: int,
    anchor_ts: float,
    timezone: str,
    session_tag: str,
) -> dict[str, Any]:
    if anchor_ts <= 0:
        return {"known": False, "open": 0.0, "high": 0.0, "low": 0.0}
    anchor_dt = datetime.fromtimestamp(anchor_ts, tz=ZoneInfo(str(timezone)))
    rows = []
    completed_bars = bars[: anchor_idx + 1] if anchor_idx >= 0 else []
    for bar in completed_bars:
        ts = parse_float(bar.get("timestamp"))
        local_dt = datetime.fromtimestamp(ts, tz=ZoneInfo(str(timezone)))
        if local_dt.date() != anchor_dt.date():
            continue
        if local_session(ts, timezone).get("session_tag") != session_tag:
            continue
        rows.append(bar)
    if not rows:
        rows = [
            bar
            for bar in completed_bars
            if datetime.fromtimestamp(parse_float(bar.get("timestamp")), tz=ZoneInfo(str(timezone))).date() == anchor_dt.date()
        ]
    if not rows:
        return {"known": False, "open": 0.0, "high": 0.0, "low": 0.0}
    return {
        "known": True,
        "open": parse_float(rows[0].get("open")),
        "high": max(parse_float(bar.get("high")) for bar in rows),
        "low": min(parse_float(bar.get("low")) for bar in rows),
    }


def _local_time(ts: float, timezone: str) -> str:
    if not ts or ts <= 0:
        return ""
    return datetime.fromtimestamp(float(ts), tz=ZoneInfo(str(timezone))).isoformat()
