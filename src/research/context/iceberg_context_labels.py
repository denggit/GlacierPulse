#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import math
import statistics
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from src.research.a1_edge.io_utils import normalize_klines
from src.research.a1_edge.schema import parse_bool, parse_float, parse_int, parse_timestamp


CONTEXT_LABEL_FIELDS = [
    "context_labels_status",
    "iceberg_context_price",
    "iceberg_context_price_source",
    "iceberg_context_side",
    "boll_15m_lower",
    "boll_15m_mid",
    "boll_15m_upper",
    "boll_15m_pct_b",
    "boll_15m_band_width",
    "boll_15m_band_width_pct",
    "boll_15m_band_width_percentile",
    "boll_15m_squeeze_flag",
    "boll_15m_expansion_flag",
    "boll_15m_position",
    "boll_15m_distance_to_lower_u",
    "boll_15m_distance_to_mid_u",
    "boll_15m_distance_to_upper_u",
    "boll_1h_lower",
    "boll_1h_mid",
    "boll_1h_upper",
    "boll_1h_pct_b",
    "boll_1h_band_width",
    "boll_1h_band_width_pct",
    "boll_1h_band_width_percentile",
    "boll_1h_squeeze_flag",
    "boll_1h_expansion_flag",
    "boll_1h_position",
    "boll_1h_distance_to_lower_u",
    "boll_1h_distance_to_mid_u",
    "boll_1h_distance_to_upper_u",
    "vp1h_proxy_poc",
    "vp1h_proxy_val",
    "vp1h_proxy_vah",
    "vp1h_proxy_total_volume",
    "vp1h_proxy_value_area_volume",
    "vp1h_proxy_value_area_ratio",
    "vp1h_proxy_location",
    "vp1h_proxy_nearest_node_type",
    "vp1h_proxy_distance_to_poc_u",
    "vp1h_proxy_distance_to_val_u",
    "vp1h_proxy_distance_to_vah_u",
    "vp1h_proxy_price_percentile",
    "vp1h_proxy_nearest_hvn",
    "vp1h_proxy_nearest_lvn",
    "vp1h_proxy_distance_to_hvn_u",
    "vp1h_proxy_distance_to_lvn_u",
    "vp1h_proxy_near_hvn_flag",
    "vp1h_proxy_near_lvn_flag",
    "vp1h_proxy_node_context",
    "vp1h_proxy_value_area_width_u",
    "vp1h_proxy_value_area_width_pct",
    "vp1h_proxy_value_area_width_atr",
    "vp4h_proxy_poc",
    "vp4h_proxy_val",
    "vp4h_proxy_vah",
    "vp4h_proxy_total_volume",
    "vp4h_proxy_value_area_volume",
    "vp4h_proxy_value_area_ratio",
    "vp4h_proxy_location",
    "vp4h_proxy_nearest_node_type",
    "vp4h_proxy_distance_to_poc_u",
    "vp4h_proxy_distance_to_val_u",
    "vp4h_proxy_distance_to_vah_u",
    "vp4h_proxy_price_percentile",
    "vp4h_proxy_nearest_hvn",
    "vp4h_proxy_nearest_lvn",
    "vp4h_proxy_distance_to_hvn_u",
    "vp4h_proxy_distance_to_lvn_u",
    "vp4h_proxy_near_hvn_flag",
    "vp4h_proxy_near_lvn_flag",
    "vp4h_proxy_node_context",
    "vp4h_proxy_value_area_width_u",
    "vp4h_proxy_value_area_width_pct",
    "vp4h_proxy_value_area_width_atr",
    "vp24h_proxy_poc",
    "vp24h_proxy_val",
    "vp24h_proxy_vah",
    "vp24h_proxy_total_volume",
    "vp24h_proxy_value_area_volume",
    "vp24h_proxy_value_area_ratio",
    "vp24h_proxy_location",
    "vp24h_proxy_nearest_node_type",
    "vp24h_proxy_distance_to_poc_u",
    "vp24h_proxy_distance_to_val_u",
    "vp24h_proxy_distance_to_vah_u",
    "vp24h_proxy_price_percentile",
    "vp24h_proxy_nearest_hvn",
    "vp24h_proxy_nearest_lvn",
    "vp24h_proxy_distance_to_hvn_u",
    "vp24h_proxy_distance_to_lvn_u",
    "vp24h_proxy_near_hvn_flag",
    "vp24h_proxy_near_lvn_flag",
    "vp24h_proxy_node_context",
    "vp24h_proxy_value_area_width_u",
    "vp24h_proxy_value_area_width_pct",
    "vp24h_proxy_value_area_width_atr",
    "vpsession_proxy_poc",
    "vpsession_proxy_val",
    "vpsession_proxy_vah",
    "vpsession_proxy_total_volume",
    "vpsession_proxy_value_area_volume",
    "vpsession_proxy_value_area_ratio",
    "vpsession_proxy_location",
    "vpsession_proxy_nearest_node_type",
    "vpsession_proxy_distance_to_poc_u",
    "vpsession_proxy_distance_to_val_u",
    "vpsession_proxy_distance_to_vah_u",
    "vpsession_proxy_price_percentile",
    "vpsession_proxy_nearest_hvn",
    "vpsession_proxy_nearest_lvn",
    "vpsession_proxy_distance_to_hvn_u",
    "vpsession_proxy_distance_to_lvn_u",
    "vpsession_proxy_near_hvn_flag",
    "vpsession_proxy_near_lvn_flag",
    "vpsession_proxy_node_context",
    "vpsession_proxy_value_area_width_u",
    "vpsession_proxy_value_area_width_pct",
    "vpsession_proxy_value_area_width_atr",
    "local_15m_low_16",
    "local_15m_high_16",
    "previous_local_15m_low_16",
    "previous_local_15m_high_16",
    "local_15m_position_16",
    "near_local_15m_low_flag",
    "near_local_15m_high_flag",
    "sweep_local_15m_low_flag",
    "sweep_local_15m_high_flag",
    "post_sweep_reclaim_15m_post_event_flag",
    "bars_to_sweep_reclaim_15m",
    "sweep_reclaim_level_15m",
    "failed_auction_15m_post_event_flag",
    "sweep_magnitude_15m_u",
    "sweep_magnitude_15m_atr",
    "local_1h_low_12",
    "local_1h_high_12",
    "previous_local_1h_low_12",
    "previous_local_1h_high_12",
    "local_1h_position_12",
    "near_local_1h_low_flag",
    "near_local_1h_high_flag",
    "sweep_local_1h_low_flag",
    "sweep_local_1h_high_flag",
    "post_sweep_reclaim_1h_post_event_flag",
    "bars_to_sweep_reclaim_1h",
    "sweep_reclaim_level_1h",
    "failed_auction_1h_post_event_flag",
    "sweep_magnitude_1h_u",
    "sweep_magnitude_1h_atr",
    "near_local_structure_flag",
    "order_block_15m_type",
    "order_block_15m_low",
    "order_block_15m_high",
    "order_block_15m_distance_u",
    "inside_order_block_15m_flag",
    "near_order_block_15m_flag",
    "order_block_15m_age_bars",
    "order_block_15m_displacement_strength",
    "order_block_15m_invalidated_flag",
    "order_block_15m_fresh_flag",
    "order_block_1h_type",
    "order_block_1h_low",
    "order_block_1h_high",
    "order_block_1h_distance_u",
    "inside_order_block_1h_flag",
    "near_order_block_1h_flag",
    "order_block_1h_age_bars",
    "order_block_1h_displacement_strength",
    "order_block_1h_invalidated_flag",
    "order_block_1h_fresh_flag",
    "vpsession_value_edge_side",
    "vpsession_outside_value_flag",
    "vpsession_reclaim_value_post_event_flag",
    "vpsession_reject_value_post_event_flag",
    "vpsession_bars_to_reclaim",
    "vpsession_reclaim_level",
    "vp24h_value_edge_side",
    "vp24h_outside_value_flag",
    "vp24h_reclaim_value_post_event_flag",
    "vp24h_reject_value_post_event_flag",
    "vp24h_bars_to_reclaim",
    "vp24h_reclaim_level",
    "a3_aggression_score",
    "a3_aggression_quality",
    "a3_body_strength",
    "a3_range_expansion_ratio",
    "a3_volume_zscore",
    "a3_close_location_score",
    "a3_no_quick_return_post_event_flag",
    "a3_failed_quick_return_post_event_flag",
    "a3_taker_imbalance_score",
    "a3_delta_flip_flag",
    "a3_cvd_pressure_score",
    "session_utc",
    "session_elapsed_minutes",
    "session_bucket",
    "is_weekend_flag",
    "book_blocking_liquidity_proxy_flag",
    "book_blocking_liquidity_proxy_strength",
    "visible_depth_proxy_flag",
    "reload_wall_proxy_flag",
    "passive_absorption_proxy_flag",
]


@dataclass(frozen=True)
class IcebergContextConfig:
    timezone: str = "Asia/Shanghai"
    boll_period: int = 20
    boll_std: float = 2.0
    atr_period: int = 14
    vp_bin_size_u: float = 1.0
    vp_value_area_ratio: float = 0.70
    local_threshold_min_u: float = 1.0
    order_block_threshold_min_u: float = 1.0
    ob_swing_15m: int = 8
    ob_swing_1h: int = 6
    value_reclaim_lookahead_bars: int = 3
    sweep_reclaim_lookahead_bars: int = 3


@dataclass
class _BarAggregator:
    interval_sec: int
    current_bucket: int | None = None
    current: dict[str, float] | None = None

    def update(self, bar: Mapping[str, float]) -> dict[str, float] | None:
        ts = float(bar["timestamp"])
        bucket = int(ts // self.interval_sec)
        if self.current_bucket is None or bucket != self.current_bucket:
            self.current_bucket = bucket
            self.current = _bar_copy(bar)
        else:
            assert self.current is not None
            self.current["high"] = max(self.current["high"], float(bar["high"]))
            self.current["low"] = min(self.current["low"], float(bar["low"]))
            self.current["close"] = float(bar["close"])
            self.current["volume"] += float(bar.get("volume", 0.0))
            self.current["timestamp"] = ts

        if _bar_close_ts(bar) < (bucket + 1) * self.interval_sec:
            return None
        completed = dict(self.current or {})
        self.current_bucket = None
        self.current = None
        return completed


class _RollingVolumeProfile:
    def __init__(self, window_sec: float | None, bin_size: float, value_area_ratio: float) -> None:
        self.window_sec = window_sec
        self.bin_size = max(float(bin_size), 0.000001)
        self.value_area_ratio = min(max(float(value_area_ratio), 0.01), 1.0)
        self.entries: deque[tuple[float, dict[float, float]]] = deque()
        self.hist: defaultdict[float, float] = defaultdict(float)
        self.cache = _vp_unavailable()

    def reset(self) -> None:
        self.entries.clear()
        self.hist.clear()
        self.cache = _vp_unavailable()

    def update(self, bar: Mapping[str, float]) -> None:
        ts = _bar_close_ts(bar)
        contrib = _bar_vp_contribution(bar, self.bin_size)
        self.entries.append((ts, contrib))
        for price_bin, volume in contrib.items():
            self.hist[price_bin] += volume
        if self.window_sec is not None:
            expire_before = ts - self.window_sec
            while self.entries and self.entries[0][0] <= expire_before:
                _old_ts, old = self.entries.popleft()
                for price_bin, volume in old.items():
                    self.hist[price_bin] -= volume
                    if self.hist[price_bin] <= 1e-12:
                        self.hist.pop(price_bin, None)
        self.cache = _compute_vp_cache(self.hist, self.value_area_ratio)

    def labels(self, price: float, atr_15m: float, prefix: str, insufficient: bool = False) -> dict[str, Any]:
        if insufficient:
            out = _vp_unavailable()
            out["location"] = "VP_INSUFFICIENT_DATA"
        else:
            out = dict(self.cache)
        near_threshold = max(self.bin_size * 2.0, atr_15m * 0.10) if atr_15m > 0 else self.bin_size * 2.0
        out.update(_classify_vp_price(out, price, near_threshold, self.bin_size, atr_15m))
        return {f"{prefix}_{key}": value for key, value in out.items()}


class ContextCacheSimulator:
    def __init__(
        self,
        klines: Iterable[Mapping[str, Any]],
        candidates: Iterable[Mapping[str, Any]],
        timezone: str = "Asia/Shanghai",
        config: IcebergContextConfig | Mapping[str, Any] | None = None,
    ) -> None:
        cfg = _coerce_config(config, timezone)
        self.config = cfg
        self.klines = normalize_klines(klines, kline_timezone=cfg.timezone)
        self.candidates = [_candidate_context_seed(row) for row in candidates or []]
        self.candidates = [row for row in self.candidates if row]
        self.candidates.sort(key=lambda row: (parse_float(row.get("_context_ts")), str(row.get("_context_key") or "")))
        self._bars_15m: list[dict[str, float]] = []
        self._bars_1h: list[dict[str, float]] = []
        self._agg_15m = _BarAggregator(900)
        self._agg_1h = _BarAggregator(3600)
        self._boll_15m = _boll_unavailable("15m")
        self._boll_1h = _boll_unavailable("1h")
        self._atr_15m = 0.0
        self._atr_1h = 0.0
        self._local_15m = _local_unavailable("15m", 16)
        self._local_1h = _local_unavailable("1h", 12)
        self._ob_15m = {"bullish": None, "bearish": None}
        self._ob_1h = {"bullish": None, "bearish": None}
        self._vp1h = _RollingVolumeProfile(3600, cfg.vp_bin_size_u, cfg.vp_value_area_ratio)
        self._vp4h = _RollingVolumeProfile(14400, cfg.vp_bin_size_u, cfg.vp_value_area_ratio)
        self._vp24h = _RollingVolumeProfile(86400, cfg.vp_bin_size_u, cfg.vp_value_area_ratio)
        self._vpsession = _RollingVolumeProfile(None, cfg.vp_bin_size_u, cfg.vp_value_area_ratio)
        self._session_id = ""
        self._session_start_ts = 0.0
        self._last_1m_bar: dict[str, float] | None = None
        self._recent_1m_bars: deque[dict[str, float]] = deque(maxlen=120)

    def run(self) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        if not self.candidates:
            return result
        if not self.klines:
            for candidate in self.candidates:
                result[str(candidate["_context_key"])] = _status_only("KLINE_UNAVAILABLE", candidate)
            return result

        idx = 0
        last_close_ts = 0.0
        for bar in self.klines:
            close_ts = _bar_close_ts(bar)
            while idx < len(self.candidates) and parse_float(self.candidates[idx].get("_context_ts")) < close_ts:
                candidate = self.candidates[idx]
                result[str(candidate["_context_key"])] = self._label_candidate(candidate)
                idx += 1

            self._update_1m(bar)
            completed_15m = self._agg_15m.update(bar)
            if completed_15m:
                self._update_15m(completed_15m)
            completed_1h = self._agg_1h.update(bar)
            if completed_1h:
                self._update_1h(completed_1h)
            last_close_ts = close_ts

        while idx < len(self.candidates):
            candidate = self.candidates[idx]
            status = "KLINE_OUT_OF_RANGE_AFTER_LAST_BAR" if parse_float(candidate.get("_context_ts")) > last_close_ts else None
            result[str(candidate["_context_key"])] = self._label_candidate(candidate, status_override=status)
            idx += 1
        return result

    def _update_1m(self, bar: Mapping[str, float]) -> None:
        self._ensure_session_for_ts(float(bar["timestamp"]))
        self._last_1m_bar = dict(bar)
        self._recent_1m_bars.append(dict(bar))
        self._vp1h.update(bar)
        self._vp4h.update(bar)
        self._vp24h.update(bar)
        self._vpsession.update(bar)

    def _ensure_session_for_ts(self, ts: float) -> None:
        session_id, session_start = _utc_session(ts)
        if session_id != self._session_id:
            self._session_id = session_id
            self._session_start_ts = session_start
            self._vpsession.reset()

    def _update_15m(self, bar: Mapping[str, float]) -> None:
        self._bars_15m.append(dict(bar))
        self._boll_15m = _compute_boll(self._bars_15m, self.config, "15m")
        self._atr_15m = _compute_atr(self._bars_15m, self.config.atr_period)
        self._local_15m = _compute_local(self._bars_15m, 16, "15m")
        self._update_ob(self._bars_15m, self._ob_15m, self.config.ob_swing_15m)

    def _update_1h(self, bar: Mapping[str, float]) -> None:
        self._bars_1h.append(dict(bar))
        self._boll_1h = _compute_boll(self._bars_1h, self.config, "1h")
        self._atr_1h = _compute_atr(self._bars_1h, self.config.atr_period)
        self._local_1h = _compute_local(self._bars_1h, 12, "1h")
        self._update_ob(self._bars_1h, self._ob_1h, self.config.ob_swing_1h)

    def _update_ob(self, bars: list[dict[str, float]], cache: dict[str, Any], swing_n: int) -> None:
        current_idx = len(bars) - 1
        current = bars[-1]
        for side, ob in list(cache.items()):
            if not ob:
                continue
            if side == "bullish" and float(current["close"]) < parse_float(ob.get("low")):
                ob["invalidated_flag"] = True
            if side == "bearish" and float(current["close"]) > parse_float(ob.get("high")):
                ob["invalidated_flag"] = True
        if len(bars) < swing_n + 2:
            return
        prior = bars[-swing_n - 1:-1]
        previous_swing_high = max(float(bar["high"]) for bar in prior)
        previous_swing_low = min(float(bar["low"]) for bar in prior)
        atr = _compute_atr(bars, self.config.atr_period)
        body = abs(float(current["close"]) - float(current["open"]))
        open_price = float(current["open"])
        pct = (float(current["close"]) / open_price - 1.0) if open_price > 0 else 0.0
        displacement = max(body / atr, abs(pct) / 0.003) if atr > 0 else abs(pct) / 0.003
        bullish = float(current["close"]) > previous_swing_high and (body >= 0.5 * atr or pct >= 0.003)
        bearish = float(current["close"]) < previous_swing_low and (body >= 0.5 * atr or pct <= -0.003)
        search = bars[:-1]
        if bullish:
            candle = next((bar for bar in reversed(search) if float(bar["close"]) < float(bar["open"])), None)
            if candle:
                cache["bullish"] = {"type": "BULLISH_OB", "low": float(candle["low"]), "high": float(candle["high"]), "created_bar_index": current_idx, "displacement_strength": round(displacement, 8), "invalidated_flag": False}
        if bearish:
            candle = next((bar for bar in reversed(search) if float(bar["close"]) > float(bar["open"])), None)
            if candle:
                cache["bearish"] = {"type": "BEARISH_OB", "low": float(candle["low"]), "high": float(candle["high"]), "created_bar_index": current_idx, "displacement_strength": round(displacement, 8), "invalidated_flag": False}

    def _label_candidate(self, candidate: Mapping[str, Any], status_override: str | None = None) -> dict[str, Any]:
        candidate_ts = parse_float(candidate.get("_context_ts"))
        self._ensure_session_for_ts(candidate_ts)
        direction = str(candidate.get("direction") or "").upper()
        price = parse_float(candidate.get("iceberg_context_price"))
        labels = {
            "context_labels_status": status_override or ("SUCCESS" if price > 0 else "CONTEXT_PRICE_UNAVAILABLE"),
            "iceberg_context_price": price,
            "iceberg_context_price_source": str(candidate.get("iceberg_context_price_source") or ""),
            "iceberg_context_side": direction,
        }
        labels.update(_boll_labels(self._boll_15m, price, "15m"))
        labels.update(_boll_labels(self._boll_1h, price, "1h"))
        labels.update(self._vp1h.labels(price, self._atr_15m, "vp1h_proxy"))
        labels.update(self._vp4h.labels(price, self._atr_15m, "vp4h_proxy"))
        labels.update(self._vp24h.labels(price, self._atr_15m, "vp24h_proxy"))
        session_volume = parse_float(self._vpsession.cache.get("total_volume"))
        labels.update(self._vpsession.labels(price, self._atr_15m, "vpsession_proxy", insufficient=session_volume <= 0))
        labels.update(_local_labels(self._local_15m, price, direction, self._atr_15m, "15m", 16))
        labels.update(_local_labels(self._local_1h, price, direction, self._atr_15m, "1h", 12))
        labels["near_local_structure_flag"] = bool(
            labels.get("near_local_15m_low_flag")
            or labels.get("near_local_15m_high_flag")
            or labels.get("near_local_1h_low_flag")
            or labels.get("near_local_1h_high_flag")
        )
        labels.update(_ob_labels(self._ob_15m, price, direction, self._atr_15m, "15m", self.config.order_block_threshold_min_u, len(self._bars_15m) - 1, 32))
        labels.update(_ob_labels(self._ob_1h, price, direction, self._atr_15m, "1h", self.config.order_block_threshold_min_u, len(self._bars_1h) - 1, 24))
        labels.update(_aggression_quality_labels(self._last_1m_bar, list(self._recent_1m_bars), direction))
        labels.update(_session_labels(candidate_ts, self._session_start_ts))
        labels.update(_book_proxy_labels(candidate))
        return labels


def label_iceberg_contexts(
    candidates: Iterable[Mapping[str, Any]],
    klines: Iterable[Mapping[str, Any]],
    config: IcebergContextConfig | Mapping[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    cfg = _coerce_config(config, None)
    return ContextCacheSimulator(klines, candidates, timezone=cfg.timezone, config=cfg).run()


def build_context_summary_rows(rows: Iterable[Mapping[str, Any]], group_fields: list[str], min_count: int = 1) -> list[dict[str, Any]]:
    groups: defaultdict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows or []:
        key = tuple(_group_value(row, field) for field in group_fields)
        groups[key].append(row)
    out = []
    for key, group in groups.items():
        if len(group) < min_count:
            continue
        record = {field: key[idx] for idx, field in enumerate(group_fields)}
        record.update(_context_group_metrics(group))
        out.append(record)
    out.sort(key=_context_summary_sort_key, reverse=True)
    return out


def _candidate_context_seed(row: Mapping[str, Any]) -> dict[str, Any] | None:
    direction = str(row.get("direction") or "").upper()
    if direction not in {"BUY", "SELL"}:
        return None
    record_type = str(row.get("record_type") or "")
    result = str(row.get("result") or "").upper()
    is_candidate_finalized = record_type == "candidate_finalized" and result == "ICEBERG"
    is_zone_truth_iceberg = record_type == "" and parse_int(row.get("iceberg_pie_count")) > 0
    if not is_candidate_finalized and not is_zone_truth_iceberg:
        return None
    price, source = _context_price(row, direction)
    key = _context_key(row)
    ts = _context_ts(row)
    if not key or ts <= 0:
        return None
    seed = dict(row)
    seed.update(
        {
            "_context_key": key,
            "_context_ts": ts,
            "iceberg_context_price": price,
            "iceberg_context_price_source": source,
            "direction": direction,
        }
    )
    return seed


def _context_price(row: Mapping[str, Any], direction: str) -> tuple[float, str]:
    if direction == "BUY":
        fields = (
            "first_iceberg_pie_min_trade_price",
            "first_pie_min_trade_price",
            "min_trade_price",
            "zone_lower",
            "settle_price",
            "trigger_price",
        )
        values = [(parse_float(row.get(field)), field) for field in fields]
        values = [(value, field) for value, field in values if value > 0]
        return min(values, default=(0.0, ""))[0], min(values, default=(0.0, ""))[1]
    fields = (
        "first_iceberg_pie_max_trade_price",
        "first_pie_max_trade_price",
        "max_trade_price",
        "zone_upper",
        "settle_price",
        "trigger_price",
    )
    values = [(parse_float(row.get(field)), field) for field in fields]
    values = [(value, field) for value, field in values if value > 0]
    return max(values, default=(0.0, ""))[0], max(values, default=(0.0, ""))[1]


def _context_key(row: Mapping[str, Any]) -> str:
    for field in ("event_key", "event_id", "iceberg_pie_event_keys", "best_pie_event_key", "zone_id"):
        value = str(row.get(field) or "").strip()
        if value:
            return value.split("|")[0]
    return ""


def _context_ts(row: Mapping[str, Any]) -> float:
    for field in ("settle_ts", "settle_recv_ts", "trigger_ts", "first_iceberg_pie_ts", "best_pie_ts", "first_seen_ts"):
        ts = parse_timestamp(row.get(field))
        if ts > 0:
            return ts
    return 0.0


def _status_only(status: str, candidate: Mapping[str, Any]) -> dict[str, Any]:
    price = parse_float(candidate.get("iceberg_context_price"))
    labels = {field: "" for field in CONTEXT_LABEL_FIELDS}
    labels.update(
        {
            "context_labels_status": status,
            "iceberg_context_price": price,
            "iceberg_context_price_source": str(candidate.get("iceberg_context_price_source") or ""),
            "iceberg_context_side": str(candidate.get("direction") or ""),
            "boll_15m_position": "BOLL_UNAVAILABLE",
            "boll_1h_position": "BOLL_UNAVAILABLE",
        }
    )
    for prefix in ("vp1h_proxy", "vp4h_proxy", "vp24h_proxy", "vpsession_proxy"):
        labels[f"{prefix}_location"] = "VP_UNAVAILABLE"
        labels[f"{prefix}_nearest_node_type"] = "NONE"
    labels.update(_local_unavailable("15m", 16))
    labels.update(_local_unavailable("1h", 12))
    labels.update(_ob_unavailable("15m"))
    labels.update(_ob_unavailable("1h"))
    labels.update(_aggression_quality_labels(None, [], str(candidate.get("direction") or "")))
    labels.update(_session_labels(parse_float(candidate.get("_context_ts")), 0.0))
    labels.update(_book_proxy_labels(candidate))
    return labels


def _coerce_config(config: IcebergContextConfig | Mapping[str, Any] | None, timezone_value: str | None) -> IcebergContextConfig:
    if isinstance(config, IcebergContextConfig):
        if timezone_value and config.timezone != timezone_value:
            return IcebergContextConfig(**{**config.__dict__, "timezone": timezone_value})
        return config
    data = dict(config or {})
    if timezone_value:
        data["timezone"] = timezone_value
    return IcebergContextConfig(**{k: v for k, v in data.items() if k in IcebergContextConfig.__dataclass_fields__})


def _bar_copy(bar: Mapping[str, float]) -> dict[str, float]:
    return {
        "timestamp": float(bar["timestamp"]),
        "open": float(bar["open"]),
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": float(bar["close"]),
        "volume": float(bar.get("volume", 0.0)),
    }


def _compute_boll(bars: list[dict[str, float]], config: IcebergContextConfig, label: str) -> dict[str, Any]:
    if len(bars) < config.boll_period:
        return _boll_unavailable(label)
    def width_at(end: int) -> float:
        sample = bars[end - config.boll_period:end]
        closes = [float(bar["close"]) for bar in sample]
        mid_v = sum(closes) / len(closes)
        std_v = statistics.pstdev(closes) if len(closes) > 1 else 0.0
        return ((mid_v + config.boll_std * std_v) - (mid_v - config.boll_std * std_v)) / mid_v if mid_v > 0 else 0.0
    closes = [float(bar["close"]) for bar in bars[-config.boll_period:]]
    mid = sum(closes) / len(closes)
    std = statistics.pstdev(closes) if len(closes) > 1 else 0.0
    upper = mid + config.boll_std * std
    lower = mid - config.boll_std * std
    width_pct = (upper - lower) / mid if mid > 0 else 0.0
    start = max(config.boll_period, len(bars) - 99)
    history = [width_at(end) for end in range(start, len(bars) + 1)]
    percentile = _percentile_rank(history, width_pct) if history else 0.0
    return {
        "lower": lower, "mid": mid, "upper": upper, "band_width": upper - lower, "band_width_pct": width_pct,
        "band_width_percentile": percentile, "squeeze_flag": percentile <= 0.20, "expansion_flag": percentile >= 0.80,
    }


def _boll_unavailable(label: str) -> dict[str, Any]:
    return {"lower": 0.0, "mid": 0.0, "upper": 0.0, "band_width": 0.0, "band_width_pct": 0.0, "band_width_percentile": 0.0, "squeeze_flag": False, "expansion_flag": False, "label": label}


def _boll_labels(cache: Mapping[str, Any], price: float, label: str) -> dict[str, Any]:
    lower = parse_float(cache.get("lower"))
    mid = parse_float(cache.get("mid"))
    upper = parse_float(cache.get("upper"))
    width = upper - lower
    if price <= 0 or lower <= 0 or upper <= lower:
        pct_b = 0.0
        position = "BOLL_UNAVAILABLE"
    else:
        pct_b = (price - lower) / width
        if pct_b < 0:
            position = "BELOW_LOWER"
        elif pct_b <= 0.05:
            position = "LOWER_TOUCH"
        elif pct_b < 0.45:
            position = "LOWER_TO_MID"
        elif pct_b <= 0.55:
            position = "MID_AREA"
        elif pct_b < 0.95:
            position = "MID_TO_UPPER"
        elif pct_b <= 1.0:
            position = "UPPER_TOUCH"
        else:
            position = "ABOVE_UPPER"
    return {
        f"boll_{label}_lower": round(lower, 8),
        f"boll_{label}_mid": round(mid, 8),
        f"boll_{label}_upper": round(upper, 8),
        f"boll_{label}_pct_b": round(pct_b, 8),
        f"boll_{label}_band_width": round(max(0.0, width), 8),
        f"boll_{label}_band_width_pct": round(parse_float(cache.get("band_width_pct")), 8),
        f"boll_{label}_band_width_percentile": round(parse_float(cache.get("band_width_percentile")), 8),
        f"boll_{label}_squeeze_flag": parse_bool(cache.get("squeeze_flag")),
        f"boll_{label}_expansion_flag": parse_bool(cache.get("expansion_flag")),
        f"boll_{label}_position": position,
        f"boll_{label}_distance_to_lower_u": round(price - lower, 8) if price > 0 and lower > 0 else 0.0,
        f"boll_{label}_distance_to_mid_u": round(price - mid, 8) if price > 0 and mid > 0 else 0.0,
        f"boll_{label}_distance_to_upper_u": round(price - upper, 8) if price > 0 and upper > 0 else 0.0,
    }


def _compute_atr(bars: list[dict[str, float]], period: int) -> float:
    if len(bars) < 2:
        return 0.0
    sample = bars[-max(1, period):]
    trs = []
    for idx, bar in enumerate(sample):
        prev_close = float(sample[idx - 1]["close"]) if idx > 0 else float(bar["open"])
        high = float(bar["high"])
        low = float(bar["low"])
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return round(sum(trs) / len(trs), 8) if trs else 0.0


def _bar_close_ts(bar: Mapping[str, Any]) -> float:
    return float(bar["timestamp"]) + 60.0


def _bar_vp_contribution(bar: Mapping[str, float], bin_size: float) -> dict[float, float]:
    high = parse_float(bar.get("high"))
    low = parse_float(bar.get("low"))
    close = parse_float(bar.get("close"))
    volume = parse_float(bar.get("volume"))
    if volume <= 0:
        return {}
    if high > low:
        start = math.floor(low / bin_size) * bin_size
        end = math.floor(high / bin_size) * bin_size
        count = int(round((end - start) / bin_size)) + 1
        bins = [round(start + i * bin_size, 8) for i in range(max(1, count))]
    else:
        bins = [round(math.floor(close / bin_size) * bin_size, 8)]
    share = volume / len(bins)
    return {price_bin: share for price_bin in bins}


def _compute_vp_cache(hist: Mapping[float, float], value_area_ratio: float) -> dict[str, Any]:
    positive = {float(k): float(v) for k, v in hist.items() if float(v) > 0}
    if not positive:
        return _vp_unavailable()
    total = sum(positive.values())
    ordered_bins = sorted(positive)
    poc = max(ordered_bins, key=lambda key: (positive[key], -abs(key)))
    poc_idx = ordered_bins.index(poc)
    target = total * value_area_ratio
    selected = {poc}
    running = positive[poc]
    lower_idx = poc_idx - 1
    upper_idx = poc_idx + 1
    while running < target and (lower_idx >= 0 or upper_idx < len(ordered_bins)):
        lower_volume = positive[ordered_bins[lower_idx]] if lower_idx >= 0 else -1.0
        upper_volume = positive[ordered_bins[upper_idx]] if upper_idx < len(ordered_bins) else -1.0
        if lower_volume > upper_volume:
            selected.add(ordered_bins[lower_idx])
            running += lower_volume
            lower_idx -= 1
        elif upper_volume > lower_volume:
            selected.add(ordered_bins[upper_idx])
            running += upper_volume
            upper_idx += 1
        else:
            if lower_idx >= 0:
                selected.add(ordered_bins[lower_idx])
                running += lower_volume
                lower_idx -= 1
            if upper_idx < len(ordered_bins) and running < target:
                selected.add(ordered_bins[upper_idx])
                running += upper_volume
                upper_idx += 1
    hvn_threshold = _percentile_value(list(positive.values()), 0.80)
    lvn_threshold = _percentile_value(list(positive.values()), 0.20)
    hvn_bins = [price_bin for price_bin, volume in positive.items() if volume >= hvn_threshold]
    lvn_bins = [price_bin for price_bin, volume in positive.items() if volume <= lvn_threshold]
    return {
        "poc": round(poc, 8),
        "val": round(min(selected), 8),
        "vah": round(max(selected), 8),
        "total_volume": round(total, 8),
        "value_area_volume": round(running, 8),
        "value_area_ratio": round(running / total, 8) if total > 0 else 0.0,
        "location": "VP_UNAVAILABLE",
        "nearest_node_type": "NONE",
        "distance_to_poc_u": 0.0,
        "distance_to_val_u": 0.0,
        "distance_to_vah_u": 0.0,
        "nearest_hvn": 0.0,
        "nearest_lvn": 0.0,
        "distance_to_hvn_u": 0.0,
        "distance_to_lvn_u": 0.0,
        "near_hvn_flag": False,
        "near_lvn_flag": False,
        "node_context": "VP_UNAVAILABLE",
        "value_area_width_u": round(max(0.0, max(selected) - min(selected)), 8),
        "value_area_width_pct": 0.0,
        "value_area_width_atr": 0.0,
        "price_percentile": 0.0,
        "_hist": positive,
        "_hvn_bins": hvn_bins,
        "_lvn_bins": lvn_bins,
    }


def _vp_unavailable() -> dict[str, Any]:
    return {
        "poc": 0.0,
        "val": 0.0,
        "vah": 0.0,
        "total_volume": 0.0,
        "value_area_volume": 0.0,
        "value_area_ratio": 0.0,
        "location": "VP_UNAVAILABLE",
        "nearest_node_type": "NONE",
        "distance_to_poc_u": 0.0,
        "distance_to_val_u": 0.0,
        "distance_to_vah_u": 0.0,
        "nearest_hvn": 0.0,
        "nearest_lvn": 0.0,
        "distance_to_hvn_u": 0.0,
        "distance_to_lvn_u": 0.0,
        "near_hvn_flag": False,
        "near_lvn_flag": False,
        "node_context": "VP_UNAVAILABLE",
        "value_area_width_u": 0.0,
        "value_area_width_pct": 0.0,
        "value_area_width_atr": 0.0,
        "price_percentile": 0.0,
        "_hist": {},
        "_hvn_bins": [],
        "_lvn_bins": [],
    }


def _classify_vp_price(cache: Mapping[str, Any], price: float, threshold: float, bin_size: float, atr_15m: float = 0.0) -> dict[str, Any]:
    poc = parse_float(cache.get("poc"))
    val = parse_float(cache.get("val"))
    vah = parse_float(cache.get("vah"))
    total = parse_float(cache.get("total_volume"))
    if price <= 0 or total <= 0 or poc <= 0:
        return {
            "location": cache.get("location") or "VP_UNAVAILABLE",
            "nearest_node_type": "NONE",
            "node_context": cache.get("node_context") or "VP_UNAVAILABLE",
        }
    hist = dict(cache.get("_hist") or {})
    hvn_bins = list(cache.get("_hvn_bins") or [])
    lvn_bins = list(cache.get("_lvn_bins") or [])
    nearest_hvn = _nearest(price, hvn_bins)
    nearest_lvn = _nearest(price, lvn_bins)
    distances = {
        "distance_to_poc_u": abs(price - poc),
        "distance_to_val_u": abs(price - val),
        "distance_to_vah_u": abs(price - vah),
        "distance_to_hvn_u": abs(price - nearest_hvn) if nearest_hvn > 0 else 0.0,
        "distance_to_lvn_u": abs(price - nearest_lvn) if nearest_lvn > 0 else 0.0,
    }
    near_hvn = nearest_hvn > 0 and distances["distance_to_hvn_u"] <= threshold
    near_lvn = nearest_lvn > 0 and distances["distance_to_lvn_u"] <= threshold
    if distances["distance_to_val_u"] <= threshold:
        location = "NEAR_VAL"
    elif distances["distance_to_vah_u"] <= threshold:
        location = "NEAR_VAH"
    elif distances["distance_to_poc_u"] <= threshold:
        location = "NEAR_POC"
    elif near_lvn:
        location = "NEAR_LVN"
    elif near_hvn:
        location = "NEAR_HVN"
    elif price > vah:
        location = "OUTSIDE_VALUE_ABOVE"
    elif price < val:
        location = "OUTSIDE_VALUE_BELOW"
    elif val <= price <= vah:
        location = "INSIDE_VALUE_AREA"
    else:
        location = "UNKNOWN"
    current_bin = round(math.floor(price / bin_size) * bin_size, 8)
    volumes = sorted(hist.values())
    volume = hist.get(current_bin, 0.0)
    if abs(current_bin - poc) <= 1e-9:
        node = "POC"
    elif not volumes or volume <= 0:
        node = "NONE"
    else:
        percentile = sum(1 for v in volumes if v <= volume) / len(volumes)
        if percentile >= 0.80:
            node = "HVN"
        elif percentile <= 0.20:
            node = "LVN"
        else:
            node = "NONE"
    below = sum(v for k, v in hist.items() if k < current_bin)
    at = hist.get(current_bin, 0.0)
    price_percentile = (below + at * 0.5) / total if total > 0 else 0.0
    width = max(0.0, vah - val)
    return {
        "location": location,
        "nearest_node_type": node,
        "nearest_hvn": round(nearest_hvn, 8),
        "nearest_lvn": round(nearest_lvn, 8),
        "near_hvn_flag": near_hvn,
        "near_lvn_flag": near_lvn,
        "node_context": location,
        "value_area_width_u": round(width, 8),
        "value_area_width_pct": round(width / price, 8) if price > 0 else 0.0,
        "value_area_width_atr": round(width / atr_15m, 8) if atr_15m > 0 else 0.0,
        **{key: round(value, 8) for key, value in distances.items()},
        "price_percentile": round(price_percentile, 8),
    }


def _nearest(price: float, values: Iterable[float]) -> float:
    vals = [parse_float(v) for v in values if parse_float(v) > 0]
    return min(vals, key=lambda value: abs(price - value), default=0.0)


def _percentile_value(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))
    return ordered[idx]


def _percentile_rank(values: list[float], value: float) -> float:
    clean = [float(v) for v in values if float(v) >= 0]
    if not clean:
        return 0.0
    return sum(1 for v in clean if v <= value) / len(clean)


def _utc_session(ts: float) -> tuple[str, float]:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    hour = dt.hour
    if hour < 8:
        start_hour = 0
        name = "ASIA"
    elif hour < 16:
        start_hour = 8
        name = "EUROPE"
    else:
        start_hour = 16
        name = "US"
    start = dt.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    return f"{dt.date()}-{name}", start.timestamp()


def _compute_local(bars: list[dict[str, float]], lookback: int, label: str) -> dict[str, Any]:
    if len(bars) < lookback:
        return _local_unavailable(label, lookback)
    window = bars[-lookback:]
    previous = bars[-lookback - 1:-1] if len(bars) >= lookback + 1 else window
    return {
        f"local_{label}_low_{lookback}": min(float(bar["low"]) for bar in window),
        f"local_{label}_high_{lookback}": max(float(bar["high"]) for bar in window),
        f"previous_local_{label}_low_{lookback}": min(float(bar["low"]) for bar in previous),
        f"previous_local_{label}_high_{lookback}": max(float(bar["high"]) for bar in previous),
    }


def _local_unavailable(label: str, lookback: int) -> dict[str, Any]:
    return {
        f"local_{label}_low_{lookback}": 0.0,
        f"local_{label}_high_{lookback}": 0.0,
        f"previous_local_{label}_low_{lookback}": 0.0,
        f"previous_local_{label}_high_{lookback}": 0.0,
    }


def _local_labels(cache: Mapping[str, Any], price: float, direction: str, atr_15m: float, label: str, lookback: int) -> dict[str, Any]:
    low = parse_float(cache.get(f"local_{label}_low_{lookback}"))
    high = parse_float(cache.get(f"local_{label}_high_{lookback}"))
    prev_low = parse_float(cache.get(f"previous_local_{label}_low_{lookback}"))
    prev_high = parse_float(cache.get(f"previous_local_{label}_high_{lookback}"))
    threshold = max(1.0, atr_15m * 0.10) if atr_15m > 0 else 1.0
    near_low = price > 0 and low > 0 and price <= low + threshold
    near_high = price > 0 and high > 0 and price >= high - threshold
    sweep_low = direction == "BUY" and price > 0 and prev_low > 0 and price < prev_low
    sweep_high = direction == "SELL" and price > 0 and prev_high > 0 and price > prev_high
    if low <= 0 or high <= 0:
        position = "LOCAL_UNAVAILABLE"
    elif near_low:
        position = "NEAR_LOW"
    elif near_high:
        position = "NEAR_HIGH"
    else:
        position = "MID_RANGE"
    return {
        f"local_{label}_low_{lookback}": round(low, 8),
        f"local_{label}_high_{lookback}": round(high, 8),
        f"previous_local_{label}_low_{lookback}": round(prev_low, 8),
        f"previous_local_{label}_high_{lookback}": round(prev_high, 8),
        f"local_{label}_position_{lookback}": position,
        f"near_local_{label}_low_flag": near_low,
        f"near_local_{label}_high_flag": near_high,
        f"sweep_local_{label}_low_flag": sweep_low,
        f"sweep_local_{label}_high_flag": sweep_high,
    }


def _ob_labels(cache: Mapping[str, Any], price: float, direction: str, atr_15m: float, label: str, min_threshold: float, current_index: int = 0, fresh_bars: int = 0) -> dict[str, Any]:
    side = "bullish" if direction == "BUY" else "bearish"
    ob = cache.get(side)
    if not ob:
        return _ob_unavailable(label)
    low = parse_float(ob.get("low"))
    high = parse_float(ob.get("high"))
    if low > high:
        low, high = high, low
    threshold = max(float(min_threshold), atr_15m * 0.10) if atr_15m > 0 else float(min_threshold)
    if low <= price <= high:
        distance = 0.0
        inside = True
    else:
        distance = min(abs(price - low), abs(price - high)) if price > 0 else 0.0
        inside = False
    near = bool(inside or (price > 0 and distance <= threshold))
    age = max(0, int(current_index) - int(parse_float(ob.get("created_bar_index"))))
    invalidated = parse_bool(ob.get("invalidated_flag"))
    return {
        f"order_block_{label}_type": str(ob.get("type") or "OB_UNAVAILABLE"),
        f"order_block_{label}_low": round(low, 8),
        f"order_block_{label}_high": round(high, 8),
        f"order_block_{label}_distance_u": round(distance, 8),
        f"inside_order_block_{label}_flag": inside,
        f"near_order_block_{label}_flag": near,
        f"order_block_{label}_age_bars": age,
        f"order_block_{label}_displacement_strength": round(parse_float(ob.get("displacement_strength")), 8),
        f"order_block_{label}_invalidated_flag": invalidated,
        f"order_block_{label}_fresh_flag": bool(fresh_bars and age <= fresh_bars and not invalidated),
    }


def _ob_unavailable(label: str) -> dict[str, Any]:
    return {
        f"order_block_{label}_type": "OB_UNAVAILABLE",
        f"order_block_{label}_low": 0.0,
        f"order_block_{label}_high": 0.0,
        f"order_block_{label}_distance_u": 0.0,
        f"inside_order_block_{label}_flag": False,
        f"near_order_block_{label}_flag": False,
        f"order_block_{label}_age_bars": 0,
        f"order_block_{label}_displacement_strength": 0.0,
        f"order_block_{label}_invalidated_flag": False,
        f"order_block_{label}_fresh_flag": False,
    }



def _aggression_quality_labels(bar: Mapping[str, Any] | None, recent_bars: list[Mapping[str, Any]], direction: str) -> dict[str, Any]:
    if not bar:
        return {
            "a3_aggression_score": 0.0, "a3_aggression_quality": "UNAVAILABLE", "a3_body_strength": 0.0,
            "a3_range_expansion_ratio": 0.0, "a3_volume_zscore": 0.0, "a3_close_location_score": 0.0,
            "a3_no_quick_return_post_event_flag": False, "a3_failed_quick_return_post_event_flag": False,
            "a3_taker_imbalance_score": 0.0, "a3_delta_flip_flag": False, "a3_cvd_pressure_score": 0.0,
        }
    high = parse_float(bar.get("high")); low = parse_float(bar.get("low")); open_ = parse_float(bar.get("open")); close = parse_float(bar.get("close"))
    rng = max(high - low, 0.0)
    body_strength = min(abs(close - open_) / rng, 1.0) if rng > 0 else 0.0
    if direction == "BUY" and rng > 0:
        close_loc = (close - low) / rng
    elif direction == "SELL" and rng > 0:
        close_loc = (high - close) / rng
    else:
        close_loc = 0.0
    ranges = [max(parse_float(b.get("high")) - parse_float(b.get("low")), 0.0) for b in recent_bars[:-1]][-20:]
    avg_range = sum(ranges) / len(ranges) if ranges else 0.0
    range_ratio = rng / avg_range if avg_range > 0 else 0.0
    vols = [parse_float(b.get("volume")) for b in recent_bars[:-1]][-20:]
    avg_vol = sum(vols) / len(vols) if vols else 0.0
    std_vol = statistics.pstdev(vols) if len(vols) > 1 else 0.0
    volume = parse_float(bar.get("volume"))
    vol_z = max(0.0, (volume - avg_vol) / std_vol) if std_vol > 0 else (1.0 if avg_vol > 0 and volume > avg_vol else 0.0)
    score = 0.30 * body_strength + 0.25 * close_loc + 0.25 * min(vol_z / 3.0, 1.0) + 0.20 * min(range_ratio / 2.0, 1.0)
    if score >= 0.75:
        quality = "STRONG"
    elif score >= 0.50:
        quality = "MEDIUM"
    elif score > 0:
        quality = "WEAK"
    else:
        quality = "UNAVAILABLE"
    taker_buy = parse_float(bar.get("taker_buy_volume")) or parse_float(bar.get("takerBuyVolume"))
    taker_sell = parse_float(bar.get("taker_sell_volume")) or parse_float(bar.get("takerSellVolume"))
    denom = taker_buy + taker_sell
    imbalance = (taker_buy - taker_sell) / denom if denom > 0 else 0.0
    return {
        "a3_aggression_score": round(score, 8), "a3_aggression_quality": quality,
        "a3_body_strength": round(body_strength, 8), "a3_range_expansion_ratio": round(range_ratio, 8),
        "a3_volume_zscore": round(vol_z, 8), "a3_close_location_score": round(close_loc, 8),
        "a3_no_quick_return_post_event_flag": False, "a3_failed_quick_return_post_event_flag": False,
        "a3_taker_imbalance_score": round(imbalance, 8), "a3_delta_flip_flag": False, "a3_cvd_pressure_score": 0.0,
    }


def _session_labels(ts: float, session_start_ts: float) -> dict[str, Any]:
    if ts <= 0:
        return {"session_utc": "UNKNOWN", "session_elapsed_minutes": 0.0, "session_bucket": "UNKNOWN", "is_weekend_flag": False}
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    session, start = _utc_session(ts)
    session_name = session.split("-")[-1]
    start_ts = session_start_ts if session_start_ts > 0 else start
    elapsed = max(0.0, (ts - start_ts) / 60.0)
    part = "EARLY" if elapsed < 160 else ("MID" if elapsed < 320 else "LATE")
    return {
        "session_utc": session_name,
        "session_elapsed_minutes": round(elapsed, 8),
        "session_bucket": f"{session_name}_{part}" if session_name in {"ASIA", "EUROPE", "US"} else "UNKNOWN",
        "is_weekend_flag": dt.weekday() >= 5,
    }

def _book_proxy_labels(row: Mapping[str, Any]) -> dict[str, Any]:
    usable = False
    local_depth_usdt = parse_float(row.get("local_depth_usdt"))
    local_depth = parse_float(row.get("local_depth"))
    if local_depth_usdt <= 0 and local_depth > 0:
        local_depth_usdt = local_depth
    reload_count = parse_int(row.get("zone_v2_reload_level_count"))
    absorption = max(parse_float(row.get("absorption_rate")), parse_float(row.get("avg_absorption_rate")), parse_float(row.get("max_absorption_rate")))
    hidden_notional = max(parse_float(row.get("hidden_notional")), parse_float(row.get("sum_hidden_notional")), parse_float(row.get("max_hidden_notional")))
    hidden_volume = max(parse_float(row.get("hidden_volume")), parse_float(row.get("sum_hidden_volume")), parse_float(row.get("max_hidden_volume")))
    active = max(parse_float(row.get("active_notional")), parse_float(row.get("sum_active_notional")), parse_float(row.get("max_active_notional")))
    for value in (local_depth_usdt, reload_count, absorption, hidden_notional, hidden_volume, active):
        usable = usable or value > 0
    visible = local_depth_usdt >= 1_000_000
    reload = reload_count >= 2 or (absorption >= 0.80 and hidden_notional >= 1_500_000)
    passive = absorption >= 0.80 and active >= 1_000_000
    flag = visible or reload or passive
    if not usable:
        strength = "UNAVAILABLE"
    elif visible and reload and passive:
        strength = "STRONG"
    elif (reload and passive) or (visible and (reload or passive)):
        strength = "MEDIUM"
    elif flag:
        strength = "WEAK"
    else:
        strength = "NONE"
    return {
        "book_blocking_liquidity_proxy_flag": flag,
        "book_blocking_liquidity_proxy_strength": strength,
        "visible_depth_proxy_flag": visible,
        "reload_wall_proxy_flag": reload,
        "passive_absorption_proxy_flag": passive,
    }


def _group_value(row: Mapping[str, Any], field: str) -> Any:
    value = row.get(field)
    if value in (None, ""):
        return "UNKNOWN"
    return value


def _context_group_metrics(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    realized_values = _values(rows, "a3_after_a2_realized_r_proxy_1h") or _values(rows, "a3_preview_realized_r_proxy_1h")
    fee_field = "a3_after_a2_fee_positive_1h" if any(row.get("a3_after_a2_fee_positive_1h") not in (None, "") for row in rows) else "a3_structural_fee_positive_1h"
    return {
        "zone_count": len(rows),
        "iceberg_zone_count": sum(1 for row in rows if parse_int(row.get("iceberg_pie_count")) > 0 or str(row.get("result") or "").upper() == "ICEBERG"),
        "avg_truth_score": _avg(rows, "truth_score_avg", "truth_score_max"),
        "median_truth_score": _median(rows, "truth_score_avg", "truth_score_max"),
        "a2_pre_pool_count": sum(1 for row in rows if parse_bool(row.get("a2_pre_pool_eligible"))),
        "a2_ready_count": sum(1 for row in rows if parse_bool(row.get("a2_ready_for_a3_watch_flag")) or parse_bool(row.get("a2_validated_candidate_flag"))),
        "a3_count": sum(1 for row in rows if _has_a3(row)),
        "avg_mfe_r": _avg(rows, "a3_after_a2_net_mfe_1h_r", "a3_preview_net_mfe_1h_r"),
        "avg_mae_r": _avg(rows, "a3_after_a2_net_mae_1h_r", "a3_preview_net_mae_1h_r"),
        "fee_positive_rate": _rate(rows, lambda row: parse_bool(row.get(fee_field)) or parse_float(row.get("a3_after_a2_realized_r_proxy_1h")) > 0 or parse_float(row.get("a3_preview_realized_r_proxy_1h")) > 0),
        "avg_realized_r_proxy": round(sum(realized_values) / len(realized_values), 8) if realized_values else 0.0,
        "median_realized_r_proxy": round(statistics.median(realized_values), 8) if realized_values else 0.0,
    }


def _context_summary_sort_key(row: Mapping[str, Any]) -> tuple[float, float, float, float]:
    return (
        parse_float(row.get("avg_realized_r_proxy")),
        parse_float(row.get("fee_positive_rate")),
        parse_float(row.get("avg_truth_score")),
        parse_float(row.get("zone_count")),
    )


def _has_a3(row: Mapping[str, Any]) -> bool:
    typ = str(row.get("a3_aggression_type_v2") or "").upper()
    return typ not in {"", "UNKNOWN", "NO_AGGRESSION", "PRICE_BREAKOUT_WEAK"} or parse_bool(row.get("a3_preview_breakout_after_a2_flag"))


def _values(rows: list[Mapping[str, Any]], *fields: str) -> list[float]:
    values = []
    for row in rows:
        for field in fields:
            if row.get(field) not in (None, ""):
                values.append(parse_float(row.get(field)))
                break
    return values


def _avg(rows: list[Mapping[str, Any]], *fields: str) -> float:
    values = _values(rows, *fields)
    return round(sum(values) / len(values), 8) if values else 0.0


def _median(rows: list[Mapping[str, Any]], *fields: str) -> float:
    values = _values(rows, *fields)
    return round(statistics.median(values), 8) if values else 0.0


def _rate(rows: list[Mapping[str, Any]], predicate) -> float:
    return round(sum(1 for row in rows if predicate(row)) / len(rows), 8) if rows else 0.0
