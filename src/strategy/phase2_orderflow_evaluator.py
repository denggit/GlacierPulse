#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
V6.2 Phase2 orderflow research evaluator.

This module is research-only. It tracks lightweight tick/orderflow aggregates
for frozen A1 zones and never places orders or calls the trading path.
"""

import logging
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Iterable, Optional, Tuple

from config.research_evaluator import (
    BOOK_NEAR_SWEEP_RANGE_USDT,
    BOOK_NEAR_ZONE_RANGE_USDT,
    MAX_ACTIVE_PHASE2_ZONES,
    PHASE2_MAX_SWEEP_DEPTH_PCT_HARD,
    PHASE2_MAX_SWEEP_DEPTH_PCT_SOFT,
    PHASE2_MAX_TIME_ABOVE_MS,
    PHASE2_MAX_TIME_BELOW_MS,
    PHASE2_MIN_ABSORPTION_SCORE,
    PHASE2_MIN_ACTIVE_NOTIONAL_3S,
    PHASE2_MIN_RECLAIM_SCORE,
    PHASE2_MIN_RELOAD_SCORE,
    PHASE2_MIN_RETEST_SCORE,
    PHASE2_MIN_TOTAL_SCORE,
    PHASE2_RECLAIM_BUFFER_USDT,
    PHASE2_RETEST_BUFFER_USDT,
    PHASE2_STATE_LOG_MIN_INTERVAL_SEC,
    PHASE2_TEST_ZONE_BUFFER_USDT,
    PHASE2_ZONE_TTL_SECONDS,
)

logger = logging.getLogger(__name__)


@dataclass
class Phase2FlowBucket:
    bucket_ts: int
    active_buy_notional: float = 0.0
    active_sell_notional: float = 0.0
    tick_count: int = 0


@dataclass
class Phase2BookSample:
    ts: float
    bid_depth_near_zone: float = 0.0
    ask_depth_near_zone: float = 0.0
    bid_depth_near_sweep: float = 0.0
    ask_depth_near_sweep: float = 0.0


@dataclass
class Phase2TrackedZone:
    zone_id: str
    direction: str
    frozen_ts: float
    frozen_low: float
    frozen_high: float
    zone_mid: float
    live_low: float
    live_high: float
    state: str = "PHASE2_WAITING"
    previous_state: str = ""
    state_updated_ts: float = 0.0
    state_entered_ts: float = 0.0
    testing_zone_ts: float = 0.0
    sweep_started_ts: float = 0.0
    sweep_reclaimed_ts: float = 0.0
    retest_started_ts: float = 0.0
    confirmed_ts: float = 0.0
    failed_ts: float = 0.0
    timeout_ts: float = 0.0
    has_tested_zone: bool = False
    has_swept_boundary: bool = False
    has_absorbed_after_sweep: bool = False
    has_reclaimed_boundary: bool = False
    has_retested_inside_zone: bool = False
    has_failed: bool = False
    has_confirmed: bool = False
    time_below_boundary_ms: float = 0.0
    time_above_boundary_ms: float = 0.0
    absorption_score: float = 0.0
    pressure_decay_score: float = 0.0
    reclaim_score: float = 0.0
    retest_score: float = 0.0
    phase2_total_score: float = 0.0
    phase2_type: str = "UNKNOWN_RESEARCH"
    phase2_reason: str = ""
    sweep_extreme: float = 0.0
    last_price: float = 0.0
    min_price_seen_after_frozen: float = 0.0
    max_price_seen_after_frozen: float = 0.0
    active_buy_notional_1s: float = 0.0
    active_buy_notional_3s: float = 0.0
    active_buy_notional_10s: float = 0.0
    active_sell_notional_1s: float = 0.0
    active_sell_notional_3s: float = 0.0
    active_sell_notional_10s: float = 0.0
    cvd_delta_3s: float = 0.0
    cvd_delta_10s: float = 0.0
    tick_count_1s: int = 0
    tick_count_3s: int = 0
    tick_count_10s: int = 0
    last_state_log_ts: float = 0.0
    break_depth_u: float = 0.0
    break_depth_pct: float = 0.0
    phase2_registered_ts: float = 0.0
    book_update_count: int = 0
    bid_depth_near_zone: float = 0.0
    ask_depth_near_zone: float = 0.0
    bid_depth_near_sweep: float = 0.0
    ask_depth_near_sweep: float = 0.0
    bid_reload_count: int = 0
    ask_reload_count: int = 0
    bid_reduction_1s: float = 0.0
    ask_reduction_1s: float = 0.0
    book_absorption_score: float = 0.0
    relevant_book_depth_available: bool = False
    reload_score: float = 0.0
    last_book_ts: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict, repr=False)
    flow_buckets: Deque[Phase2FlowBucket] = field(default_factory=deque, repr=False)
    book_samples: Deque[Phase2BookSample] = field(default_factory=deque, repr=False)
    bid_reload_watch_active: bool = field(default=False, repr=False)
    bid_reload_low: float = field(default=0.0, repr=False)
    bid_reload_recover_target: float = field(default=0.0, repr=False)
    ask_reload_watch_active: bool = field(default=False, repr=False)
    ask_reload_low: float = field(default=0.0, repr=False)
    ask_reload_recover_target: float = field(default=0.0, repr=False)
    previous_break_depth_u: float = field(default=0.0, repr=False)
    previous_pressure_notional_3s: float = field(default=0.0, repr=False)

    def to_snapshot(self) -> Dict[str, Any]:
        snapshot = dict(self.metadata)
        snapshot.update(
            {
                "zone_id": self.zone_id,
                "direction": self.direction,
                "frozen_ts": self.frozen_ts,
                "frozen_low": self.frozen_low,
                "frozen_high": self.frozen_high,
                "zone_mid": self.zone_mid,
                "live_low": self.live_low,
                "live_high": self.live_high,
                "state": self.state,
                "previous_state": self.previous_state,
                "state_updated_ts": self.state_updated_ts,
                "state_entered_ts": self.state_entered_ts,
                "testing_zone_ts": self.testing_zone_ts,
                "sweep_started_ts": self.sweep_started_ts,
                "sweep_reclaimed_ts": self.sweep_reclaimed_ts,
                "retest_started_ts": self.retest_started_ts,
                "confirmed_ts": self.confirmed_ts,
                "failed_ts": self.failed_ts,
                "timeout_ts": self.timeout_ts,
                "has_tested_zone": self.has_tested_zone,
                "has_swept_boundary": self.has_swept_boundary,
                "has_absorbed_after_sweep": self.has_absorbed_after_sweep,
                "has_reclaimed_boundary": self.has_reclaimed_boundary,
                "has_retested_inside_zone": self.has_retested_inside_zone,
                "has_failed": self.has_failed,
                "has_confirmed": self.has_confirmed,
                "time_below_boundary_ms": self.time_below_boundary_ms,
                "time_above_boundary_ms": self.time_above_boundary_ms,
                "absorption_score": self.absorption_score,
                "pressure_decay_score": self.pressure_decay_score,
                "reclaim_score": self.reclaim_score,
                "retest_score": self.retest_score,
                "phase2_total_score": self.phase2_total_score,
                "phase2_type": self.phase2_type,
                "phase2_reason": self.phase2_reason,
                "sweep_extreme": self.sweep_extreme,
                "last_price": self.last_price,
                "min_price_seen_after_frozen": self.min_price_seen_after_frozen,
                "max_price_seen_after_frozen": self.max_price_seen_after_frozen,
                "active_buy_notional_1s": self.active_buy_notional_1s,
                "active_buy_notional_3s": self.active_buy_notional_3s,
                "active_buy_notional_10s": self.active_buy_notional_10s,
                "active_sell_notional_1s": self.active_sell_notional_1s,
                "active_sell_notional_3s": self.active_sell_notional_3s,
                "active_sell_notional_10s": self.active_sell_notional_10s,
                "cvd_delta_3s": self.cvd_delta_3s,
                "cvd_delta_10s": self.cvd_delta_10s,
                "tick_count_1s": self.tick_count_1s,
                "tick_count_3s": self.tick_count_3s,
                "tick_count_10s": self.tick_count_10s,
                "last_state_log_ts": self.last_state_log_ts,
                "break_depth_u": self.break_depth_u,
                "break_depth_pct": self.break_depth_pct,
                "phase2_registered_ts": self.phase2_registered_ts,
                "book_update_count": self.book_update_count,
                "bid_depth_near_zone": self.bid_depth_near_zone,
                "ask_depth_near_zone": self.ask_depth_near_zone,
                "bid_depth_near_sweep": self.bid_depth_near_sweep,
                "ask_depth_near_sweep": self.ask_depth_near_sweep,
                "bid_reload_count": self.bid_reload_count,
                "ask_reload_count": self.ask_reload_count,
                "bid_reduction_1s": self.bid_reduction_1s,
                "ask_reduction_1s": self.ask_reduction_1s,
                "book_absorption_score": self.book_absorption_score,
                "relevant_book_depth_available": self.relevant_book_depth_available,
                "reload_score": self.reload_score,
                "last_book_ts": self.last_book_ts,
            }
        )
        return snapshot


class Phase2OrderflowEvaluator:
    RELOAD_MIN_DEPTH_USDT = 10_000.0
    RELOAD_MIN_DEPTH_PCT = 0.10
    RELOAD_RECOVER_PCT = 0.70

    def __init__(
        self,
        max_active_zones: int = MAX_ACTIVE_PHASE2_ZONES,
        zone_ttl_seconds: float = PHASE2_ZONE_TTL_SECONDS,
        book_near_zone_range_usdt: float = BOOK_NEAR_ZONE_RANGE_USDT,
        book_near_sweep_range_usdt: float = BOOK_NEAR_SWEEP_RANGE_USDT,
    ):
        self.max_active_zones = max(1, int(max_active_zones))
        self.zone_ttl_seconds = max(0.0, float(zone_ttl_seconds))
        self.book_near_zone_range_usdt = max(0.0, float(book_near_zone_range_usdt))
        self.book_near_sweep_range_usdt = max(0.0, float(book_near_sweep_range_usdt))
        self.test_zone_buffer_usdt = max(0.0, float(PHASE2_TEST_ZONE_BUFFER_USDT))
        self.reclaim_buffer_usdt = max(0.0, float(PHASE2_RECLAIM_BUFFER_USDT))
        self.retest_buffer_usdt = max(0.0, float(PHASE2_RETEST_BUFFER_USDT))
        self.min_active_notional_3s = max(0.0, float(PHASE2_MIN_ACTIVE_NOTIONAL_3S))
        self.min_absorption_score = max(0.0, float(PHASE2_MIN_ABSORPTION_SCORE))
        self.min_reload_score = max(0.0, float(PHASE2_MIN_RELOAD_SCORE))
        self.min_reclaim_score = max(0.0, float(PHASE2_MIN_RECLAIM_SCORE))
        self.min_retest_score = max(0.0, float(PHASE2_MIN_RETEST_SCORE))
        self.min_total_score = max(0.0, float(PHASE2_MIN_TOTAL_SCORE))
        self.max_sweep_depth_pct_soft = max(0.0, float(PHASE2_MAX_SWEEP_DEPTH_PCT_SOFT))
        self.max_sweep_depth_pct_hard = max(0.0, float(PHASE2_MAX_SWEEP_DEPTH_PCT_HARD))
        self.max_time_below_ms = max(0.0, float(PHASE2_MAX_TIME_BELOW_MS))
        self.max_time_above_ms = max(0.0, float(PHASE2_MAX_TIME_ABOVE_MS))
        self.state_log_min_interval_sec = max(0.0, float(PHASE2_STATE_LOG_MIN_INTERVAL_SEC))
        self.active_zones: "OrderedDict[str, Phase2TrackedZone]" = OrderedDict()

    def register_frozen_zone(self, zone: Dict[str, Any], now_ts: Optional[float] = None) -> bool:
        """
        Register one public frozen iceberg zone.

        Returns True only when a new zone_id is accepted. Duplicate zone_id
        inputs are ignored silently to preserve one [PHASE2-REGISTERED] log.
        """
        try:
            return self._register_frozen_zone(zone=zone, now_ts=now_ts)
        except Exception:
            logger.exception(
                "[PHASE2-REGISTER-FAILED] zone_id=%s",
                zone.get("zone_id") if isinstance(zone, dict) else None,
            )
            return False

    def on_trade(self, trade_data: Dict[str, Any]) -> None:
        """Update active zone rolling windows from one trade tick."""
        try:
            self._on_trade(trade_data)
        except Exception:
            logger.exception("[PHASE2-ORDERFLOW-FAILED] source=trade")

    def on_price(self, price: float, ts: Optional[float] = None) -> None:
        """Update active zone price extremes without adding orderflow volume."""
        try:
            now = float(ts if ts is not None else time.time())
            self._prune(now_ts=now)
            price_value = self._safe_float(price, 0.0)
            if price_value <= 0:
                return
            for zone in list(self.active_zones.values()):
                if now >= zone.frozen_ts:
                    self._update_zone_price(zone=zone, price=price_value)
                    self._recompute_windows(zone=zone, now_ts=now)
                    self._evaluate_state_machine(zone=zone, now_ts=now)
        except Exception:
            logger.exception("[PHASE2-PRICE-FAILED]")

    def on_book_update(self, book_data: Dict[str, Any]) -> None:
        """Update active zone local book aggregates from one book snapshot."""
        try:
            self._on_book_update(book_data)
        except Exception:
            logger.exception("[PHASE2-BOOK-FAILED]")

    def on_orderflow(self, data: Dict[str, Any]) -> None:
        """Update from trade-like or pre-aggregated orderflow data."""
        try:
            if not isinstance(data, dict):
                return
            if self._has_trade_fields(data):
                self._on_trade(data)
                return

            now = self._extract_ts(data)
            self._prune(now_ts=now)
            price = self._extract_price(data)
            buy_notional = self._safe_float(
                data.get("active_buy_notional", data.get("buy_notional")),
                0.0,
            )
            sell_notional = self._safe_float(
                data.get("active_sell_notional", data.get("sell_notional")),
                0.0,
            )
            tick_count = self._safe_int(data.get("tick_count"), 0)

            if price <= 0 and buy_notional <= 0 and sell_notional <= 0 and tick_count <= 0:
                return

            for zone in list(self.active_zones.values()):
                if now < zone.frozen_ts:
                    continue
                if price > 0:
                    self._update_zone_price(zone=zone, price=price)
                if buy_notional > 0 or sell_notional > 0 or tick_count > 0:
                    self._add_flow_to_zone(
                        zone=zone,
                        ts=now,
                        active_buy_notional=max(0.0, buy_notional),
                        active_sell_notional=max(0.0, sell_notional),
                        tick_count=max(0, tick_count),
                    )
                else:
                    self._recompute_windows(zone=zone, now_ts=now)
                self._evaluate_state_machine(zone=zone, now_ts=now)
        except Exception:
            logger.exception("[PHASE2-ORDERFLOW-FAILED] source=orderflow")

    def get_active_zone(self, zone_id: str) -> Optional[Dict[str, Any]]:
        zone = self.active_zones.get(str(zone_id))
        return zone.to_snapshot() if zone else None

    def debug_snapshot(self, zone_id: Optional[str] = None) -> Any:
        """Return a lightweight debug snapshot. Call manually; it does not log."""
        try:
            if zone_id is not None:
                return self.get_active_zone(str(zone_id))
            return [zone.to_snapshot() for zone in self.active_zones.values()]
        except Exception:
            logger.exception("[PHASE2-SNAPSHOT-FAILED]")
            return None if zone_id is not None else []

    def _register_frozen_zone(self, zone: Dict[str, Any], now_ts: Optional[float] = None) -> bool:
        if not isinstance(zone, dict) or not zone.get("is_frozen"):
            return False

        zone_id = str(zone.get("zone_id") or "").strip()
        if not zone_id or zone_id in self.active_zones:
            return False

        now = float(now_ts if now_ts is not None else time.time())
        self._prune(now_ts=now, reserve_slots=1)

        tracked_zone = self._build_tracked_zone(zone=zone, now_ts=now)
        self.active_zones[zone_id] = tracked_zone
        self.active_zones.move_to_end(zone_id)

        logger.info(
            "[PHASE2-REGISTERED] zone_id=%s direction=%s frozen_ts=%.3f frozen_reason=%s frozen_state=%s frozen_event_id=%s frozen_low=%.2f frozen_high=%.2f live_low=%.2f live_high=%.2f",
            zone_id,
            tracked_zone.direction,
            tracked_zone.frozen_ts,
            tracked_zone.metadata.get("frozen_reason"),
            tracked_zone.metadata.get("frozen_state"),
            tracked_zone.metadata.get("frozen_event_id"),
            tracked_zone.frozen_low,
            tracked_zone.frozen_high,
            tracked_zone.live_low,
            tracked_zone.live_high,
        )
        return True

    def _build_tracked_zone(self, zone: Dict[str, Any], now_ts: float) -> Phase2TrackedZone:
        frozen_low = self._safe_float(
            zone.get("frozen_zone_lower", zone.get("zone_lower")),
            0.0,
        )
        frozen_high = self._safe_float(
            zone.get("frozen_zone_upper", zone.get("zone_upper")),
            0.0,
        )
        live_low = self._safe_float(
            zone.get("live_zone_lower", zone.get("zone_lower", frozen_low)),
            frozen_low,
        )
        live_high = self._safe_float(
            zone.get("live_zone_upper", zone.get("zone_upper", frozen_high)),
            frozen_high,
        )
        zone_mid = (frozen_low + frozen_high) / 2.0 if frozen_low > 0 and frozen_high > 0 else 0.0
        direction = str(zone.get("direction") or "").upper()

        return Phase2TrackedZone(
            zone_id=str(zone.get("zone_id") or "").strip(),
            direction=direction,
            frozen_ts=self._safe_float(zone.get("frozen_ts"), now_ts),
            frozen_low=frozen_low,
            frozen_high=frozen_high,
            zone_mid=zone_mid,
            live_low=live_low,
            live_high=live_high,
            state_updated_ts=now_ts,
            state_entered_ts=now_ts,
            phase2_registered_ts=now_ts,
            metadata=dict(zone),
        )

    def _on_book_update(self, book_data: Dict[str, Any]) -> None:
        if not self.active_zones or not isinstance(book_data, dict):
            return

        bids = self._extract_book_levels(book_data.get("bids"))
        asks = self._extract_book_levels(book_data.get("asks"))
        if bids is None or asks is None:
            return

        now = self._extract_ts(book_data)
        self._prune(now_ts=now)
        if not self.active_zones:
            return

        for zone in list(self.active_zones.values()):
            if now < zone.frozen_ts:
                continue

            zone_anchor = self._zone_book_anchor(zone)
            if zone_anchor <= 0:
                continue
            sweep_anchor = zone.sweep_extreme if zone.sweep_extreme > 0 else zone_anchor

            bid_depth_near_zone = self._sum_depth_near_anchor(
                bids,
                zone_anchor,
                self.book_near_zone_range_usdt,
            )
            ask_depth_near_zone = self._sum_depth_near_anchor(
                asks,
                zone_anchor,
                self.book_near_zone_range_usdt,
            )
            bid_depth_near_sweep = self._sum_depth_near_anchor(
                bids,
                sweep_anchor,
                self.book_near_sweep_range_usdt,
            )
            ask_depth_near_sweep = self._sum_depth_near_anchor(
                asks,
                sweep_anchor,
                self.book_near_sweep_range_usdt,
            )

            self._update_zone_book_metrics(
                zone=zone,
                ts=now,
                bid_depth_near_zone=bid_depth_near_zone,
                ask_depth_near_zone=ask_depth_near_zone,
                bid_depth_near_sweep=bid_depth_near_sweep,
                ask_depth_near_sweep=ask_depth_near_sweep,
            )
            self._evaluate_state_machine(zone=zone, now_ts=now)

    def _on_trade(self, trade_data: Dict[str, Any]) -> None:
        if not self.active_zones or not isinstance(trade_data, dict):
            return

        ts = self._extract_ts(trade_data)
        self._prune(now_ts=ts)
        if not self.active_zones:
            return

        price = self._extract_price(trade_data)
        if price <= 0:
            return

        side = str(trade_data.get("side") or trade_data.get("S") or "").lower()
        size = self._safe_float(trade_data.get("size", trade_data.get("sz")), 0.0)
        notional = self._safe_float(
            trade_data.get("notional", trade_data.get("trade_notional")),
            0.0,
        )
        if notional <= 0 and size > 0:
            notional = price * size

        active_buy_notional = notional if side == "buy" else 0.0
        active_sell_notional = notional if side == "sell" else 0.0
        tick_count = 1 if side in ("buy", "sell") and notional > 0 else 0

        for zone in list(self.active_zones.values()):
            if ts < zone.frozen_ts:
                continue
            self._update_zone_price(zone=zone, price=price)
            if tick_count:
                self._add_flow_to_zone(
                    zone=zone,
                    ts=ts,
                    active_buy_notional=active_buy_notional,
                    active_sell_notional=active_sell_notional,
                    tick_count=tick_count,
                )
            else:
                self._recompute_windows(zone=zone, now_ts=ts)
            self._evaluate_state_machine(zone=zone, now_ts=ts)

    def _update_zone_book_metrics(
        self,
        zone: Phase2TrackedZone,
        ts: float,
        bid_depth_near_zone: float,
        ask_depth_near_zone: float,
        bid_depth_near_sweep: float,
        ask_depth_near_sweep: float,
    ) -> None:
        previous_bid_depth = zone.bid_depth_near_zone
        previous_ask_depth = zone.ask_depth_near_zone

        zone.book_update_count += 1
        zone.bid_depth_near_zone = bid_depth_near_zone
        zone.ask_depth_near_zone = ask_depth_near_zone
        zone.bid_depth_near_sweep = bid_depth_near_sweep
        zone.ask_depth_near_sweep = ask_depth_near_sweep
        zone.last_book_ts = ts

        zone.book_samples.append(
            Phase2BookSample(
                ts=ts,
                bid_depth_near_zone=bid_depth_near_zone,
                ask_depth_near_zone=ask_depth_near_zone,
                bid_depth_near_sweep=bid_depth_near_sweep,
                ask_depth_near_sweep=ask_depth_near_sweep,
            )
        )
        self._prune_book_samples(zone=zone, now_ts=ts)

        zone.bid_reduction_1s = max(
            0.0,
            self._max_sample_depth(zone.book_samples, "bid_depth_near_zone") - bid_depth_near_zone,
        )
        zone.ask_reduction_1s = max(
            0.0,
            self._max_sample_depth(zone.book_samples, "ask_depth_near_zone") - ask_depth_near_zone,
        )

        self._update_reload_state(
            zone=zone,
            side="bid",
            previous_depth=previous_bid_depth,
            current_depth=bid_depth_near_zone,
        )
        self._update_reload_state(
            zone=zone,
            side="ask",
            previous_depth=previous_ask_depth,
            current_depth=ask_depth_near_zone,
        )
        self._recompute_book_scores(zone)

    def _prune_book_samples(self, zone: Phase2TrackedZone, now_ts: float) -> None:
        cutoff_ts = now_ts - 1.0
        while zone.book_samples and zone.book_samples[0].ts < cutoff_ts:
            zone.book_samples.popleft()

    def _update_reload_state(
        self,
        zone: Phase2TrackedZone,
        side: str,
        previous_depth: float,
        current_depth: float,
    ) -> None:
        if previous_depth <= 0:
            return

        depth_drop = max(0.0, previous_depth - current_depth)
        reload_threshold = max(self.RELOAD_MIN_DEPTH_USDT, previous_depth * self.RELOAD_MIN_DEPTH_PCT)
        if depth_drop >= reload_threshold:
            recover_target = current_depth + depth_drop * self.RELOAD_RECOVER_PCT
            if side == "bid":
                zone.bid_reload_watch_active = True
                zone.bid_reload_low = current_depth
                zone.bid_reload_recover_target = recover_target
            else:
                zone.ask_reload_watch_active = True
                zone.ask_reload_low = current_depth
                zone.ask_reload_recover_target = recover_target
            return

        if side == "bid" and zone.bid_reload_watch_active:
            zone.bid_reload_low = min(zone.bid_reload_low, current_depth)
            if current_depth >= zone.bid_reload_recover_target:
                zone.bid_reload_count += 1
                zone.bid_reload_watch_active = False
                zone.bid_reload_low = 0.0
                zone.bid_reload_recover_target = 0.0
        elif side == "ask" and zone.ask_reload_watch_active:
            zone.ask_reload_low = min(zone.ask_reload_low, current_depth)
            if current_depth >= zone.ask_reload_recover_target:
                zone.ask_reload_count += 1
                zone.ask_reload_watch_active = False
                zone.ask_reload_low = 0.0
                zone.ask_reload_recover_target = 0.0

    def _recompute_book_scores(self, zone: Phase2TrackedZone) -> None:
        self._update_relevant_book_depth_available(zone)
        if zone.direction == "BUY":
            pressure_notional = zone.active_sell_notional_1s
            book_reduction = zone.bid_reduction_1s
            reload_count = zone.bid_reload_count
        elif zone.direction == "SELL":
            pressure_notional = zone.active_buy_notional_1s
            book_reduction = zone.ask_reduction_1s
            reload_count = zone.ask_reload_count
        else:
            pressure_notional = max(zone.active_buy_notional_1s, zone.active_sell_notional_1s)
            book_reduction = max(zone.bid_reduction_1s, zone.ask_reduction_1s)
            reload_count = max(zone.bid_reload_count, zone.ask_reload_count)

        if pressure_notional > 0:
            if zone.relevant_book_depth_available:
                absorbed_notional = max(0.0, pressure_notional - max(0.0, book_reduction))
                zone.book_absorption_score = min(1.0, absorbed_notional / pressure_notional)
            else:
                zone.book_absorption_score = 0.0
        else:
            zone.book_absorption_score = 0.0
        zone.reload_score = min(1.0, max(0, reload_count) / 3.0)

    def _update_relevant_book_depth_available(self, zone: Phase2TrackedZone) -> None:
        if zone.direction == "BUY":
            zone.relevant_book_depth_available = (
                zone.bid_depth_near_zone > 0
                or zone.bid_depth_near_sweep > 0
            )
        elif zone.direction == "SELL":
            zone.relevant_book_depth_available = (
                zone.ask_depth_near_zone > 0
                or zone.ask_depth_near_sweep > 0
            )
        else:
            zone.relevant_book_depth_available = (
                zone.bid_depth_near_zone > 0
                or zone.ask_depth_near_zone > 0
                or zone.bid_depth_near_sweep > 0
                or zone.ask_depth_near_sweep > 0
            )

    def _update_zone_price(self, zone: Phase2TrackedZone, price: float) -> None:
        zone.last_price = price
        if zone.min_price_seen_after_frozen <= 0:
            zone.min_price_seen_after_frozen = price
        else:
            zone.min_price_seen_after_frozen = min(zone.min_price_seen_after_frozen, price)

        if zone.max_price_seen_after_frozen <= 0:
            zone.max_price_seen_after_frozen = price
        else:
            zone.max_price_seen_after_frozen = max(zone.max_price_seen_after_frozen, price)

        if zone.direction == "BUY":
            zone.sweep_extreme = zone.min_price_seen_after_frozen
            zone.break_depth_u = max(0.0, zone.frozen_low - zone.min_price_seen_after_frozen)
            zone.break_depth_pct = zone.break_depth_u / zone.frozen_low if zone.frozen_low > 0 else 0.0
        elif zone.direction == "SELL":
            zone.sweep_extreme = zone.max_price_seen_after_frozen
            zone.break_depth_u = max(0.0, zone.max_price_seen_after_frozen - zone.frozen_high)
            zone.break_depth_pct = zone.break_depth_u / zone.frozen_high if zone.frozen_high > 0 else 0.0
        else:
            zone.sweep_extreme = price
            zone.break_depth_u = 0.0
            zone.break_depth_pct = 0.0

    def _add_flow_to_zone(
        self,
        zone: Phase2TrackedZone,
        ts: float,
        active_buy_notional: float,
        active_sell_notional: float,
        tick_count: int,
    ) -> None:
        bucket_ts = int(ts)
        bucket = self._get_or_create_bucket(zone=zone, bucket_ts=bucket_ts)
        bucket.active_buy_notional += active_buy_notional
        bucket.active_sell_notional += active_sell_notional
        bucket.tick_count += tick_count
        self._recompute_windows(zone=zone, now_ts=ts)

    def _get_or_create_bucket(self, zone: Phase2TrackedZone, bucket_ts: int) -> Phase2FlowBucket:
        if zone.flow_buckets and zone.flow_buckets[-1].bucket_ts == bucket_ts:
            return zone.flow_buckets[-1]

        for bucket in reversed(zone.flow_buckets):
            if bucket.bucket_ts == bucket_ts:
                return bucket
            if bucket.bucket_ts < bucket_ts:
                break

        bucket = Phase2FlowBucket(bucket_ts=bucket_ts)
        zone.flow_buckets.append(bucket)
        return bucket

    def _recompute_windows(self, zone: Phase2TrackedZone, now_ts: float) -> None:
        oldest_bucket_ts = int(now_ts) - 9
        while zone.flow_buckets and zone.flow_buckets[0].bucket_ts < oldest_bucket_ts:
            zone.flow_buckets.popleft()

        buy_1s, sell_1s, ticks_1s = self._sum_buckets(zone.flow_buckets, now_ts, 1)
        buy_3s, sell_3s, ticks_3s = self._sum_buckets(zone.flow_buckets, now_ts, 3)
        buy_10s, sell_10s, ticks_10s = self._sum_buckets(zone.flow_buckets, now_ts, 10)

        zone.active_buy_notional_1s = buy_1s
        zone.active_buy_notional_3s = buy_3s
        zone.active_buy_notional_10s = buy_10s
        zone.active_sell_notional_1s = sell_1s
        zone.active_sell_notional_3s = sell_3s
        zone.active_sell_notional_10s = sell_10s
        zone.cvd_delta_3s = buy_3s - sell_3s
        zone.cvd_delta_10s = buy_10s - sell_10s
        zone.tick_count_1s = ticks_1s
        zone.tick_count_3s = ticks_3s
        zone.tick_count_10s = ticks_10s
        self._recompute_book_scores(zone)

    def _sum_buckets(
        self,
        buckets: Deque[Phase2FlowBucket],
        now_ts: float,
        window_seconds: int,
    ) -> Tuple[float, float, int]:
        min_bucket_ts = int(now_ts) - int(window_seconds) + 1
        buy_notional = 0.0
        sell_notional = 0.0
        tick_count = 0
        for bucket in buckets:
            if bucket.bucket_ts < min_bucket_ts:
                continue
            buy_notional += bucket.active_buy_notional
            sell_notional += bucket.active_sell_notional
            tick_count += bucket.tick_count
        return buy_notional, sell_notional, tick_count

    def _evaluate_state_machine(self, zone: Phase2TrackedZone, now_ts: float) -> None:
        if zone.state in ("PHASE2_CONFIRMED", "PHASE2_FAILED", "PHASE2_TIMEOUT"):
            return
        price = zone.last_price
        if price <= 0 or zone.frozen_low <= 0 or zone.frozen_high <= 0:
            return

        self._update_boundary_timers(zone=zone, now_ts=now_ts)
        self._recompute_phase2_scores(zone=zone)

        if not zone.has_tested_zone and self._is_testing_zone(zone=zone, price=price):
            zone.has_tested_zone = True
            zone.testing_zone_ts = now_ts
            if zone.state == "PHASE2_WAITING":
                self._transition_state(
                    zone=zone,
                    new_state="PHASE2_TESTING_ZONE",
                    now_ts=now_ts,
                    reason="price_entered_frozen_zone_buffer",
                )

        if self._is_sweeping_boundary(zone=zone, price=price):
            zone.has_swept_boundary = True
            if zone.sweep_started_ts <= 0:
                zone.sweep_started_ts = now_ts
            sweep_state = "PHASE2_SWEEPING_LOW" if zone.direction == "BUY" else "PHASE2_SWEEPING_HIGH"
            if zone.state in ("PHASE2_WAITING", "PHASE2_TESTING_ZONE"):
                self._transition_state(
                    zone=zone,
                    new_state=sweep_state,
                    now_ts=now_ts,
                    reason="price_swept_boundary",
                )

        failure_reason = self._hard_failure_reason(zone=zone, now_ts=now_ts)
        if failure_reason:
            zone.has_failed = True
            zone.failed_ts = now_ts
            self._transition_state(zone=zone, new_state="PHASE2_FAILED", now_ts=now_ts, reason=failure_reason)
            return

        if zone.state in ("PHASE2_SWEEPING_LOW", "PHASE2_SWEEPING_HIGH"):
            absorption_reason = self._absorption_reason(zone=zone)
            if absorption_reason:
                zone.has_absorbed_after_sweep = True
                absorbing_state = (
                    "PHASE2_ABSORBING_BELOW_LOW"
                    if zone.direction == "BUY"
                    else "PHASE2_ABSORBING_ABOVE_HIGH"
                )
                self._transition_state(
                    zone=zone,
                    new_state=absorbing_state,
                    now_ts=now_ts,
                    reason=absorption_reason,
                )

        if zone.state in (
            "PHASE2_SWEEPING_LOW",
            "PHASE2_SWEEPING_HIGH",
            "PHASE2_ABSORBING_BELOW_LOW",
            "PHASE2_ABSORBING_ABOVE_HIGH",
        ):
            reclaim_reason = self._reclaim_reason(zone=zone, price=price)
            if reclaim_reason:
                zone.has_reclaimed_boundary = True
                zone.sweep_reclaimed_ts = now_ts
                reclaim_state = "PHASE2_RECLAIMING_LOW" if zone.direction == "BUY" else "PHASE2_RECLAIMING_HIGH"
                self._transition_state(
                    zone=zone,
                    new_state=reclaim_state,
                    now_ts=now_ts,
                    reason=reclaim_reason,
                )
                zone.previous_break_depth_u = zone.break_depth_u
                zone.previous_pressure_notional_3s = self._pressure_notional_3s(zone)
                return

        if zone.state in ("PHASE2_RECLAIMING_LOW", "PHASE2_RECLAIMING_HIGH"):
            retest_reason = self._retest_reason(zone=zone, price=price)
            if retest_reason:
                zone.has_retested_inside_zone = True
                zone.retest_started_ts = now_ts
                self._transition_state(
                    zone=zone,
                    new_state="PHASE2_RETEST_INSIDE_ZONE",
                    now_ts=now_ts,
                    reason=retest_reason,
                )

        confirm_reason = self._confirm_reason(zone=zone)
        if not confirm_reason:
            confirm_reason = self._clean_hold_confirm_reason(zone=zone)
        if confirm_reason:
            zone.has_confirmed = True
            zone.confirmed_ts = now_ts
            self._transition_state(
                zone=zone,
                new_state="PHASE2_CONFIRMED",
                now_ts=now_ts,
                reason=confirm_reason,
            )
            self._log_phase2_confirmed(zone=zone, now_ts=now_ts, reason=confirm_reason)

        zone.previous_break_depth_u = zone.break_depth_u
        zone.previous_pressure_notional_3s = self._pressure_notional_3s(zone)

    def _update_boundary_timers(self, zone: Phase2TrackedZone, now_ts: float) -> None:
        price = zone.last_price
        if zone.direction == "BUY":
            if zone.sweep_started_ts > 0:
                end_ts = zone.sweep_reclaimed_ts if zone.sweep_reclaimed_ts > 0 else now_ts
                zone.time_below_boundary_ms = max(0.0, (end_ts - zone.sweep_started_ts) * 1000.0)
            zone.time_above_boundary_ms = 0.0 if price <= zone.frozen_high else max(0.0, (now_ts - zone.phase2_registered_ts) * 1000.0)
        elif zone.direction == "SELL":
            if zone.sweep_started_ts > 0:
                end_ts = zone.sweep_reclaimed_ts if zone.sweep_reclaimed_ts > 0 else now_ts
                zone.time_above_boundary_ms = max(0.0, (end_ts - zone.sweep_started_ts) * 1000.0)
            zone.time_below_boundary_ms = 0.0 if price >= zone.frozen_low else max(0.0, (now_ts - zone.phase2_registered_ts) * 1000.0)

    def _recompute_phase2_scores(self, zone: Phase2TrackedZone) -> None:
        pressure_1s = self._pressure_notional_1s(zone)
        pressure_3s = self._pressure_notional_3s(zone)
        opposite_1s = self._opposite_notional_1s(zone)

        pressure_decay = 0.0
        if pressure_3s >= self.min_active_notional_3s:
            one_second_ratio = pressure_1s / pressure_3s if pressure_3s > 0 else 1.0
            if one_second_ratio <= 0.35:
                pressure_decay += 0.55
            if zone.break_depth_pct <= self.max_sweep_depth_pct_soft:
                pressure_decay += 0.25
            if zone.previous_break_depth_u > 0 and zone.break_depth_u <= zone.previous_break_depth_u * 1.15:
                pressure_decay += 0.20
        zone.pressure_decay_score = min(1.0, pressure_decay)

        directional_reload_score = self._directional_reload_score(zone)
        if zone.relevant_book_depth_available:
            book_score = max(zone.book_absorption_score, directional_reload_score)
        else:
            book_score = 0.0
        zone.absorption_score = min(1.0, max(book_score, zone.pressure_decay_score))

        location_score = 0.0
        if zone.direction == "BUY":
            if zone.last_price >= zone.frozen_low:
                location_score = 0.45
            elif zone.last_price >= zone.frozen_low - self.reclaim_buffer_usdt:
                location_score = 0.30
            cvd_score = 0.25 if zone.cvd_delta_3s >= 0 or opposite_1s >= pressure_1s else 0.0
        elif zone.direction == "SELL":
            if zone.last_price <= zone.frozen_high:
                location_score = 0.45
            elif zone.last_price <= zone.frozen_high + self.reclaim_buffer_usdt:
                location_score = 0.30
            cvd_score = 0.25 if zone.cvd_delta_3s <= 0 or opposite_1s >= pressure_1s else 0.0
        else:
            cvd_score = 0.0
        flow_shift_score = 0.20 if pressure_1s <= max(1.0, pressure_3s * 0.40) else 0.0
        book_support_score = 0.10 if self._directional_reload_score(zone) >= self.min_reload_score else 0.0
        zone.reclaim_score = min(1.0, location_score + cvd_score + flow_shift_score + book_support_score)

        inside_score = 0.45 if self._is_retest_inside_zone(zone=zone, price=zone.last_price) else 0.0
        no_deep_resweep_score = 0.20 if zone.break_depth_pct <= self.max_sweep_depth_pct_soft else 0.0
        decay_score = 0.20 if pressure_1s <= max(1.0, pressure_3s * 0.40) else 0.0
        retest_book_score = 0.15 if zone.relevant_book_depth_available and self._directional_reload_score(zone) >= self.min_reload_score else 0.0
        zone.retest_score = min(1.0, inside_score + no_deep_resweep_score + decay_score + retest_book_score)

        if zone.break_depth_pct > self.max_sweep_depth_pct_soft:
            depth_penalty = min(0.20, (zone.break_depth_pct - self.max_sweep_depth_pct_soft) * 20.0)
        else:
            depth_penalty = 0.0
        zone.phase2_total_score = max(
            0.0,
            min(
                1.0,
                0.35 * zone.absorption_score
                + 0.30 * zone.reclaim_score
                + 0.25 * zone.retest_score
                + 0.10 * self._directional_reload_score(zone)
                - depth_penalty,
            ),
        )

    def _absorption_reason(self, zone: Phase2TrackedZone) -> str:
        if not self._is_sweeping_boundary(zone=zone, price=zone.last_price):
            return ""
        pressure_3s = self._pressure_notional_3s(zone)
        if pressure_3s < self.min_active_notional_3s:
            return ""
        if zone.relevant_book_depth_available and zone.book_absorption_score >= self.min_absorption_score:
            return "book_absorption_with_relevant_depth"
        if self._directional_reload_score(zone) >= self.min_reload_score and self._directional_reload_count(zone) > 0:
            return "book_reload_with_relevant_depth" if zone.relevant_book_depth_available else ""
        if zone.pressure_decay_score >= self.min_absorption_score:
            return "trade_flow_pressure_decay_without_book_depth" if not zone.relevant_book_depth_available else "trade_flow_pressure_decay"
        return ""

    def _reclaim_reason(self, zone: Phase2TrackedZone, price: float) -> str:
        if zone.direction == "BUY":
            reclaimed = price >= zone.frozen_low - self.reclaim_buffer_usdt
        elif zone.direction == "SELL":
            reclaimed = price <= zone.frozen_high + self.reclaim_buffer_usdt
        else:
            return ""
        if not reclaimed or zone.reclaim_score < self.min_reclaim_score:
            return ""
        return "boundary_reclaimed_with_orderflow_shift"

    def _retest_reason(self, zone: Phase2TrackedZone, price: float) -> str:
        if not self._is_retest_inside_zone(zone=zone, price=price):
            return ""
        if zone.retest_score < self.min_retest_score:
            return ""
        return "inside_zone_retest_holding"

    def _confirm_reason(self, zone: Phase2TrackedZone) -> str:
        if zone.state != "PHASE2_RETEST_INSIDE_ZONE":
            return ""
        if zone.phase2_total_score < self.min_total_score:
            return ""
        if (
            zone.has_tested_zone
            and zone.has_swept_boundary
            and zone.has_absorbed_after_sweep
            and zone.has_reclaimed_boundary
            and zone.has_retested_inside_zone
            and not zone.has_failed
        ):
            zone.phase2_type = "SWEEP_RECLAIM"
            return "event_sequence_sweep_absorb_reclaim_retest"
        return ""

    def _clean_hold_confirm_reason(self, zone: Phase2TrackedZone) -> str:
        if zone.state != "PHASE2_TESTING_ZONE":
            return ""
        if not zone.has_tested_zone or zone.has_swept_boundary or zone.has_failed:
            return ""
        if not self._is_retest_inside_zone(zone=zone, price=zone.last_price):
            return ""
        if self._pressure_notional_3s(zone) < self.min_active_notional_3s:
            return ""
        if zone.phase2_total_score < self.min_total_score:
            return ""
        if zone.relevant_book_depth_available and zone.book_absorption_score >= self.min_absorption_score:
            zone.phase2_type = "CLEAN_HOLD"
            return "clean_hold_book_absorption_with_relevant_depth"
        if self._directional_reload_score(zone) >= self.min_reload_score and self._directional_reload_count(zone) > 0:
            zone.phase2_type = "CLEAN_HOLD"
            return "clean_hold_book_reload"
        if zone.pressure_decay_score >= self.min_absorption_score:
            zone.phase2_type = "CLEAN_HOLD"
            return "clean_hold_trade_flow_pressure_decay"
        return ""

    def _hard_failure_reason(self, zone: Phase2TrackedZone, now_ts: float) -> str:
        if zone.break_depth_pct >= self.max_sweep_depth_pct_hard:
            return "hard_sweep_depth_exceeded"
        if zone.direction == "BUY":
            if (
                zone.time_below_boundary_ms >= self.max_time_below_ms
                and not zone.has_reclaimed_boundary
            ):
                return "max_time_below_without_reclaim"
            pressure_10s = zone.active_sell_notional_10s
            pressure_1s = zone.active_sell_notional_1s
            away_from_boundary = zone.last_price < zone.frozen_low - self.retest_buffer_usdt
            max_away_time_ms = self.max_time_below_ms
        elif zone.direction == "SELL":
            if (
                zone.time_above_boundary_ms >= self.max_time_above_ms
                and not zone.has_reclaimed_boundary
            ):
                return "max_time_above_without_reclaim"
            pressure_10s = zone.active_buy_notional_10s
            pressure_1s = zone.active_buy_notional_1s
            away_from_boundary = zone.last_price > zone.frozen_high + self.retest_buffer_usdt
            max_away_time_ms = self.max_time_above_ms
        else:
            return ""
        if (
            pressure_10s >= self.min_active_notional_3s * 3.0
            and pressure_1s >= self.min_active_notional_3s
            and not zone.has_absorbed_after_sweep
            and self._directional_reload_count(zone) <= 0
            and not zone.has_reclaimed_boundary
        ):
            return "pressure_expanding_without_absorption_reload_reclaim"
        if (
            away_from_boundary
            and not zone.relevant_book_depth_available
            and not zone.has_absorbed_after_sweep
            and (now_ts - max(zone.sweep_started_ts, zone.phase2_registered_ts)) * 1000.0 >= max_away_time_ms
        ):
            return "far_from_boundary_without_book_depth_verification"
        return ""

    def _transition_state(
        self,
        zone: Phase2TrackedZone,
        new_state: str,
        now_ts: float,
        reason: str,
    ) -> bool:
        if zone.state == new_state:
            return False
        previous_state = zone.state
        zone.previous_state = previous_state
        zone.state = new_state
        zone.state_updated_ts = now_ts
        zone.state_entered_ts = now_ts
        zone.phase2_reason = reason
        if new_state == "PHASE2_FAILED" and zone.failed_ts <= 0:
            zone.failed_ts = now_ts
        elif new_state == "PHASE2_TIMEOUT" and zone.timeout_ts <= 0:
            zone.timeout_ts = now_ts
        elif new_state == "PHASE2_CONFIRMED" and zone.confirmed_ts <= 0:
            zone.confirmed_ts = now_ts
        zone.last_state_log_ts = now_ts
        self._log_phase2_state(zone=zone, previous_state=previous_state, now_ts=now_ts, reason=reason)
        return True

    def _log_phase2_state(
        self,
        zone: Phase2TrackedZone,
        previous_state: str,
        now_ts: float,
        reason: str,
    ) -> None:
        logger.info(
            "[PHASE2-STATE] zone_id=%s direction=%s previous_state=%s state=%s ts=%.3f price=%.2f frozen_low=%.2f frozen_high=%.2f live_low=%.2f live_high=%.2f sweep_extreme=%.2f break_depth_u=%.4f break_depth_pct=%.6f time_below_boundary_ms=%.1f time_above_boundary_ms=%.1f active_buy_notional_1s=%.2f active_sell_notional_1s=%.2f active_buy_notional_3s=%.2f active_sell_notional_3s=%.2f cvd_delta_3s=%.2f cvd_delta_10s=%.2f bid_depth_near_zone=%.2f ask_depth_near_zone=%.2f bid_depth_near_sweep=%.2f ask_depth_near_sweep=%.2f bid_reduction_1s=%.2f ask_reduction_1s=%.2f bid_reload_count=%d ask_reload_count=%d book_absorption_score=%.4f relevant_book_depth_available=%s reload_score=%.4f absorption_score=%.4f pressure_decay_score=%.4f reclaim_score=%.4f retest_score=%.4f phase2_total_score=%.4f reason=%s",
            zone.zone_id,
            zone.direction,
            previous_state,
            zone.state,
            now_ts,
            zone.last_price,
            zone.frozen_low,
            zone.frozen_high,
            zone.live_low,
            zone.live_high,
            zone.sweep_extreme,
            zone.break_depth_u,
            zone.break_depth_pct,
            zone.time_below_boundary_ms,
            zone.time_above_boundary_ms,
            zone.active_buy_notional_1s,
            zone.active_sell_notional_1s,
            zone.active_buy_notional_3s,
            zone.active_sell_notional_3s,
            zone.cvd_delta_3s,
            zone.cvd_delta_10s,
            zone.bid_depth_near_zone,
            zone.ask_depth_near_zone,
            zone.bid_depth_near_sweep,
            zone.ask_depth_near_sweep,
            zone.bid_reduction_1s,
            zone.ask_reduction_1s,
            zone.bid_reload_count,
            zone.ask_reload_count,
            zone.book_absorption_score,
            zone.relevant_book_depth_available,
            zone.reload_score,
            zone.absorption_score,
            zone.pressure_decay_score,
            zone.reclaim_score,
            zone.retest_score,
            zone.phase2_total_score,
            reason,
        )

    def _log_phase2_confirmed(self, zone: Phase2TrackedZone, now_ts: float, reason: str) -> None:
        suggested_stop = self._suggested_stop(zone)
        risk_to_stop_u = abs(zone.last_price - suggested_stop) if suggested_stop > 0 and zone.last_price > 0 else 0.0
        risk_to_stop_pct = risk_to_stop_u / zone.last_price if zone.last_price > 0 else 0.0
        logger.info(
            "[PHASE2-CONFIRMED] zone_id=%s direction=%s phase2_type=%s confirm_ts=%.3f confirm_price=%.2f confirm_reason=%s frozen_low=%.2f frozen_high=%.2f sweep_extreme=%.2f suggested_stop=%.2f risk_to_stop_u=%.4f risk_to_stop_pct=%.6f orderflow_score=%.4f book_absorption_score=%.4f relevant_book_depth_available=%s reload_score=%.4f absorption_score=%.4f pressure_decay_score=%.4f reclaim_score=%.4f retest_score=%.4f phase2_total_score=%.4f",
            zone.zone_id,
            zone.direction,
            zone.phase2_type,
            now_ts,
            zone.last_price,
            reason,
            zone.frozen_low,
            zone.frozen_high,
            zone.sweep_extreme,
            suggested_stop,
            risk_to_stop_u,
            risk_to_stop_pct,
            zone.phase2_total_score,
            zone.book_absorption_score,
            zone.relevant_book_depth_available,
            zone.reload_score,
            zone.absorption_score,
            zone.pressure_decay_score,
            zone.reclaim_score,
            zone.retest_score,
            zone.phase2_total_score,
        )

    def _suggested_stop(self, zone: Phase2TrackedZone) -> float:
        if zone.direction == "BUY":
            return (zone.sweep_extreme - 0.5) if zone.sweep_extreme > 0 else (zone.frozen_low - 0.8)
        if zone.direction == "SELL":
            return (zone.sweep_extreme + 0.5) if zone.sweep_extreme > 0 else (zone.frozen_high + 0.8)
        return 0.0

    def _is_testing_zone(self, zone: Phase2TrackedZone, price: float) -> bool:
        return (
            price >= zone.frozen_low - self.test_zone_buffer_usdt
            and price <= zone.frozen_high + self.test_zone_buffer_usdt
        )

    def _is_sweeping_boundary(self, zone: Phase2TrackedZone, price: float) -> bool:
        if zone.direction == "BUY":
            return price < zone.frozen_low
        if zone.direction == "SELL":
            return price > zone.frozen_high
        return False

    def _is_retest_inside_zone(self, zone: Phase2TrackedZone, price: float) -> bool:
        if zone.direction == "BUY":
            return (
                price >= zone.frozen_low - self.retest_buffer_usdt
                and price <= zone.frozen_high + self.test_zone_buffer_usdt
            )
        if zone.direction == "SELL":
            return (
                price <= zone.frozen_high + self.retest_buffer_usdt
                and price >= zone.frozen_low - self.test_zone_buffer_usdt
            )
        return False

    def _pressure_notional_1s(self, zone: Phase2TrackedZone) -> float:
        return zone.active_sell_notional_1s if zone.direction == "BUY" else zone.active_buy_notional_1s

    def _pressure_notional_3s(self, zone: Phase2TrackedZone) -> float:
        return zone.active_sell_notional_3s if zone.direction == "BUY" else zone.active_buy_notional_3s

    def _opposite_notional_1s(self, zone: Phase2TrackedZone) -> float:
        return zone.active_buy_notional_1s if zone.direction == "BUY" else zone.active_sell_notional_1s

    def _directional_reload_count(self, zone: Phase2TrackedZone) -> int:
        return zone.bid_reload_count if zone.direction == "BUY" else zone.ask_reload_count

    def _directional_reload_score(self, zone: Phase2TrackedZone) -> float:
        if self._directional_reload_count(zone) <= 0:
            return 0.0
        return zone.reload_score

    def _prune(self, now_ts: Optional[float] = None, reserve_slots: int = 0) -> None:
        now = float(now_ts if now_ts is not None else time.time())
        if self.zone_ttl_seconds > 0:
            expired_ids = [
                zone_id
                for zone_id, zone in self.active_zones.items()
                if now - zone.phase2_registered_ts > self.zone_ttl_seconds
            ]
            for zone_id in expired_ids:
                zone = self.active_zones.pop(zone_id, None)
                if zone:
                    if zone.state not in ("PHASE2_CONFIRMED", "PHASE2_FAILED", "PHASE2_TIMEOUT"):
                        zone.has_failed = False
                        zone.timeout_ts = now
                        self._transition_state(
                            zone=zone,
                            new_state="PHASE2_TIMEOUT",
                            now_ts=now,
                            reason="zone_ttl_exceeded",
                        )
                    self._log_zone_expired(zone=zone, now_ts=now, expire_reason="TTL")

        target_size = max(0, self.max_active_zones - max(0, int(reserve_slots)))
        while len(self.active_zones) > target_size:
            _, zone = self.active_zones.popitem(last=False)
            self._log_zone_expired(zone=zone, now_ts=now, expire_reason="CAPACITY_LIMIT")

    def _log_zone_expired(self, zone: Phase2TrackedZone, now_ts: float, expire_reason: str) -> None:
        logger.info(
            "[PHASE2-ZONE-EXPIRED] zone_id=%s direction=%s expire_reason=%s age_seconds=%.1f last_state=%s frozen_low=%.2f frozen_high=%.2f min_price_seen=%.2f max_price_seen=%.2f bid_depth_near_zone=%.0f ask_depth_near_zone=%.0f bid_depth_near_sweep=%.0f ask_depth_near_sweep=%.0f bid_reload_count=%d ask_reload_count=%d book_absorption_score=%.4f relevant_book_depth_available=%s reload_score=%.4f book_update_count=%d",
            zone.zone_id,
            zone.direction,
            expire_reason,
            max(0.0, now_ts - zone.phase2_registered_ts),
            zone.state,
            zone.frozen_low,
            zone.frozen_high,
            zone.min_price_seen_after_frozen,
            zone.max_price_seen_after_frozen,
            zone.bid_depth_near_zone,
            zone.ask_depth_near_zone,
            zone.bid_depth_near_sweep,
            zone.ask_depth_near_sweep,
            zone.bid_reload_count,
            zone.ask_reload_count,
            zone.book_absorption_score,
            zone.relevant_book_depth_available,
            zone.reload_score,
            zone.book_update_count,
        )

    def _has_trade_fields(self, data: Dict[str, Any]) -> bool:
        return (
            self._extract_price(data) > 0
            and str(data.get("side") or data.get("S") or "").lower() in ("buy", "sell")
            and (
                self._safe_float(data.get("size", data.get("sz")), 0.0) > 0
                or self._safe_float(data.get("notional", data.get("trade_notional")), 0.0) > 0
            )
        )

    def _extract_ts(self, data: Dict[str, Any]) -> float:
        return self._safe_float(
            data.get("ts", data.get("time", data.get("recv_ts"))),
            time.time(),
        )

    def _extract_price(self, data: Dict[str, Any]) -> float:
        return self._safe_float(
            data.get("price", data.get("px", data.get("last_price", data.get("current_price")))),
            0.0,
        )

    def _extract_book_levels(self, levels: Any) -> Optional[Tuple[Tuple[float, float], ...]]:
        if levels is None:
            return None
        items: Iterable[Any]
        if isinstance(levels, dict):
            items = levels.items()
        elif isinstance(levels, (list, tuple)):
            items = levels
        else:
            return None

        parsed_levels = []
        for item in items:
            if isinstance(item, dict):
                price = self._safe_float(item.get("price", item.get("px")), 0.0)
                size = self._safe_float(item.get("size", item.get("sz", item.get("qty"))), 0.0)
            else:
                try:
                    price = self._safe_float(item[0], 0.0)
                    size = self._safe_float(item[1], 0.0)
                except (TypeError, IndexError, KeyError):
                    continue
            if price > 0 and size > 0:
                parsed_levels.append((price, size))
        return tuple(parsed_levels)

    def _zone_book_anchor(self, zone: Phase2TrackedZone) -> float:
        if zone.direction == "BUY":
            return zone.frozen_low
        if zone.direction == "SELL":
            return zone.frozen_high
        return zone.zone_mid

    @staticmethod
    def _sum_depth_near_anchor(
        levels: Tuple[Tuple[float, float], ...],
        anchor: float,
        price_range: float,
    ) -> float:
        if anchor <= 0:
            return 0.0
        lower = anchor - price_range
        upper = anchor + price_range
        depth = 0.0
        for price, size in levels:
            if lower <= price <= upper:
                depth += price * size
        return depth

    @staticmethod
    def _max_sample_depth(samples: Deque[Phase2BookSample], attr_name: str) -> float:
        max_depth = 0.0
        for sample in samples:
            max_depth = max(max_depth, float(getattr(sample, attr_name, 0.0)))
        return max_depth

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(default)

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)
