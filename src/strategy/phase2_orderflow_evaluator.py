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
from typing import Any, Deque, Dict, Optional, Tuple

from config.research_evaluator import MAX_ACTIVE_PHASE2_ZONES, PHASE2_ZONE_TTL_SECONDS

logger = logging.getLogger(__name__)


@dataclass
class Phase2FlowBucket:
    bucket_ts: int
    active_buy_notional: float = 0.0
    active_sell_notional: float = 0.0
    tick_count: int = 0


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
    state: str = "TRACKING"
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
    metadata: Dict[str, Any] = field(default_factory=dict, repr=False)
    flow_buckets: Deque[Phase2FlowBucket] = field(default_factory=deque, repr=False)

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
            }
        )
        return snapshot


class Phase2OrderflowEvaluator:
    def __init__(
        self,
        max_active_zones: int = MAX_ACTIVE_PHASE2_ZONES,
        zone_ttl_seconds: float = PHASE2_ZONE_TTL_SECONDS,
    ):
        self.max_active_zones = max(1, int(max_active_zones))
        self.zone_ttl_seconds = max(0.0, float(zone_ttl_seconds))
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
        except Exception:
            logger.exception("[PHASE2-PRICE-FAILED]")

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
            phase2_registered_ts=now_ts,
            metadata=dict(zone),
        )

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
        oldest_bucket_ts = int(now_ts) - 10
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

    def _sum_buckets(
        self,
        buckets: Deque[Phase2FlowBucket],
        now_ts: float,
        window_seconds: int,
    ) -> Tuple[float, float, int]:
        min_bucket_ts = int(now_ts) - int(window_seconds)
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
                    self._log_zone_expired(zone=zone, now_ts=now)

        target_size = max(0, self.max_active_zones - max(0, int(reserve_slots)))
        while len(self.active_zones) > target_size:
            _, zone = self.active_zones.popitem(last=False)
            self._log_zone_expired(zone=zone, now_ts=now)

    def _log_zone_expired(self, zone: Phase2TrackedZone, now_ts: float) -> None:
        logger.info(
            "[PHASE2-ZONE-EXPIRED] zone_id=%s direction=%s age_seconds=%.1f last_state=%s frozen_low=%.2f frozen_high=%.2f min_price_seen=%.2f max_price_seen=%.2f",
            zone.zone_id,
            zone.direction,
            max(0.0, now_ts - zone.phase2_registered_ts),
            zone.state,
            zone.frozen_low,
            zone.frozen_high,
            zone.min_price_seen_after_frozen,
            zone.max_price_seen_after_frozen,
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
