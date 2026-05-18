#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Iceberg Zone Tracker / Lifecycle Tracker (A1-Iceberg V6).

The tracker consumes every settled Phase1 iceberg impact, including positive
and negative evidence, and upgrades single events into lifecycle-aware zones.
"""

import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class IcebergZoneTracker:
    def __init__(
        self,
        zone_merge_time_sec: float = 120.0,
        zone_price_gap_usdt: float = 2.0,
        max_zone_width_usdt: float = 6.0,
        zone_expire_sec: float = 180.0,
        broken_buffer_usdt: float = 1.0,
        max_active_zones: int = 100,
    ):
        self.zone_merge_time_sec = float(zone_merge_time_sec)
        self.zone_price_gap_usdt = float(zone_price_gap_usdt)
        self.max_zone_width_usdt = float(max_zone_width_usdt)
        self.zone_expire_sec = float(zone_expire_sec)
        self.broken_buffer_usdt = float(broken_buffer_usdt)

        self.quality_weight = {
            "LOW": 1.0,
            "MEDIUM": 2.0,
            "HIGH": 3.0,
        }
        self.absorption_rate_score_cap = 1.2
        self.hidden_vs_active_score_cap = 1.2
        self.max_active_zones = int(max_active_zones)

        self.zones: List[Dict[str, Any]] = []
        self._zone_seq = 0
        self._pending_finalized_zones: List[Dict[str, Any]] = []

    def update(self, event: Dict[str, Any], current_price: float = 0.0) -> Optional[Dict[str, Any]]:
        """
        Receive an IcebergImpactEvent, merge/create a zone, update lifecycle
        state and score, and return the updated zone.
        """
        normalized = self._normalize_event(event)
        if not normalized:
            return None

        self.expire_old_zones(float(normalized.get("ts", time.time())))

        zone = self._find_merge_zone(normalized)
        if zone is None:
            zone = self._create_zone(normalized)
            if normalized.get("cancel_reason") in ("PRICE_BROKE_DOWN", "PRICE_BROKE_UP"):
                self._update_state(zone, normalized, current_price)
            self._log_zone_update(zone, normalized, "NEW")
            self._prune_zones()
            return zone

        self._update_zone(zone, normalized, current_price=current_price)
        self._log_zone_update(zone, normalized, "UPDATE")
        self._prune_zones()
        return zone

    def expire_old_zones(self, now_ts: float) -> List[Dict[str, Any]]:
        """Mark zones with no recent updates as EXPIRED and return them."""
        expired = []
        now = float(now_ts or time.time())
        for zone in self.zones:
            if zone.get("state") in ("BROKEN", "EXPIRED"):
                continue
            last_seen = float(zone.get("last_seen_ts", 0.0))
            if now - last_seen <= self.zone_expire_sec:
                continue

            previous_state = zone.get("state")
            zone["previous_state"] = previous_state
            zone["state"] = "EXPIRED"
            expired.append(zone)
            self._append_finalized_zone(zone)
            logger.info(
                "[ICEBERG-ZONE-EXPIRED] id=%s direction=%s previous_state=%s final_state=%s events=%d icebergs=%d ignore=%d spoof=%d pos=%.2f neg=%.2f net=%.2f",
                zone.get("zone_id"),
                zone.get("direction"),
                previous_state,
                zone.get("state"),
                int(zone.get("event_count", 0)),
                int(zone.get("iceberg_count", 0)),
                int(zone.get("ignore_count", 0)),
                int(zone.get("spoof_count", 0)),
                float(zone.get("positive_score", 0.0)),
                float(zone.get("negative_score", 0.0)),
                float(zone.get("net_score", 0.0)),
            )
        return expired

    def get_active_zones(self) -> List[Dict[str, Any]]:
        return [
            self._public_zone(zone)
            for zone in self.zones
            if zone.get("state") not in ("BROKEN", "EXPIRED")
        ]

    def _find_merge_zone(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        candidates = []
        event_lower = float(event.get("zone_lower", 0.0))
        event_upper = float(event.get("zone_upper", 0.0))
        event_ts = float(event.get("ts", 0.0))

        for zone in self.zones:
            if zone.get("direction") != event.get("direction"):
                continue
            if zone.get("state") in ("BROKEN", "EXPIRED"):
                continue
            if event_ts - float(zone.get("last_seen_ts", 0.0)) > self.zone_merge_time_sec:
                continue

            zone_lower = float(zone.get("zone_lower", 0.0))
            zone_upper = float(zone.get("zone_upper", 0.0))
            distance = self._interval_distance(event_lower, event_upper, zone_lower, zone_upper)
            if distance > self.zone_price_gap_usdt:
                continue

            merged_lower = min(zone_lower, event_lower)
            merged_upper = max(zone_upper, event_upper)
            if merged_upper - merged_lower > self.max_zone_width_usdt:
                continue

            candidates.append((distance, -float(zone.get("last_seen_ts", 0.0)), zone))

        if not candidates:
            return None

        candidates.sort(key=lambda item: (item[0], item[1]))
        return candidates[0][2]

    def _create_zone(self, event: Dict[str, Any]) -> Dict[str, Any]:
        self._zone_seq += 1
        zone_id = f"iz-{self._zone_seq}"
        capped_hidden = self._capped_hidden_volume(event)
        result = str(event.get("result") or "")
        initial_state = "DISCOVERED" if result == "ICEBERG" else "STRESSED"

        zone = {
            "zone_id": zone_id,
            "direction": event.get("direction"),
            "state": initial_state,
            "zone_lower": float(event.get("zone_lower", 0.0)),
            "zone_upper": float(event.get("zone_upper", 0.0)),
            "anchor_price": float(event.get("trigger_price", 0.0)),
            "first_seen_ts": float(event.get("ts", 0.0)),
            "last_seen_ts": float(event.get("ts", 0.0)),
            "first_event_id": event.get("event_id"),
            "last_event_id": event.get("event_id"),
            "event_count": 1,
            "iceberg_count": 1 if result == "ICEBERG" else 0,
            "ignore_count": 1 if result == "IGNORE" else 0,
            "spoof_count": 1 if result == "SPOOFING" else 0,
            "cancel_count": 1 if result == "CANCEL" else 0,
            "high_count": 1 if event.get("quality") == "HIGH" else 0,
            "medium_count": 1 if event.get("quality") == "MEDIUM" else 0,
            "low_count": 1 if event.get("quality") == "LOW" else 0,
            "total_active_volume": float(event.get("active_volume", 0.0)),
            "total_hidden_volume": float(event.get("hidden_volume", 0.0)),
            "total_capped_hidden_volume": capped_hidden,
            "total_book_reduction": float(event.get("book_reduction", 0.0)),
            "total_trade_count": int(event.get("trade_count", 0)),
            "max_absorption_rate": float(event.get("absorption_rate", 0.0)),
            "avg_absorption_rate": float(event.get("absorption_rate", 0.0)),
            "max_confidence": float(event.get("confidence", 0.0)),
            "positive_score": 0.0,
            "negative_score": 0.0,
            "net_score": 0.0,
            "reload_count": 0,
            "stress_count": 1 if initial_state == "STRESSED" else 0,
            "broken_count": 0,
            "last_result": result,
            "last_quality": event.get("quality"),
            "_events": [dict(event)],
        }
        self._recalculate_score(zone)
        self.zones.append(zone)
        return zone

    def _update_zone(self, zone: Dict[str, Any], event: Dict[str, Any], current_price: float = 0.0) -> Dict[str, Any]:
        previous_lower = float(zone.get("zone_lower", 0.0))
        previous_upper = float(zone.get("zone_upper", 0.0))
        event_lower = float(event.get("zone_lower", 0.0))
        event_upper = float(event.get("zone_upper", 0.0))
        result = str(event.get("result") or "")

        old_count = max(1, int(zone.get("event_count", 0)))
        zone["zone_lower"] = min(previous_lower, event_lower)
        zone["zone_upper"] = max(previous_upper, event_upper)
        zone["anchor_price"] = (
            float(zone.get("anchor_price", 0.0)) * old_count + float(event.get("trigger_price", 0.0))
        ) / (old_count + 1)

        zone["last_seen_ts"] = float(event.get("ts", 0.0))
        zone["last_event_id"] = event.get("event_id")
        zone["event_count"] = int(zone.get("event_count", 0)) + 1
        zone["iceberg_count"] = int(zone.get("iceberg_count", 0)) + (1 if result == "ICEBERG" else 0)
        zone["ignore_count"] = int(zone.get("ignore_count", 0)) + (1 if result == "IGNORE" else 0)
        zone["spoof_count"] = int(zone.get("spoof_count", 0)) + (1 if result == "SPOOFING" else 0)
        zone["cancel_count"] = int(zone.get("cancel_count", 0)) + (1 if result == "CANCEL" else 0)

        quality = event.get("quality")
        zone["high_count"] = int(zone.get("high_count", 0)) + (1 if quality == "HIGH" else 0)
        zone["medium_count"] = int(zone.get("medium_count", 0)) + (1 if quality == "MEDIUM" else 0)
        zone["low_count"] = int(zone.get("low_count", 0)) + (1 if quality == "LOW" else 0)

        zone["total_active_volume"] = float(zone.get("total_active_volume", 0.0)) + float(event.get("active_volume", 0.0))
        zone["total_hidden_volume"] = float(zone.get("total_hidden_volume", 0.0)) + float(event.get("hidden_volume", 0.0))
        zone["total_capped_hidden_volume"] = float(zone.get("total_capped_hidden_volume", 0.0)) + self._capped_hidden_volume(event)
        zone["total_book_reduction"] = float(zone.get("total_book_reduction", 0.0)) + float(event.get("book_reduction", 0.0))
        zone["total_trade_count"] = int(zone.get("total_trade_count", 0)) + int(event.get("trade_count", 0))

        absorption_rate = float(event.get("absorption_rate", 0.0))
        zone["max_absorption_rate"] = max(float(zone.get("max_absorption_rate", 0.0)), absorption_rate)
        zone["avg_absorption_rate"] = self._average_absorption_rate(zone, absorption_rate)
        zone["max_confidence"] = max(float(zone.get("max_confidence", 0.0)), float(event.get("confidence", 0.0)))

        zone["last_result"] = result
        zone["last_quality"] = quality
        zone.setdefault("_events", []).append(dict(event))

        self._recalculate_score(zone)
        self._update_state(
            zone,
            event,
            current_price=current_price,
            previous_lower=previous_lower,
            previous_upper=previous_upper,
        )
        return zone

    def _recalculate_score(self, zone: Dict[str, Any]) -> None:
        positive_score = 0.0
        negative_score = 0.0

        for event in zone.get("_events", []):
            result = str(event.get("result") or "")
            active_volume = float(event.get("active_volume", 0.0))
            hidden_volume = float(event.get("hidden_volume", 0.0))
            absorption_rate = min(
                float(event.get("absorption_rate", 0.0)),
                self.absorption_rate_score_cap,
            )
            confidence = float(event.get("confidence", 0.0))
            capped_hidden = self._capped_hidden_volume(event)

            if result == "ICEBERG":
                positive_score += self.quality_weight.get(event.get("quality"), 0.0)
                positive_score += min(capped_hidden / 1_000_000.0, 3.0)
                if absorption_rate >= 0.8:
                    positive_score += 0.5
                if confidence >= 0.85:
                    positive_score += 0.5
            elif result == "IGNORE":
                negative_score += min(active_volume / 1_000_000.0, 3.0)
                if absorption_rate < 0.3:
                    negative_score += 1.0
            elif result == "SPOOFING":
                negative_score += min(abs(hidden_volume) / 1_000_000.0, 3.0)
                negative_score += 1.5
            elif result == "CANCEL":
                negative_score += 2.0

        zone["positive_score"] = positive_score
        zone["negative_score"] = negative_score
        zone["net_score"] = positive_score - negative_score

    def _update_state(
        self,
        zone: Dict[str, Any],
        event: Dict[str, Any],
        current_price: float = 0.0,
        previous_lower: Optional[float] = None,
        previous_upper: Optional[float] = None,
    ) -> None:
        old_state = str(zone.get("state") or "DISCOVERED")
        result = str(event.get("result") or "")
        direction = str(zone.get("direction") or "")
        zone_lower = float(zone.get("zone_lower", 0.0))
        zone_upper = float(zone.get("zone_upper", 0.0))
        check_lower = float(previous_lower if previous_lower is not None else zone_lower)
        check_upper = float(previous_upper if previous_upper is not None else zone_upper)
        event_lower = float(event.get("zone_lower", 0.0))
        event_upper = float(event.get("zone_upper", 0.0))
        current = float(current_price or 0.0)
        cancel_reason = event.get("cancel_reason")

        broke_zone = False
        if direction == "BUY":
            broke_zone = (
                (current > 0 and current < check_lower - self.broken_buffer_usdt)
                or cancel_reason == "PRICE_BROKE_DOWN"
            )
        elif direction == "SELL":
            broke_zone = (
                (current > 0 and current > check_upper + self.broken_buffer_usdt)
                or cancel_reason == "PRICE_BROKE_UP"
            )

        if broke_zone:
            zone["state"] = "BROKEN"
            zone["broken_count"] = int(zone.get("broken_count", 0)) + 1
            if old_state != "BROKEN":
                self._append_finalized_zone(zone)
                logger.info(
                    "[ICEBERG-ZONE-BROKEN] id=%s direction=%s event=%s current=%.2f old_zone=[%.2f,%.2f] reason=price_broke_zone",
                    zone.get("zone_id"),
                    direction,
                    event.get("event_id"),
                    current,
                    check_lower,
                    check_upper,
                )
            return

        if zone.get("state") in ("BROKEN", "EXPIRED"):
            return

        iceberg_count = int(zone.get("iceberg_count", 0))
        event_count = int(zone.get("event_count", 0))
        ignore_count = int(zone.get("ignore_count", 0))
        spoof_count = int(zone.get("spoof_count", 0))
        positive_score = float(zone.get("positive_score", 0.0))
        negative_score = float(zone.get("negative_score", 0.0))
        negative_count = ignore_count + spoof_count

        new_state = str(zone.get("state") or "DISCOVERED")
        if result in ("IGNORE", "SPOOFING") and negative_count >= iceberg_count:
            new_state = "STRESSED"
        elif new_state == "DISCOVERED" and (iceberg_count >= 2 or positive_score >= 4.0):
            new_state = "ACTIVE"
        elif new_state == "ACTIVE":
            if result == "ICEBERG" and event_count >= 3:
                new_state = "RELOADING"
            elif negative_count >= iceberg_count and negative_score >= positive_score * 0.7:
                new_state = "STRESSED"
        elif new_state == "RELOADING":
            if negative_count >= iceberg_count and negative_score >= positive_score * 0.7:
                new_state = "STRESSED"
        elif new_state == "STRESSED":
            if result == "ICEBERG" and positive_score > negative_score + 1.0:
                new_state = "RELOADING" if event_count >= 3 else "ACTIVE"

        if new_state == "RELOADING" and old_state != "RELOADING":
            zone["reload_count"] = int(zone.get("reload_count", 0)) + 1
        if new_state == "STRESSED" and old_state != "STRESSED":
            zone["stress_count"] = int(zone.get("stress_count", 0)) + 1
            logger.info(
                "[ICEBERG-ZONE-STRESSED] id=%s direction=%s result=%s event=%s reason=negative_pressure pos=%.2f neg=%.2f net=%.2f",
                zone.get("zone_id"),
                direction,
                result,
                event.get("event_id"),
                positive_score,
                negative_score,
                float(zone.get("net_score", 0.0)),
            )

        zone["state"] = new_state

    def _log_zone_update(self, zone: Dict[str, Any], event: Dict[str, Any], action: str) -> None:
        if action == "NEW":
            logger.info(
                "[ICEBERG-ZONE-NEW] id=%s direction=%s state=%s result=%s event=%s zone=[%.2f,%.2f] quality=%s score=%.2f",
                zone.get("zone_id"),
                zone.get("direction"),
                zone.get("state"),
                event.get("result"),
                event.get("event_id"),
                float(zone.get("zone_lower", 0.0)),
                float(zone.get("zone_upper", 0.0)),
                event.get("quality"),
                float(zone.get("net_score", 0.0)),
            )
            return

        logger.info(
            "[ICEBERG-ZONE-UPDATE] id=%s direction=%s state=%s result=%s event=%s events=%d icebergs=%d ignore=%d spoof=%d pos=%.2f neg=%.2f net=%.2f hidden=%.0fU capped_hidden=%.0fU zone=[%.2f,%.2f]",
            zone.get("zone_id"),
            zone.get("direction"),
            zone.get("state"),
            event.get("result"),
            event.get("event_id"),
            int(zone.get("event_count", 0)),
            int(zone.get("iceberg_count", 0)),
            int(zone.get("ignore_count", 0)),
            int(zone.get("spoof_count", 0)),
            float(zone.get("positive_score", 0.0)),
            float(zone.get("negative_score", 0.0)),
            float(zone.get("net_score", 0.0)),
            float(zone.get("total_hidden_volume", 0.0)),
            float(zone.get("total_capped_hidden_volume", 0.0)),
            float(zone.get("zone_lower", 0.0)),
            float(zone.get("zone_upper", 0.0)),
        )

    def _normalize_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(event, dict):
            return None

        now = time.time()
        direction = str(event.get("direction") or "").upper()
        result = str(event.get("result") or "").upper()
        if direction not in ("BUY", "SELL") or result not in ("ICEBERG", "IGNORE", "SPOOFING", "CANCEL"):
            return None

        trigger_price = self._safe_float(event.get("trigger_price"), 0.0)
        zone_lower = self._safe_float(event.get("zone_lower"), trigger_price)
        zone_upper = self._safe_float(event.get("zone_upper"), trigger_price)
        if zone_lower > zone_upper:
            zone_lower, zone_upper = zone_upper, zone_lower

        ts = self._safe_float(event.get("ts"), 0.0) or self._safe_float(event.get("recv_ts"), 0.0) or now
        recv_ts = self._safe_float(event.get("recv_ts"), ts)
        quality = event.get("quality")
        if quality not in self.quality_weight:
            quality = None

        return {
            "event_id": str(event.get("event_id") or f"ice-{int(now * 1000)}"),
            "ts": ts,
            "recv_ts": recv_ts,
            "direction": direction,
            "result": result,
            "quality": quality,
            "trigger_price": trigger_price,
            "zone_lower": zone_lower,
            "zone_upper": zone_upper,
            "active_volume": self._safe_float(event.get("active_volume"), 0.0),
            "hidden_volume": self._safe_float(event.get("hidden_volume"), 0.0),
            "absorption_rate": self._safe_float(event.get("absorption_rate"), 0.0),
            "confidence": self._safe_float(event.get("confidence"), 0.0),
            "book_reduction": self._safe_float(event.get("book_reduction"), 0.0),
            "trade_count": self._safe_int(event.get("trade_count"), 0),
            "wait_ms": self._safe_float(event.get("wait_ms"), 0.0),
            "behavior": str(event.get("behavior") or ""),
            "cancel_reason": event.get("cancel_reason"),
        }

    def _capped_hidden_volume(self, event: Dict[str, Any]) -> float:
        hidden_volume = max(0.0, float(event.get("hidden_volume", 0.0)))
        active_volume = max(0.0, float(event.get("active_volume", 0.0)))
        return min(hidden_volume, active_volume * self.hidden_vs_active_score_cap)

    @staticmethod
    def _interval_distance(a_lower: float, a_upper: float, b_lower: float, b_upper: float) -> float:
        if a_lower <= b_upper and b_lower <= a_upper:
            return 0.0
        return min(abs(a_lower - b_upper), abs(a_upper - b_lower))

    @staticmethod
    def _average_absorption_rate(zone: Dict[str, Any], newest_rate: float) -> float:
        count = max(1, int(zone.get("event_count", 1)))
        previous_count = max(0, count - 1)
        previous_avg = float(zone.get("avg_absorption_rate", 0.0))
        return (previous_avg * previous_count + newest_rate) / count

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(default)

    @staticmethod
    def _public_zone(zone: Dict[str, Any]) -> Dict[str, Any]:
        return {key: value for key, value in zone.items() if not str(key).startswith("_")}

    def _prune_zones(self) -> None:
        if len(self.zones) <= self.max_active_zones:
            return
        self.zones.sort(
            key=lambda zone: (
                str(zone.get("state")) not in ("BROKEN", "EXPIRED"),
                float(zone.get("last_seen_ts", 0.0)),
            )
        )
        del self.zones[: max(0, len(self.zones) - self.max_active_zones)]

    def drain_finalized_zones(self) -> List[Dict[str, Any]]:
        finalized = list(self._pending_finalized_zones)
        self._pending_finalized_zones.clear()
        return finalized

    def _append_finalized_zone(self, zone: Dict[str, Any]) -> None:
        if zone.get("_finalized_emitted"):
            return
        zone["_finalized_emitted"] = True
        self._pending_finalized_zones.append(self._public_zone(zone))
