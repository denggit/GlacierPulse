#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
V6.2 Phase2 orderflow research evaluator skeleton.

Step1 only registers frozen A1 public zones for research. It does not place
orders, open virtual positions, or evaluate realtime entry conditions.
"""

import logging
import time
from collections import OrderedDict
from typing import Any, Dict, Optional

from config.research_evaluator import MAX_ACTIVE_PHASE2_ZONES, PHASE2_ZONE_TTL_SECONDS

logger = logging.getLogger(__name__)


class Phase2OrderflowEvaluator:
    def __init__(
        self,
        max_active_zones: int = MAX_ACTIVE_PHASE2_ZONES,
        zone_ttl_seconds: float = PHASE2_ZONE_TTL_SECONDS,
    ):
        self.max_active_zones = max(1, int(max_active_zones))
        self.zone_ttl_seconds = max(0.0, float(zone_ttl_seconds))
        self.active_zones: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

    def register_frozen_zone(self, zone: Dict[str, Any], now_ts: Optional[float] = None) -> bool:
        """
        Register one public frozen iceberg zone.

        Returns True only when a new zone_id is accepted. Duplicate zone_id
        inputs are ignored silently to preserve one [PHASE2-REGISTERED] log.
        """
        if not isinstance(zone, dict) or not zone.get("is_frozen"):
            return False

        zone_id = str(zone.get("zone_id") or "").strip()
        if not zone_id or zone_id in self.active_zones:
            return False

        now = float(now_ts if now_ts is not None else time.time())
        self._prune(now_ts=now, reserve_slots=1)

        snapshot = dict(zone)
        snapshot["phase2_registered_ts"] = now
        self.active_zones[zone_id] = snapshot
        self.active_zones.move_to_end(zone_id)

        logger.info(
            "[PHASE2-REGISTERED] zone_id=%s direction=%s frozen_ts=%.3f frozen_reason=%s frozen_state=%s frozen_event_id=%s frozen_low=%.2f frozen_high=%.2f live_low=%.2f live_high=%.2f",
            zone_id,
            snapshot.get("direction"),
            self._safe_float(snapshot.get("frozen_ts"), 0.0),
            snapshot.get("frozen_reason"),
            snapshot.get("frozen_state"),
            snapshot.get("frozen_event_id"),
            self._safe_float(snapshot.get("frozen_zone_lower"), 0.0),
            self._safe_float(snapshot.get("frozen_zone_upper"), 0.0),
            self._safe_float(snapshot.get("live_zone_lower", snapshot.get("zone_lower")), 0.0),
            self._safe_float(snapshot.get("live_zone_upper", snapshot.get("zone_upper")), 0.0),
        )
        return True

    def on_orderflow(self, data: Dict[str, Any]) -> None:
        """Reserved for tick/trades/books/orderflow data in later V6.2 steps."""
        return None

    def get_active_zone(self, zone_id: str) -> Optional[Dict[str, Any]]:
        zone = self.active_zones.get(str(zone_id))
        return dict(zone) if zone else None

    def _prune(self, now_ts: Optional[float] = None, reserve_slots: int = 0) -> None:
        now = float(now_ts if now_ts is not None else time.time())
        if self.zone_ttl_seconds > 0:
            expired_ids = [
                zone_id
                for zone_id, zone in self.active_zones.items()
                if now - self._safe_float(zone.get("phase2_registered_ts"), now) > self.zone_ttl_seconds
            ]
            for zone_id in expired_ids:
                self.active_zones.pop(zone_id, None)

        target_size = max(0, self.max_active_zones - max(0, int(reserve_slots)))
        while len(self.active_zones) > target_size:
            self.active_zones.popitem(last=False)

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)
