#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class IcebergOutcomeEvaluator:
    def __init__(
        self,
        horizons_sec=None,
        stop_loss_offsets_usdt=None,
        stop_loss_offsets_pct=None,
        max_tracking_zones=500,
    ):
        self.horizons_sec = horizons_sec or {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "1h": 3600,
            "4h": 14400,
        }
        self.stop_loss_offsets_usdt = stop_loss_offsets_usdt or [0.5, 1.0, 1.5, 2.0, 3.0]
        self.stop_loss_offsets_pct = stop_loss_offsets_pct or [0.001, 0.0015, 0.002]
        self.max_tracking_zones = self._safe_int(max_tracking_zones, 500)

        self._tracking_items: List[Dict[str, Any]] = []
        self._registered_ids = set()

    def register_zone(self, zone: dict, now_ts: float, current_price: float = 0.0) -> None:
        zid = str(zone.get("zone_id") or "")
        if not zid or zid in self._registered_ids:
            return
        direction = str(zone.get("direction") or "")
        if direction not in ("BUY", "SELL"):
            return

        zone_lower = self._safe_float(zone.get("zone_lower"), 0.0)
        zone_upper = self._safe_float(zone.get("zone_upper"), 0.0)
        now = self._safe_float(now_ts, 0.0)
        cp = self._safe_float(current_price, 0.0)
        if cp > 0:
            ref = cp
        else:
            ref = zone_upper if direction == "BUY" else zone_lower

        item = {
            "zone_id": zid,
            "direction": direction,
            "zone_lower": zone_lower,
            "zone_upper": zone_upper,
            "anchor_price": self._safe_float(zone.get("anchor_price"), 0.0),
            "first_seen_ts": self._safe_float(zone.get("first_seen_ts"), now),
            "last_seen_ts": self._safe_float(zone.get("last_seen_ts"), now),
            "registered_ts": now,
            "reference_price": ref,
            "final_state": str(zone.get("state") or ""),
            "previous_state": zone.get("previous_state"),
            "event_count": self._safe_int(zone.get("event_count"), 0),
            "iceberg_count": self._safe_int(zone.get("iceberg_count"), 0),
            "ignore_count": self._safe_int(zone.get("ignore_count"), 0),
            "spoof_count": self._safe_int(zone.get("spoof_count"), 0),
            "cancel_count": self._safe_int(zone.get("cancel_count"), 0),
            "high_count": self._safe_int(zone.get("high_count"), 0),
            "medium_count": self._safe_int(zone.get("medium_count"), 0),
            "low_count": self._safe_int(zone.get("low_count"), 0),
            "positive_score": self._safe_float(zone.get("positive_score"), 0.0),
            "negative_score": self._safe_float(zone.get("negative_score"), 0.0),
            "net_score": self._safe_float(zone.get("net_score"), 0.0),
            "min_price_seen": ref,
            "max_price_seen": ref,
            "broke_iceberg_boundary": False,
            "boundary_break_ts": None,
            "boundary_break_depth_usdt": 0.0,
            "boundary_break_depth_pct": 0.0,
            "reclaimed_iceberg_boundary": False,
            "boundary_reclaim_ts": None,
            "reclaimed_opposite_boundary": False,
            "opposite_boundary_reclaim_ts": None,
            "emitted_horizons": set(),
            "completed": False,
        }
        self._tracking_items.append(item)
        self._registered_ids.add(zid)
        self._prune_completed()

    def on_price(self, price: float, ts: float) -> None:
        p = self._safe_float(price, 0.0)
        t = self._safe_float(ts, 0.0)
        if p <= 0 or t <= 0:
            return

        for item in self._tracking_items:
            if item.get("completed"):
                continue
            self._update_tracking_zone(item, p, t)
            self._emit_outcome_if_due(item, t)
        self._prune_completed()

    def _update_tracking_zone(self, item: dict, price: float, ts: float) -> None:
        item["min_price_seen"] = min(self._safe_float(item.get("min_price_seen"), price), price)
        item["max_price_seen"] = max(self._safe_float(item.get("max_price_seen"), price), price)
        direction = item.get("direction")
        zone_lower = self._safe_float(item.get("zone_lower"), 0.0)
        zone_upper = self._safe_float(item.get("zone_upper"), 0.0)

        if direction == "BUY":
            if price < zone_lower:
                item["broke_iceberg_boundary"] = True
                if item.get("boundary_break_ts") is None:
                    item["boundary_break_ts"] = ts
            if item.get("broke_iceberg_boundary") and price >= zone_lower and not item.get("reclaimed_iceberg_boundary"):
                item["reclaimed_iceberg_boundary"] = True
                item["boundary_reclaim_ts"] = ts
            if price >= zone_upper and not item.get("reclaimed_opposite_boundary"):
                item["reclaimed_opposite_boundary"] = True
                item["opposite_boundary_reclaim_ts"] = ts
            depth = max(zone_lower - self._safe_float(item.get("min_price_seen"), zone_lower), 0.0)
            item["boundary_break_depth_usdt"] = depth
            item["boundary_break_depth_pct"] = depth / zone_lower if zone_lower > 0 else 0.0
        else:
            if price > zone_upper:
                item["broke_iceberg_boundary"] = True
                if item.get("boundary_break_ts") is None:
                    item["boundary_break_ts"] = ts
            if item.get("broke_iceberg_boundary") and price <= zone_upper and not item.get("reclaimed_iceberg_boundary"):
                item["reclaimed_iceberg_boundary"] = True
                item["boundary_reclaim_ts"] = ts
            if price <= zone_lower and not item.get("reclaimed_opposite_boundary"):
                item["reclaimed_opposite_boundary"] = True
                item["opposite_boundary_reclaim_ts"] = ts
            depth = max(self._safe_float(item.get("max_price_seen"), zone_upper) - zone_upper, 0.0)
            item["boundary_break_depth_usdt"] = depth
            item["boundary_break_depth_pct"] = depth / zone_upper if zone_upper > 0 else 0.0

    def _emit_outcome_if_due(self, item: dict, ts: float) -> None:
        elapsed = ts - self._safe_float(item.get("registered_ts"), ts)
        for label, horizon_sec in self.horizons_sec.items():
            if label in item.get("emitted_horizons", set()):
                continue
            if elapsed < self._safe_float(horizon_sec, 0.0):
                continue
            snap = self._calculate_snapshot(item, label, ts)
            self._log_outcome(snap)
            item["emitted_horizons"].add(label)

        if len(item.get("emitted_horizons", set())) >= len(self.horizons_sec):
            item["completed"] = True

    def _calculate_snapshot(self, item: dict, horizon_label: str, ts: float) -> dict:
        direction = item.get("direction")
        ref = self._safe_float(item.get("reference_price"), 0.0)
        min_p = self._safe_float(item.get("min_price_seen"), ref)
        max_p = self._safe_float(item.get("max_price_seen"), ref)

        if direction == "BUY":
            mfe = max_p - ref
            mae = min_p - ref
        else:
            mfe = ref - min_p
            mae = ref - max_p

        mfe_pct = mfe / ref if ref > 0 else 0.0
        mae_pct = mae / ref if ref > 0 else 0.0

        snapshot = dict(item)
        snapshot.update({
            "horizon": horizon_label,
            "snapshot_ts": ts,
            "mfe": mfe,
            "mae": mae,
            "mfe_pct": mfe_pct,
            "mae_pct": mae_pct,
            "label": self._determine_label({"mfe_pct": mfe_pct, "mae_pct": mae_pct, **item}),
        })

        zone_lower = self._safe_float(item.get("zone_lower"), 0.0)
        zone_upper = self._safe_float(item.get("zone_upper"), 0.0)
        for offset in self.stop_loss_offsets_usdt:
            key = f"sl_{str(offset).replace('.', '_')}u_hit"
            if direction == "BUY":
                snapshot[key] = min_p <= zone_lower - self._safe_float(offset, 0.0)
            else:
                snapshot[key] = max_p >= zone_upper + self._safe_float(offset, 0.0)

        for pct in self.stop_loss_offsets_pct:
            pct_text = str(pct * 100.0).rstrip("0").rstrip(".")
            key = f"sl_{pct_text.replace('.', '_')}pct_hit"
            if direction == "BUY":
                snapshot[key] = min_p <= zone_lower * (1.0 - pct)
            else:
                snapshot[key] = max_p >= zone_upper * (1.0 + pct)

        return snapshot

    def _determine_label(self, snapshot: Dict[str, Any]) -> str:
        broke = bool(snapshot.get("broke_iceberg_boundary"))
        reclaimed = bool(snapshot.get("reclaimed_iceberg_boundary"))
        mfe_pct = self._safe_float(snapshot.get("mfe_pct"), 0.0)
        mae_pct = self._safe_float(snapshot.get("mae_pct"), 0.0)

        if (not broke) and mfe_pct > 0:
            return "CLEAN_HOLD"
        if broke and reclaimed and mfe_pct > 0:
            return "SWEPT_THEN_RECLAIMED"
        if broke and (not reclaimed) and mae_pct < -0.001:
            return "FAILED_ABSORPTION"
        if abs(mfe_pct) < 0.001 and abs(mae_pct) < 0.001:
            return "WEAK_REACTION"
        if mfe_pct <= 0 and mae_pct < -0.001:
            return "BAD_LOCATION"
        return "MIXED"

    def _log_outcome(self, snapshot: dict) -> None:
        direction = snapshot.get("direction")
        broke_field = "broke_iceberg_low" if direction == "BUY" else "broke_iceberg_high"
        reclaim_field = "reclaimed_iceberg_low" if direction == "BUY" else "reclaimed_iceberg_high"
        logger.info(
            "[ICEBERG-ZONE-OUTCOME] id=%s horizon=%s direction=%s final_state=%s previous_state=%s zone=[%.2f,%.2f] ref=%.2f "
            "events=%d icebergs=%d ignore=%d spoof=%d pos=%.2f neg=%.2f net=%.2f mfe=%.2fU mfe_pct=%.3f%% mae=%.2fU mae_pct=%.3f%% "
            "min=%.2f max=%.2f %s=%s break_depth=%.2fU break_depth_pct=%.3f%% %s=%s reclaimed_opposite_boundary=%s "
            "sl_0_5u_hit=%s sl_1_0u_hit=%s sl_1_5u_hit=%s sl_2_0u_hit=%s sl_3_0u_hit=%s "
            "sl_0_1pct_hit=%s sl_0_15pct_hit=%s sl_0_2pct_hit=%s label=%s",
            snapshot.get("zone_id"),
            snapshot.get("horizon"),
            direction,
            snapshot.get("final_state"),
            snapshot.get("previous_state"),
            self._safe_float(snapshot.get("zone_lower"), 0.0),
            self._safe_float(snapshot.get("zone_upper"), 0.0),
            self._safe_float(snapshot.get("reference_price"), 0.0),
            self._safe_int(snapshot.get("event_count"), 0),
            self._safe_int(snapshot.get("iceberg_count"), 0),
            self._safe_int(snapshot.get("ignore_count"), 0),
            self._safe_int(snapshot.get("spoof_count"), 0),
            self._safe_float(snapshot.get("positive_score"), 0.0),
            self._safe_float(snapshot.get("negative_score"), 0.0),
            self._safe_float(snapshot.get("net_score"), 0.0),
            self._safe_float(snapshot.get("mfe"), 0.0),
            self._safe_float(snapshot.get("mfe_pct"), 0.0) * 100.0,
            self._safe_float(snapshot.get("mae"), 0.0),
            self._safe_float(snapshot.get("mae_pct"), 0.0) * 100.0,
            self._safe_float(snapshot.get("min_price_seen"), 0.0),
            self._safe_float(snapshot.get("max_price_seen"), 0.0),
            broke_field,
            str(bool(snapshot.get("broke_iceberg_boundary"))).lower(),
            self._safe_float(snapshot.get("boundary_break_depth_usdt"), 0.0),
            self._safe_float(snapshot.get("boundary_break_depth_pct"), 0.0) * 100.0,
            reclaim_field,
            str(bool(snapshot.get("reclaimed_iceberg_boundary"))).lower(),
            str(bool(snapshot.get("reclaimed_opposite_boundary"))).lower(),
            str(bool(snapshot.get("sl_0_5u_hit"))).lower(),
            str(bool(snapshot.get("sl_1_0u_hit"))).lower(),
            str(bool(snapshot.get("sl_1_5u_hit"))).lower(),
            str(bool(snapshot.get("sl_2_0u_hit"))).lower(),
            str(bool(snapshot.get("sl_3_0u_hit"))).lower(),
            str(bool(snapshot.get("sl_0_1pct_hit"))).lower(),
            str(bool(snapshot.get("sl_0_15pct_hit"))).lower(),
            str(bool(snapshot.get("sl_0_2pct_hit"))).lower(),
            snapshot.get("label"),
        )

    def _prune_completed(self) -> None:
        if len(self._tracking_items) <= self.max_tracking_zones:
            return
        self._tracking_items = [x for x in self._tracking_items if not x.get("completed")] + [
            x for x in self._tracking_items if x.get("completed")
        ]
        if len(self._tracking_items) <= self.max_tracking_zones:
            return
        self._tracking_items.sort(key=lambda x: self._safe_float(x.get("registered_ts"), 0.0))
        while len(self._tracking_items) > self.max_tracking_zones:
            dropped = self._tracking_items.pop(0)
            self._registered_ids.discard(str(dropped.get("zone_id") or ""))

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
