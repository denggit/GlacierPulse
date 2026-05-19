#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class IcebergOutcomeEvaluator:
    def __init__(self, horizons_sec=None, stop_loss_offsets_usdt=None, stop_loss_offsets_pct=None, max_tracking_zones=500):
        self.horizons_sec = horizons_sec or {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
        self.stop_loss_offsets_usdt = stop_loss_offsets_usdt or [0.5, 1.0, 1.5, 2.0, 3.0]
        self.stop_loss_offsets_pct = stop_loss_offsets_pct or [0.001, 0.0015, 0.002]
        self.max_tracking_zones = self._safe_int(max_tracking_zones, 500)
        self._tracking_items: List[Dict[str, Any]] = []
        self._items_by_id: Dict[str, Dict[str, Any]] = {}
        self._seen_zone_ids = set()

    def register_zone(self, zone: dict, now_ts: float, current_price: float = 0.0) -> None:
        self.upsert_zone(zone, now_ts=now_ts, current_price=current_price)

    def upsert_zone(self, zone: dict, now_ts: float, current_price: float = 0.0) -> None:
        zid = str(zone.get("zone_id") or "")
        direction = str(zone.get("direction") or "")
        if not zid or direction not in ("BUY", "SELL"):
            return
        now = self._safe_float(now_ts, 0.0)
        live_low = self._safe_float(zone.get("live_zone_lower"), self._safe_float(zone.get("zone_lower"), 0.0))
        live_high = self._safe_float(zone.get("live_zone_upper"), self._safe_float(zone.get("zone_upper"), 0.0))
        frozen = bool(zone.get("is_frozen"))
        fr_low = self._safe_float(zone.get("frozen_zone_lower"), 0.0) if frozen else None
        fr_high = self._safe_float(zone.get("frozen_zone_upper"), 0.0) if frozen else None
        cp = self._safe_float(current_price, 0.0)
        if cp > 0:
            ref = cp
        elif frozen and fr_low is not None and fr_high is not None:
            ref = fr_high if direction == "BUY" else fr_low
        else:
            ref = live_high if direction == "BUY" else live_low

        item = self._items_by_id.get(zid)
        if item is None:
            if zid in self._seen_zone_ids:
                return
            item = {
                "zone_id": zid, "direction": direction,
                "live_zone_lower": live_low, "live_zone_upper": live_high, "zone_lower": live_low, "zone_upper": live_high,
                "frozen_zone_lower": fr_low if frozen else None, "frozen_zone_upper": fr_high if frozen else None,
                "frozen_ts": zone.get("frozen_ts") if frozen else None, "frozen_reason": zone.get("frozen_reason") if frozen else None,
                "frozen_state": zone.get("frozen_state") if frozen else None, "frozen_event_id": zone.get("frozen_event_id") if frozen else None,
                "is_frozen": frozen,
                "anchor_price": self._safe_float(zone.get("anchor_price"), 0.0), "first_seen_ts": self._safe_float(zone.get("first_seen_ts"), now),
                "last_seen_ts": self._safe_float(zone.get("last_seen_ts"), now), "registered_ts": now, "reference_price": ref,
                "current_state": str(zone.get("state") or ""), "final_state": None, "previous_state": None,
                "event_count": self._safe_int(zone.get("event_count"), 0), "iceberg_count": self._safe_int(zone.get("iceberg_count"), 0),
                "ignore_count": self._safe_int(zone.get("ignore_count"), 0), "spoof_count": self._safe_int(zone.get("spoof_count"), 0),
                "cancel_count": self._safe_int(zone.get("cancel_count"), 0), "high_count": self._safe_int(zone.get("high_count"), 0),
                "medium_count": self._safe_int(zone.get("medium_count"), 0), "low_count": self._safe_int(zone.get("low_count"), 0),
                "positive_score": self._safe_float(zone.get("positive_score"), 0.0), "negative_score": self._safe_float(zone.get("negative_score"), 0.0),
                "net_score": self._safe_float(zone.get("net_score"), 0.0), "min_price_seen": ref, "max_price_seen": ref,
                "emitted_horizons": set(), "completed": False,
            }
            self._reset_boundary_fields(item)
            self._tracking_items.append(item); self._items_by_id[zid] = item; self._seen_zone_ids.add(zid)
            self._recompute_boundary_states(item)
            self._prune_completed(); return

        item["live_zone_lower"] = min(self._safe_float(item.get("live_zone_lower"), live_low), live_low)
        item["live_zone_upper"] = max(self._safe_float(item.get("live_zone_upper"), live_high), live_high)
        item["zone_lower"] = item["live_zone_lower"]; item["zone_upper"] = item["live_zone_upper"]
        if (not item.get("is_frozen")) and frozen:
            item["frozen_zone_lower"] = fr_low; item["frozen_zone_upper"] = fr_high; item["frozen_ts"] = zone.get("frozen_ts")
            item["frozen_reason"] = zone.get("frozen_reason"); item["frozen_state"] = zone.get("frozen_state")
            item["frozen_event_id"] = zone.get("frozen_event_id"); item["is_frozen"] = True
        item["anchor_price"] = self._safe_float(zone.get("anchor_price"), 0.0)
        item["last_seen_ts"] = self._safe_float(zone.get("last_seen_ts"), now)
        item["current_state"] = str(zone.get("state") or "")
        for k in ("event_count","iceberg_count","ignore_count","spoof_count","cancel_count","high_count","medium_count","low_count"):
            item[k] = self._safe_int(zone.get(k), 0)
        for k in ("positive_score","negative_score","net_score"):
            item[k] = self._safe_float(zone.get(k), 0.0)
        self._recompute_boundary_states(item)

    def finalize_zone(self, zone: dict, now_ts: float, current_price: float = 0.0) -> None:
        zid = str(zone.get("zone_id") or "")
        if not zid:
            return
        if zid not in self._items_by_id:
            self.upsert_zone(zone, now_ts=now_ts, current_price=current_price)
        item = self._items_by_id.get(zid)
        if item is None:
            return
        self.upsert_zone(zone, now_ts=now_ts, current_price=current_price)
        item["final_state"] = str(zone.get("state") or "")
        item["previous_state"] = zone.get("previous_state")
        item["finalized_ts"] = self._safe_float(now_ts, 0.0)
        item["current_state"] = str(zone.get("state") or "")

    def on_price(self, price: float, ts: float) -> None:
        p, t = self._safe_float(price, 0.0), self._safe_float(ts, 0.0)
        if p <= 0 or t <= 0:
            return
        for item in self._tracking_items:
            if item.get("completed"):
                continue
            item["min_price_seen"] = min(self._safe_float(item.get("min_price_seen"), p), p)
            item["max_price_seen"] = max(self._safe_float(item.get("max_price_seen"), p), p)
            self._recompute_boundary_states(item, price=p, ts=t)
            self._emit_outcome_if_due(item, t)
        self._prune_completed()

    def _reset_boundary_fields(self, item):
        for k in ["broke_frozen_iceberg_low","reclaimed_frozen_iceberg_low","reclaimed_frozen_opposite_boundary","broke_frozen_iceberg_high","reclaimed_frozen_iceberg_high","broke_live_iceberg_low","reclaimed_live_iceberg_low","reclaimed_live_opposite_boundary","broke_live_iceberg_high","reclaimed_live_iceberg_high"]:
            item[k] = False
        item["frozen_break_depth_usdt"] = item["frozen_break_depth_pct"] = item["live_break_depth_usdt"] = item["live_break_depth_pct"] = 0.0

    def _recompute_boundary_states(self, item, price=None, ts=None):
        d = item.get("direction"); minp=self._safe_float(item.get("min_price_seen"),0.0); maxp=self._safe_float(item.get("max_price_seen"),0.0)
        ll=self._safe_float(item.get("live_zone_lower"),0.0); lh=self._safe_float(item.get("live_zone_upper"),0.0)
        if d=="BUY":
            item["broke_live_iceberg_low"] = minp < ll
            item["reclaimed_live_iceberg_low"] = bool(item.get("reclaimed_live_iceberg_low")) or (
                item["broke_live_iceberg_low"] and price is not None and price >= ll
            )
            item["reclaimed_live_opposite_boundary"] = bool(item.get("reclaimed_live_opposite_boundary")) or (price is not None and price >= lh)
            item["live_break_depth_usdt"] = max(ll-minp,0.0); item["live_break_depth_pct"] = item["live_break_depth_usdt"]/ll if ll>0 else 0.0
        else:
            item["broke_live_iceberg_high"] = maxp > lh
            item["reclaimed_live_iceberg_high"] = bool(item.get("reclaimed_live_iceberg_high")) or (
                item["broke_live_iceberg_high"] and price is not None and price <= lh
            )
            item["reclaimed_live_opposite_boundary"] = bool(item.get("reclaimed_live_opposite_boundary")) or (price is not None and price <= ll)
            item["live_break_depth_usdt"] = max(maxp-lh,0.0); item["live_break_depth_pct"] = item["live_break_depth_usdt"]/lh if lh>0 else 0.0
        if not item.get("is_frozen"):
            return
        fl=self._safe_float(item.get("frozen_zone_lower"),0.0); fh=self._safe_float(item.get("frozen_zone_upper"),0.0)
        if d=="BUY":
            item["broke_frozen_iceberg_low"] = minp < fl
            item["reclaimed_frozen_iceberg_low"] = bool(item.get("reclaimed_frozen_iceberg_low")) or (
                item["broke_frozen_iceberg_low"] and price is not None and price >= fl
            )
            item["reclaimed_frozen_opposite_boundary"] = bool(item.get("reclaimed_frozen_opposite_boundary")) or (price is not None and price >= fh)
            item["frozen_break_depth_usdt"] = max(fl-minp,0.0); item["frozen_break_depth_pct"] = item["frozen_break_depth_usdt"]/fl if fl>0 else 0.0
        else:
            item["broke_frozen_iceberg_high"] = maxp > fh
            item["reclaimed_frozen_iceberg_high"] = bool(item.get("reclaimed_frozen_iceberg_high")) or (
                item["broke_frozen_iceberg_high"] and price is not None and price <= fh
            )
            item["reclaimed_frozen_opposite_boundary"] = bool(item.get("reclaimed_frozen_opposite_boundary")) or (price is not None and price <= fl)
            item["frozen_break_depth_usdt"] = max(maxp-fh,0.0); item["frozen_break_depth_pct"] = item["frozen_break_depth_usdt"]/fh if fh>0 else 0.0

    def _emit_outcome_if_due(self, item: dict, ts: float) -> None:
        elapsed = ts - self._safe_float(item.get("registered_ts"), ts)
        for label, horizon_sec in self.horizons_sec.items():
            if label in item.get("emitted_horizons", set()) or elapsed < self._safe_float(horizon_sec, 0.0):
                continue
            snap = self._calculate_snapshot(item, label, ts); self._log_outcome(snap); item["emitted_horizons"].add(label)
        if len(item.get("emitted_horizons", set())) >= len(self.horizons_sec): item["completed"] = True

    def _calculate_snapshot(self, item: dict, horizon_label: str, ts: float) -> dict:
        direction=item.get("direction"); ref=self._safe_float(item.get("reference_price"),0.0); min_p=self._safe_float(item.get("min_price_seen"),ref); max_p=self._safe_float(item.get("max_price_seen"),ref)
        mfe,maxe=((max_p-ref,min_p-ref) if direction=="BUY" else (ref-min_p,ref-max_p)); mfe_pct=mfe/ref if ref>0 else 0.0; mae_pct=maxe/ref if ref>0 else 0.0
        s=dict(item); s.update({"horizon":horizon_label,"snapshot_ts":ts,"mfe":mfe,"mae":maxe,"mfe_pct":mfe_pct,"mae_pct":mae_pct})
        for prefix,low,high in (("frozen",item.get("frozen_zone_lower"),item.get("frozen_zone_upper")),("live",item.get("live_zone_lower"),item.get("live_zone_upper"))):
            zl=self._safe_float(low,0.0); zu=self._safe_float(high,0.0); enabled=(prefix=="live") or bool(item.get("is_frozen"))
            for o in self.stop_loss_offsets_usdt:
                key=f"{prefix}_sl_{str(o).replace('.', '_')}u_hit"; s[key]=enabled and ((min_p<=zl-o) if direction=="BUY" else (max_p>=zu+o))
            for pct in self.stop_loss_offsets_pct:
                txt=str(pct*100.0).rstrip("0").rstrip("."); key=f"{prefix}_sl_{txt.replace('.', '_')}pct_hit"; s[key]=enabled and ((min_p<=zl*(1-pct)) if direction=="BUY" else (max_p>=zu*(1+pct)))
        s["label"]=self._determine_label({**item,"mfe_pct":mfe_pct,"mae_pct":mae_pct})
        return s

    def _determine_label(self, snapshot: Dict[str, Any]) -> str:
        if not snapshot.get("is_frozen"): return "UNFROZEN"
        d=snapshot.get("direction"); broke=bool(snapshot.get("broke_frozen_iceberg_low" if d=="BUY" else "broke_frozen_iceberg_high")); reclaimed=bool(snapshot.get("reclaimed_frozen_iceberg_low" if d=="BUY" else "reclaimed_frozen_iceberg_high")); mfe_pct=self._safe_float(snapshot.get("mfe_pct"),0.0); mae_pct=self._safe_float(snapshot.get("mae_pct"),0.0)
        if (not broke) and mfe_pct > 0: return "CLEAN_HOLD"
        if broke and reclaimed and mfe_pct > 0: return "SWEPT_THEN_RECLAIMED"
        if broke and (not reclaimed) and mae_pct < -0.001: return "FAILED_ABSORPTION"
        if abs(mfe_pct)<0.001 and abs(mae_pct)<0.001: return "WEAK_REACTION"
        if mfe_pct<=0 and mae_pct < -0.001: return "BAD_LOCATION"
        return "MIXED"

    def _log_outcome(self, s: dict) -> None:
        direction = s.get("direction")
        if direction == "BUY":
            reclaimed_frozen_low = bool(s.get("reclaimed_frozen_iceberg_low"))
            reclaimed_frozen_high = bool(s.get("reclaimed_frozen_opposite_boundary"))
            reclaimed_live_low = bool(s.get("reclaimed_live_iceberg_low"))
            reclaimed_live_high = bool(s.get("reclaimed_live_opposite_boundary"))
        elif direction == "SELL":
            reclaimed_frozen_high = bool(s.get("reclaimed_frozen_iceberg_high"))
            reclaimed_frozen_low = bool(s.get("reclaimed_frozen_opposite_boundary"))
            reclaimed_live_high = bool(s.get("reclaimed_live_iceberg_high"))
            reclaimed_live_low = bool(s.get("reclaimed_live_opposite_boundary"))
        else:
            reclaimed_frozen_low = False
            reclaimed_frozen_high = False
            reclaimed_live_low = False
            reclaimed_live_high = False

        logger.info("[ICEBERG-ZONE-OUTCOME] id=%s horizon=%s direction=%s current_state=%s finalized=%s final_state=%s previous_state=%s is_frozen=%s frozen_reason=%s frozen_state=%s frozen_event=%s tracking_age=%.1fs finalized_ts=%.3f live_zone=[%.2f,%.2f] frozen_zone=[%.2f,%.2f] ref=%.2f events=%d icebergs=%d ignore=%d spoof=%d pos=%.2f neg=%.2f net=%.2f mfe=%.2fU mfe_pct=%.3f%% mae=%.2fU mae_pct=%.3f%% min=%.2f max=%.2f broke_frozen_low=%s broke_frozen_high=%s frozen_break_depth=%.2fU frozen_break_depth_pct=%.3f%% reclaimed_frozen_low=%s reclaimed_frozen_high=%s broke_live_low=%s broke_live_high=%s live_break_depth=%.2fU live_break_depth_pct=%.3f%% reclaimed_live_low=%s reclaimed_live_high=%s frozen_sl_0_5u_hit=%s frozen_sl_1_0u_hit=%s frozen_sl_1_5u_hit=%s frozen_sl_2_0u_hit=%s frozen_sl_3_0u_hit=%s frozen_sl_0_1pct_hit=%s frozen_sl_0_15pct_hit=%s frozen_sl_0_2pct_hit=%s live_sl_0_5u_hit=%s live_sl_1_0u_hit=%s live_sl_1_5u_hit=%s live_sl_2_0u_hit=%s live_sl_3_0u_hit=%s live_sl_0_1pct_hit=%s live_sl_0_15pct_hit=%s live_sl_0_2pct_hit=%s label=%s",
                    s.get("zone_id"),s.get("horizon"),s.get("direction"),s.get("current_state"),str(bool(s.get("final_state"))).lower(),s.get("final_state"),s.get("previous_state"),str(bool(s.get("is_frozen"))).lower(),s.get("frozen_reason"),s.get("frozen_state"),s.get("frozen_event_id"),self._safe_float(s.get("snapshot_ts"),0.0)-self._safe_float(s.get("registered_ts"),0.0),self._safe_float(s.get("finalized_ts"),0.0),self._safe_float(s.get("live_zone_lower"),0.0),self._safe_float(s.get("live_zone_upper"),0.0),self._safe_float(s.get("frozen_zone_lower"),0.0),self._safe_float(s.get("frozen_zone_upper"),0.0),self._safe_float(s.get("reference_price"),0.0),self._safe_int(s.get("event_count"),0),self._safe_int(s.get("iceberg_count"),0),self._safe_int(s.get("ignore_count"),0),self._safe_int(s.get("spoof_count"),0),self._safe_float(s.get("positive_score"),0.0),self._safe_float(s.get("negative_score"),0.0),self._safe_float(s.get("net_score"),0.0),self._safe_float(s.get("mfe"),0.0),self._safe_float(s.get("mfe_pct"),0.0)*100.0,self._safe_float(s.get("mae"),0.0),self._safe_float(s.get("mae_pct"),0.0)*100.0,self._safe_float(s.get("min_price_seen"),0.0),self._safe_float(s.get("max_price_seen"),0.0),str(bool(s.get("broke_frozen_iceberg_low"))).lower(),str(bool(s.get("broke_frozen_iceberg_high"))).lower(),self._safe_float(s.get("frozen_break_depth_usdt"),0.0),self._safe_float(s.get("frozen_break_depth_pct"),0.0)*100.0,str(reclaimed_frozen_low).lower(),str(reclaimed_frozen_high).lower(),str(bool(s.get("broke_live_iceberg_low"))).lower(),str(bool(s.get("broke_live_iceberg_high"))).lower(),self._safe_float(s.get("live_break_depth_usdt"),0.0),self._safe_float(s.get("live_break_depth_pct"),0.0)*100.0,str(reclaimed_live_low).lower(),str(reclaimed_live_high).lower(),str(bool(s.get("frozen_sl_0_5u_hit"))).lower(),str(bool(s.get("frozen_sl_1_0u_hit"))).lower(),str(bool(s.get("frozen_sl_1_5u_hit"))).lower(),str(bool(s.get("frozen_sl_2_0u_hit"))).lower(),str(bool(s.get("frozen_sl_3_0u_hit"))).lower(),str(bool(s.get("frozen_sl_0_1pct_hit"))).lower(),str(bool(s.get("frozen_sl_0_15pct_hit"))).lower(),str(bool(s.get("frozen_sl_0_2pct_hit"))).lower(),str(bool(s.get("live_sl_0_5u_hit"))).lower(),str(bool(s.get("live_sl_1_0u_hit"))).lower(),str(bool(s.get("live_sl_1_5u_hit"))).lower(),str(bool(s.get("live_sl_2_0u_hit"))).lower(),str(bool(s.get("live_sl_3_0u_hit"))).lower(),str(bool(s.get("live_sl_0_1pct_hit"))).lower(),str(bool(s.get("live_sl_0_15pct_hit"))).lower(),str(bool(s.get("live_sl_0_2pct_hit"))).lower(),s.get("label"))

    def _prune_completed(self) -> None:
        completed_ids = {str(x.get("zone_id") or "") for x in self._tracking_items if x.get("completed")}
        if completed_ids:
            self._tracking_items = [x for x in self._tracking_items if str(x.get("zone_id") or "") not in completed_ids]
            for zid in completed_ids: self._items_by_id.pop(zid, None); self._seen_zone_ids.add(zid)
        if len(self._tracking_items) <= self.max_tracking_zones: return
        self._tracking_items.sort(key=lambda x: self._safe_float(x.get("registered_ts"), 0.0))
        while len(self._tracking_items) > self.max_tracking_zones:
            dropped = self._tracking_items.pop(0); zid = str(dropped.get("zone_id") or ""); self._items_by_id.pop(zid, None); self._seen_zone_ids.add(zid)

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try: return float(value)
        except (TypeError, ValueError): return float(default)

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try: return int(value)
        except (TypeError, ValueError): return int(default)
