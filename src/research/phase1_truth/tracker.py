#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import time
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict, Iterable, Mapping
from zoneinfo import ZoneInfo

from config import research_evaluator as cfg

from .models import Phase1Observation, SCHEMA_VERSION, safe_float, safe_int
from .recorder import Phase1CandidateRecorder
from .scorer import IcebergTruthScorer

logger = logging.getLogger(__name__)


class Phase1TruthTracker:
    def __init__(
        self,
        enabled: bool = True,
        recorder: Phase1CandidateRecorder | None = None,
        scorer: IcebergTruthScorer | None = None,
        post_windows_sec: Iterable[int] = (1, 5, 30, 120),
        max_active_observations: int = 500,
        finalize_after_sec: float = 120.0,
        timezone: str = "Asia/Shanghai",
    ) -> None:
        self.enabled = bool(enabled)
        self.recorder = recorder or Phase1CandidateRecorder(enabled=enabled)
        self.scorer = scorer or IcebergTruthScorer()
        self.post_windows_sec = sorted({int(w) for w in post_windows_sec if int(w) > 0}) or [1, 5, 30, 120]
        self.max_active_observations = max(1, int(max_active_observations))
        self.finalize_after_sec = float(finalize_after_sec)
        self.timezone = timezone
        self.active_observations: "OrderedDict[str, Phase1Observation]" = OrderedDict()
        if self.enabled:
            logger.info(
                "[PHASE1-TRUTH] enabled=true path=%s post_windows=%s",
                getattr(self.recorder, "jsonl_path", "logs/research/phase1_candidates.jsonl"),
                self.post_windows_sec,
            )

    @classmethod
    def from_config(cls) -> "Phase1TruthTracker":
        try:
            enabled = bool(getattr(cfg, "PHASE1_TRUTH_SHADOW_ENABLED", True))
            recorder = Phase1CandidateRecorder(
                enabled=bool(getattr(cfg, "PHASE1_CANDIDATE_RECORDER_ENABLED", True)),
                write_jsonl=bool(getattr(cfg, "PHASE1_CANDIDATE_RECORDER_WRITE_JSONL", True)),
                jsonl_path=str(getattr(cfg, "PHASE1_CANDIDATE_RECORDER_JSONL_PATH", "logs/research/phase1_candidates.jsonl")),
            )
            return cls(
                enabled=enabled,
                recorder=recorder,
                post_windows_sec=getattr(cfg, "PHASE1_TRUTH_POST_WINDOWS_SEC", [1, 5, 30, 120]),
                max_active_observations=int(getattr(cfg, "PHASE1_TRUTH_MAX_ACTIVE_OBSERVATIONS", 500)),
                finalize_after_sec=float(getattr(cfg, "PHASE1_TRUTH_FINALIZE_AFTER_SEC", 120.0)),
            )
        except Exception as exc:
            logger.warning("[PHASE1-TRUTH-FALLBACK] reason=config_init_failed error=%s", exc)
            return cls(enabled=False)

    def on_trade(self, tick: Mapping[str, Any]) -> None:
        if not self.enabled:
            return
        try:
            ts = safe_float(tick.get("ts"), time.time())
            recv_ts = safe_float(tick.get("recv_ts"), ts)
            price = safe_float(tick.get("price"))
            size = safe_float(tick.get("size"))
            side = str(tick.get("side") or "").lower()
            if price <= 0 or size <= 0:
                return
            notional = price * size
            cvd_delta = notional if side == "buy" else -notional if side == "sell" else 0.0
            for obs in list(self.active_observations.values()):
                self._update_trade(obs, ts, recv_ts, price, notional, cvd_delta)
            self._finalize_expired(ts, recv_ts)
        except Exception as exc:
            logger.warning("[PHASE1-TRUTH-FALLBACK] reason=on_trade_failed error=%s", exc)

    def on_book_update(self, book_data: Mapping[str, Any]) -> None:
        if not self.enabled:
            return
        try:
            ts = safe_float(book_data.get("ts"), time.time())
            recv_ts = safe_float(book_data.get("recv_ts"), ts)
            for obs in list(self.active_observations.values()):
                self._update_book(obs, book_data, ts, recv_ts)
            self._finalize_expired(ts, recv_ts)
        except Exception as exc:
            logger.warning("[PHASE1-TRUTH-FALLBACK] reason=on_book_update_failed error=%s", exc)

    def register_candidate_settlement(self, candidate_snapshot: Mapping[str, Any]) -> None:
        if not self.enabled:
            return
        try:
            candidate = dict(candidate_snapshot)
            candidate["schema_version"] = candidate.get("schema_version") or SCHEMA_VERSION
            candidate["record_type"] = "candidate_settled"
            event_key = str(candidate.get("event_key") or candidate.get("event_id") or "")
            if not event_key:
                event_key = f"{candidate.get('symbol', 'UNKNOWN')}|{candidate.get('direction', '')}|{candidate.get('settle_recv_ts', time.time())}"
                candidate["event_key"] = event_key
            self.recorder.record_settled(candidate)
            logger.info(
                "[PHASE1-CANDIDATE-SETTLED] result=%s direction=%s active=%.0f hidden=%.0f absorption=%.4f event_key=%s",
                candidate.get("result"),
                candidate.get("direction"),
                safe_float(candidate.get("active_notional")),
                safe_float(candidate.get("hidden_volume")),
                safe_float(candidate.get("absorption_rate")),
                event_key,
            )
            self._enforce_capacity()
            settle_ts = safe_float(candidate.get("settle_ts"), safe_float(candidate.get("settle_recv_ts"), time.time()))
            settle_recv_ts = safe_float(candidate.get("settle_recv_ts"), settle_ts)
            self.active_observations[event_key] = Phase1Observation(
                candidate=candidate,
                settle_ts=settle_ts,
                settle_recv_ts=settle_recv_ts,
                created_recv_ts=settle_recv_ts,
                post_min_price=0.0,
                post_max_price=0.0,
                post_5s_min_price=0.0,
                post_5s_max_price=0.0,
                post_30s_min_price=0.0,
                post_30s_max_price=0.0,
            )
        except Exception as exc:
            logger.warning("[PHASE1-TRUTH-FALLBACK] reason=register_candidate_failed error=%s", exc)

    def finalize_all(self, reason: str = "manual") -> int:
        count = 0
        for key in list(self.active_observations.keys()):
            obs = self.active_observations.pop(key, None)
            if obs:
                self._finalize(obs, reason=reason)
                count += 1
        return count

    def _update_trade(
        self,
        obs: Phase1Observation,
        ts: float,
        recv_ts: float,
        price: float,
        notional: float,
        cvd_delta: float,
    ) -> None:
        if ts < obs.settle_ts:
            return
        age = max(0.0, ts - obs.settle_ts)
        if age > self.finalize_after_sec:
            return
        self._accumulate_outside_time(obs, ts, price)
        obs.post_trade_count += 1
        obs.post_total_notional += notional
        if cvd_delta >= 0:
            obs.post_buy_notional += notional
        else:
            obs.post_sell_notional += notional
        obs.post_cvd_delta += cvd_delta
        obs.post_last_price = price
        obs.post_min_price = price if obs.post_min_price <= 0 else min(obs.post_min_price, price)
        obs.post_max_price = price if obs.post_max_price <= 0 else max(obs.post_max_price, price)
        if age <= 5:
            obs.post_5s_cvd_delta += cvd_delta
            obs.post_5s_min_price = price if obs.post_5s_min_price <= 0 else min(obs.post_5s_min_price, price)
            obs.post_5s_max_price = price if obs.post_5s_max_price <= 0 else max(obs.post_5s_max_price, price)
        if age <= 30:
            obs.post_30s_cvd_delta += cvd_delta
            obs.post_30s_min_price = price if obs.post_30s_min_price <= 0 else min(obs.post_30s_min_price, price)
            obs.post_30s_max_price = price if obs.post_30s_max_price <= 0 else max(obs.post_30s_max_price, price)
        self._capture_checkpoints(obs, age)
        obs._last_trade_ts = ts

    def _update_book(self, obs: Phase1Observation, book_data: Mapping[str, Any], ts: float, recv_ts: float) -> None:
        age = max(0.0, ts - obs.settle_ts)
        if age > self.finalize_after_sec:
            return
        direction = str(obs.candidate.get("direction") or "").upper()
        levels = book_data.get("bids") if direction == "BUY" else book_data.get("asks")
        local_depth = self._calc_local_depth_usdt(
            levels,
            safe_float(obs.candidate.get("zone_lower")),
            safe_float(obs.candidate.get("zone_upper")),
        )
        if local_depth <= 0:
            return
        start_depth = safe_float(obs.candidate.get("start_thickness_usdt"))
        end_depth = safe_float(obs.candidate.get("end_thickness_usdt"))
        recovery = max(0.0, local_depth - end_depth) / max(start_depth, 1.0)
        if age <= 1:
            obs.depth_recovery_ratio_1s = max(obs.depth_recovery_ratio_1s, recovery)
        if age <= 5:
            obs.depth_recovery_ratio_5s = max(obs.depth_recovery_ratio_5s, recovery)
        if age <= 30:
            obs.depth_recovery_ratio_30s = max(obs.depth_recovery_ratio_30s, recovery)
        if obs.local_depth_last > 0 and local_depth > obs.local_depth_last + max(50_000.0, obs.local_depth_last * 0.10):
            obs.replenish_count += 1
            if obs._first_replenish_ts <= 0:
                obs._first_replenish_ts = ts
            obs._last_replenish_ts = ts
            if obs.replenish_count >= 2:
                obs.reload_interval_ms = max(0.0, (obs._last_replenish_ts - obs._first_replenish_ts) * 1000.0)
        obs.local_depth_min = local_depth if obs.local_depth_min <= 0 else min(obs.local_depth_min, local_depth)
        obs.local_depth_max = max(obs.local_depth_max, local_depth)
        obs.local_depth_last = local_depth
        obs._last_book_ts = ts

    def _accumulate_outside_time(self, obs: Phase1Observation, ts: float, price: float) -> None:
        direction = str(obs.candidate.get("direction") or "").upper()
        lower = safe_float(obs.candidate.get("zone_lower"))
        upper = safe_float(obs.candidate.get("zone_upper"))
        outside = price < lower if direction == "BUY" else price > upper if direction == "SELL" else False
        if outside:
            obs._seen_sweep = True
        elif obs._seen_sweep and obs.reclaim_time_sec is None:
            obs.reclaim_time_sec = max(0.0, ts - obs.settle_ts)
        if obs._last_trade_ts > 0 and obs._last_outside_state:
            delta = max(0.0, ts - obs._last_trade_ts)
            obs.time_outside_zone += delta
            if obs._last_trade_ts - obs.settle_ts <= 30:
                obs.time_outside_zone_30s += min(delta, max(0.0, obs.settle_ts + 30 - obs._last_trade_ts))
        age = max(0.0, ts - obs.settle_ts)
        if outside and age >= 60:
            obs.accepted_beyond_zone = True
        obs._last_outside_state = outside

    def _capture_checkpoints(self, obs: Phase1Observation, age: float) -> None:
        for window in self.post_windows_sec:
            if age <= window and window not in obs.checkpoints:
                obs.checkpoints[window] = {}
            if age <= window:
                obs.checkpoints[window] = {
                    "last_price": obs.post_last_price,
                    "cvd_delta": obs.post_cvd_delta,
                    "min_price": obs.post_min_price,
                    "max_price": obs.post_max_price,
                    "trade_count": obs.post_trade_count,
                }

    def _finalize_expired(self, ts: float, recv_ts: float) -> None:
        for key, obs in list(self.active_observations.items()):
            if ts - obs.settle_ts >= self.finalize_after_sec or recv_ts - obs.settle_recv_ts >= self.finalize_after_sec:
                self.active_observations.pop(key, None)
                self._finalize(obs, reason="finalize_after_sec")

    def _enforce_capacity(self) -> None:
        while len(self.active_observations) >= self.max_active_observations:
            key, obs = self.active_observations.popitem(last=False)
            obs.candidate["dropped_due_to_capacity"] = True
            logger.warning(
                "[PHASE1-TRUTH-CAPACITY] active_observations=%d max=%d event_key=%s",
                len(self.active_observations) + 1,
                self.max_active_observations,
                key,
            )
            self._finalize(obs, reason="dropped_due_to_capacity")

    def _finalize(self, obs: Phase1Observation, reason: str) -> None:
        try:
            post_features = self._post_features(obs, reason)
            record = dict(obs.candidate)
            record["record_type"] = "candidate_finalized"
            record["post_features"] = post_features
            record.update(post_features)
            score = self.scorer.score(record)
            record["truth_score"] = score
            record.update(score)
            self.recorder.record_finalized(record)
            logger.info(
                "[PHASE1-CANDIDATE-FINALIZED] result=%s truth_score=%.0f label=%s event_key=%s",
                record.get("result"),
                safe_float(score.get("truth_score_total")),
                score.get("truth_label"),
                obs.event_key,
            )
        except Exception as exc:
            logger.warning("[PHASE1-TRUTH-FALLBACK] reason=finalize_failed error=%s", exc)

    def _post_features(self, obs: Phase1Observation, reason: str) -> Dict[str, Any]:
        return {
            "finalize_reason": reason,
            "post_trade_count": obs.post_trade_count,
            "post_total_notional": round(obs.post_total_notional, 6),
            "post_buy_notional": round(obs.post_buy_notional, 6),
            "post_sell_notional": round(obs.post_sell_notional, 6),
            "post_cvd_delta": round(obs.post_cvd_delta, 6),
            "post_5s_cvd_delta": round(obs.post_5s_cvd_delta, 6),
            "post_30s_cvd_delta": round(obs.post_30s_cvd_delta, 6),
            "post_min_price": obs.post_min_price,
            "post_max_price": obs.post_max_price,
            "post_5s_min_price": obs.post_5s_min_price,
            "post_5s_max_price": obs.post_5s_max_price,
            "post_30s_min_price": obs.post_30s_min_price,
            "post_30s_max_price": obs.post_30s_max_price,
            "post_last_price": obs.post_last_price,
            "time_outside_zone": round(obs.time_outside_zone, 6),
            "time_outside_zone_30s": round(obs.time_outside_zone_30s, 6),
            "reclaim_time_sec": obs.reclaim_time_sec if obs.reclaim_time_sec is not None else -1.0,
            "accepted_beyond_zone": obs.accepted_beyond_zone,
            "has_sweep": obs._seen_sweep,
            "depth_recovery_ratio_1s": round(obs.depth_recovery_ratio_1s, 6),
            "depth_recovery_ratio_5s": round(obs.depth_recovery_ratio_5s, 6),
            "depth_recovery_ratio_30s": round(obs.depth_recovery_ratio_30s, 6),
            "replenish_count": obs.replenish_count,
            "reload_interval_ms": round(obs.reload_interval_ms, 6),
            "local_depth_min": round(obs.local_depth_min, 6),
            "local_depth_max": round(obs.local_depth_max, 6),
            "local_depth_last": round(obs.local_depth_last, 6),
            "post_window_checkpoints": obs.checkpoints,
        }

    @staticmethod
    def _calc_local_depth_usdt(levels: Any, zone_lower: float, zone_upper: float) -> float:
        depth = 0.0
        if isinstance(levels, Mapping):
            iterator = levels.items()
        elif isinstance(levels, list):
            iterator = levels
        else:
            return 0.0
        for item in iterator:
            try:
                if isinstance(item, tuple) and len(item) == 2:
                    price, size = item
                elif isinstance(item, list) and len(item) >= 2:
                    price, size = item[0], item[1]
                else:
                    continue
                p = float(price)
                if zone_lower <= p <= zone_upper:
                    depth += p * float(size)
            except (TypeError, ValueError):
                continue
        return depth


def session_snapshot(ts: float, timezone_name: str = "Asia/Shanghai") -> dict[str, Any]:
    tz = ZoneInfo(timezone_name)
    dt = datetime.fromtimestamp(float(ts or time.time()), tz=tz)
    return {
        "timezone": timezone_name,
        "local_time": dt.isoformat(),
        "session_tag": get_session_tag(dt),
        "is_weekend": dt.weekday() >= 5,
    }


def get_session_tag(dt: datetime) -> str:
    minutes = dt.hour * 60 + dt.minute
    if minutes < 5 * 60:
        return "US_LATE"
    if minutes < 8 * 60:
        return "ASIA_OFF"
    if minutes < 16 * 60:
        return "ASIA_DAY"
    if minutes < 21 * 60 + 30:
        return "EUROPE_PRE_US"
    return "US_OPEN"
