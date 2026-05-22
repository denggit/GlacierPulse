#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Mapping

from .models import safe_float, safe_int


class IcebergTruthScorer:
    LABEL_HIGH = "HIGH_CONFIDENCE_ICEBERG"
    LABEL_MEDIUM = "MEDIUM_CONFIDENCE_ICEBERG"
    LABEL_WEAK = "WEAK_ABSORPTION"
    LABEL_LOW = "LOW_CONFIDENCE"
    LABEL_NOT = "NOT_ICEBERG"
    LABEL_INSUFFICIENT = "INSUFFICIENT_POST_DATA"

    def score(self, candidate: Mapping[str, Any]) -> dict[str, Any]:
        post = candidate.get("post_features") if isinstance(candidate.get("post_features"), Mapping) else {}
        c = _Merged(candidate, post)
        coverage = self._coverage(c)
        warnings = self._warnings_for_coverage(coverage)

        components = {
            "attack_strength_score": self._attack_strength(c),
            "impact_suppression_score": self._impact_suppression(c, coverage),
            "hidden_absorption_score": self._hidden_absorption(c),
            "reload_replenish_score": self._reload_replenish(c, coverage),
            "sweep_reclaim_score": self._sweep_reclaim(c, coverage),
            "cvd_divergence_score": self._cvd_divergence(c, coverage),
            "non_acceptance_score": self._non_acceptance(c, coverage),
        }
        total = min(100.0, max(0.0, sum(components.values())))

        width = max(0.000001, c.f("local_zone_width", 0.0) or abs(c.f("zone_upper") - c.f("zone_lower")) or 1.5)
        insufficient_post_data = self._insufficient_post_data(coverage)
        if abs(c.f("price_displacement")) > width * 2.0:
            total = min(total, 64.0)
        if c.f("active_notional") < 150_000:
            total = min(total, 49.0)
        if insufficient_post_data:
            total = min(total, 49.0)
            label = self.LABEL_INSUFFICIENT
        else:
            label = self.label_for_score(total)

        return {
            "truth_score_total": round(total, 2),
            "truth_label": label,
            "score_components": {k: round(v, 2) for k, v in components.items()},
            "post_data_coverage": coverage,
            "score_warnings": warnings,
        }

    @staticmethod
    def label_for_score(total: float) -> str:
        if total >= 80:
            return IcebergTruthScorer.LABEL_HIGH
        if total >= 65:
            return IcebergTruthScorer.LABEL_MEDIUM
        if total >= 50:
            return IcebergTruthScorer.LABEL_WEAK
        if total >= 30:
            return IcebergTruthScorer.LABEL_LOW
        return IcebergTruthScorer.LABEL_NOT

    def _attack_strength(self, c: "_Merged") -> float:
        score = 0.0
        active = c.f("active_notional")
        if active >= 1_000_000:
            score += 11
        elif active >= 700_000:
            score += 8
        elif active >= 300_000:
            score += 4
        ratio = c.f("active_side_ratio")
        if ratio >= 0.80:
            score += 4
        elif ratio >= 0.65:
            score += 2
        trades = c.i("trade_count")
        if trades >= 30:
            score += 2
        elif trades >= 10:
            score += 1
        return min(15.0, score)

    def _impact_suppression(self, c: "_Merged", coverage: Mapping[str, bool]) -> float:
        score = 0.0
        width = max(0.000001, c.f("local_zone_width") or abs(c.f("zone_upper") - c.f("zone_lower")) or 1.5)
        displacement = abs(c.f("price_displacement"))
        if displacement <= 0.25 * width:
            score += 12
        elif displacement <= 0.5 * width:
            score += 8
        direction = c.s("direction")
        settle = c.f("settle_price") or c.f("trigger_price")
        if coverage.get("has_5s_trade_data"):
            if direction == "BUY":
                post_attack_extension = max(0.0, settle - c.f("post_5s_min_price"))
            else:
                post_attack_extension = max(0.0, c.f("post_5s_max_price") - settle)
            if post_attack_extension <= width:
                score += 4
        settle_price_valid = settle > 0 and c.f("zone_lower") > 0 and c.f("zone_upper") > 0
        if settle_price_valid and (self._inside_zone(c, settle) or 0 <= c.f("reclaim_time_sec", -1.0) <= 5.0):
            score += 4
        return min(20.0, score)

    def _hidden_absorption(self, c: "_Merged") -> float:
        score = 0.0
        hidden = c.f("hidden_volume")
        if hidden >= 2_000_000:
            score += 12
        elif hidden >= 1_000_000:
            score += 8
        elif hidden >= 500_000:
            score += 4
        rate = c.f("absorption_rate")
        if rate >= 0.85:
            score += 8
        elif rate >= 0.70:
            score += 5
        elif rate >= 0.50:
            score += 3
        active = c.f("active_notional")
        book_reduction = c.f("book_reduction")
        if active > 0 and book_reduction > active * 1.2:
            score = min(score, 4.0)
        return min(20.0, score)

    def _reload_replenish(self, c: "_Merged", coverage: Mapping[str, bool]) -> float:
        if not coverage.get("has_book_recovery_data"):
            return 0.0
        score = 0.0
        if c.f("depth_recovery_ratio_1s") >= 0.30:
            score += 4
        if c.f("depth_recovery_ratio_5s") >= 0.50:
            score += 6
        if c.i("replenish_count") >= 2:
            score += 3
        reload_ms = c.f("reload_interval_ms")
        if 0 < reload_ms <= 2000:
            score += 2
        return min(15.0, score)

    def _sweep_reclaim(self, c: "_Merged", coverage: Mapping[str, bool]) -> float:
        if not coverage.get("has_sweep_data"):
            return 0.0
        score = 0.0
        sweep = c.b("has_sweep") or c.b("seen_sweep") or c.f("time_outside_zone") > 0
        if not sweep:
            return 0.0
        reclaim = c.f("reclaim_time_sec", -1.0)
        if sweep and 0 <= reclaim <= 10:
            score += 8
        elif sweep and 0 <= reclaim <= 30:
            score += 5
        outside_30 = c.f("time_outside_zone_30s")
        if coverage.get("observed_through_30s") and outside_30 <= 7.5:
            score += 3
        direction = c.s("direction")
        if 0 <= reclaim and not self._continued_extreme_after_reclaim(c, direction):
            score += 4
        return min(15.0, score)

    def _cvd_divergence(self, c: "_Merged", coverage: Mapping[str, bool]) -> float:
        if not coverage.get("has_cvd_data"):
            return 0.0
        direction = c.s("direction")
        width = max(0.000001, c.f("local_zone_width") or abs(c.f("zone_upper") - c.f("zone_lower")) or 1.5)
        settle = c.f("settle_price") or c.f("trigger_price")
        score = 0.0
        if direction == "BUY":
            attack_5s = c.f("post_5s_cvd_delta") < 0
            attack_30s = c.f("post_30s_cvd_delta") < 0
            no_follow_5s = (settle - (c.f("post_5s_min_price") or settle)) <= width
            no_follow_30s = (settle - (c.f("post_30s_min_price") or settle)) <= width
        else:
            attack_5s = c.f("post_5s_cvd_delta") > 0
            attack_30s = c.f("post_30s_cvd_delta") > 0
            no_follow_5s = ((c.f("post_5s_max_price") or settle) - settle) <= width
            no_follow_30s = ((c.f("post_30s_max_price") or settle) - settle) <= width
        if coverage.get("has_5s_trade_data") and attack_5s and no_follow_5s:
            score += 4
        if (
            coverage.get("has_30s_trade_data")
            and coverage.get("observed_through_30s")
            and attack_30s
            and (no_follow_30s or self._inside_zone(c, c.f("post_last_price") or settle))
        ):
            score += 6
        if (
            c.b("cvd_extreme_price_not_extreme")
            and coverage.get("has_cvd_data")
            and coverage.get("observed_through_30s")
            and coverage.get("has_post_price_data")
        ):
            score = 10
        return min(10.0, score)

    def _non_acceptance(self, c: "_Merged", coverage: Mapping[str, bool]) -> float:
        if not coverage.get("has_post_price_data"):
            return 0.0
        if not (coverage.get("observed_through_120s") or coverage.get("observed_through_30s")):
            return 0.0
        score = 0.0
        if self._inside_or_favorable(c, c.f("post_60s_last_price") or c.f("post_last_price")):
            score += 2
        if not c.b("accepted_beyond_zone"):
            score += 3
        return min(5.0, score)

    @staticmethod
    def _inside_zone(c: "_Merged", price: float) -> bool:
        return c.f("zone_lower") <= price <= c.f("zone_upper")

    @staticmethod
    def _inside_or_favorable(c: "_Merged", price: float) -> bool:
        if not price:
            return False
        if c.f("zone_lower") <= price <= c.f("zone_upper"):
            return True
        if c.s("direction") == "BUY":
            return price > c.f("zone_upper")
        return price < c.f("zone_lower")

    @staticmethod
    def _continued_extreme_after_reclaim(c: "_Merged", direction: str) -> bool:
        settle = c.f("settle_price") or c.f("trigger_price")
        width = max(0.000001, c.f("local_zone_width") or abs(c.f("zone_upper") - c.f("zone_lower")) or 1.5)
        if direction == "BUY":
            return (c.f("post_min_price") or settle) < settle - width
        return (c.f("post_max_price") or settle) > settle + width

    def _coverage(self, c: "_Merged") -> dict[str, bool]:
        has_any_post_trade = c.i("post_trade_count") > 0
        has_5s_trade_data = c.b("has_5s_trade_data") or (
            c.f("post_5s_min_price") > 0
            and c.f("post_5s_max_price") > 0
        ) or (has_any_post_trade and self._has_checkpoint(c, 5))
        has_30s_trade_data = c.b("has_30s_trade_data") or (
            c.f("post_30s_min_price") > 0
            and c.f("post_30s_max_price") > 0
        ) or self._has_checkpoint(c, 30)
        observed_through_5s = c.b("observed_through_5s") or c.f("observation_age_sec") >= 5 or self._has_checkpoint(c, 5)
        observed_through_30s = c.b("observed_through_30s") or c.f("observation_age_sec") >= 30 or self._has_checkpoint(c, 30)
        observed_through_120s = (
            c.b("observed_through_120s")
            or c.b("has_120s_observation")
            or c.f("observation_age_sec") >= 120
            or self._has_checkpoint(c, 120)
        )
        has_book_recovery_data = (
            c.b("has_book_recovery_data")
            or
            c.f("local_depth_last") > 0
            or c.f("local_depth_max") > 0
            or c.f("depth_recovery_ratio_1s") > 0
            or c.f("depth_recovery_ratio_5s") > 0
            or c.f("depth_recovery_ratio_30s") > 0
        )
        has_cvd_data = has_any_post_trade and c.f("post_total_notional") > 0
        has_sweep_data = (
            has_any_post_trade
            and c.f("zone_lower") > 0
            and c.f("zone_upper") > 0
            and c.f("post_min_price") > 0
            and c.f("post_max_price") > 0
        )
        has_post_price_data = (
            c.f("post_last_price") > 0
            or c.f("post_min_price") > 0
            or c.f("post_max_price") > 0
        )
        return {
            "has_any_post_trade": has_any_post_trade,
            "has_5s_trade_data": has_5s_trade_data,
            "has_30s_trade_data": has_30s_trade_data,
            "observed_through_5s": observed_through_5s,
            "observed_through_30s": observed_through_30s,
            "observed_through_120s": observed_through_120s,
            "has_book_recovery_data": has_book_recovery_data,
            "has_cvd_data": has_cvd_data,
            "has_sweep_data": has_sweep_data,
            "has_post_price_data": has_post_price_data,
            "has_5s_trade_window": has_5s_trade_data,
            "has_30s_trade_window": has_30s_trade_data,
            "has_120s_observation": observed_through_120s,
        }

    @staticmethod
    def _has_checkpoint(c: "_Merged", window: int) -> bool:
        checkpoints = c.get("post_window_checkpoints", {})
        if not isinstance(checkpoints, Mapping):
            return False
        for key in (window, str(window)):
            value = checkpoints.get(key)
            if isinstance(value, Mapping) and value:
                return True
        return False

    @staticmethod
    def _warnings_for_coverage(coverage: Mapping[str, bool]) -> list[str]:
        warnings: list[str] = []
        if not coverage.get("has_any_post_trade"):
            warnings.append("insufficient_post_trade_data")
        if not coverage.get("has_5s_trade_data"):
            warnings.append("insufficient_5s_trade_data")
        if not coverage.get("has_30s_trade_data"):
            warnings.append("insufficient_30s_trade_data")
        if not coverage.get("observed_through_30s"):
            warnings.append("insufficient_observed_through_30s")
        if not coverage.get("observed_through_120s"):
            warnings.append("insufficient_observed_through_120s")
        if not coverage.get("has_book_recovery_data"):
            warnings.append("insufficient_book_recovery_data")
        if not coverage.get("has_sweep_data"):
            warnings.append("insufficient_sweep_data")
        if not coverage.get("has_cvd_data"):
            warnings.append("insufficient_cvd_data")
        if not coverage.get("has_post_price_data"):
            warnings.append("insufficient_post_price_data")
        if not (coverage.get("observed_through_120s") or coverage.get("observed_through_30s")):
            warnings.append("insufficient_non_acceptance_window")
        return warnings

    @staticmethod
    def _insufficient_post_data(coverage: Mapping[str, bool]) -> bool:
        severe_missing = (
            not coverage.get("has_any_post_trade")
            and not coverage.get("has_book_recovery_data")
        )
        no_post_windows = (
            not coverage.get("has_5s_trade_data")
            and not coverage.get("has_30s_trade_data")
            and not coverage.get("observed_through_30s")
            and not coverage.get("observed_through_120s")
        )
        return severe_missing or no_post_windows


class _Merged:
    def __init__(self, primary: Mapping[str, Any], post: Mapping[str, Any]) -> None:
        self.primary = primary
        self.post = post

    def get(self, key: str, default: Any = None) -> Any:
        if key in self.post:
            return self.post.get(key, default)
        return self.primary.get(key, default)

    def f(self, key: str, default: float = 0.0) -> float:
        return safe_float(self.get(key, default), default)

    def i(self, key: str, default: int = 0) -> int:
        return safe_int(self.get(key, default), default)

    def s(self, key: str, default: str = "") -> str:
        return str(self.get(key, default) or default).upper()

    def b(self, key: str) -> bool:
        value = self.get(key, False)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}
