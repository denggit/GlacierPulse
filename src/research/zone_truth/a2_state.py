#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Iterable, Mapping

from src.research.a1_edge.schema import parse_bool, parse_float




def classify_a3_latency_bucket(after_a2_flag: bool, latency_sec: float) -> str:
    if not after_a2_flag:
        return "NO_IGNITION"
    if latency_sec <= 60:
        return "FAST_IGNITION"
    if latency_sec <= 900:
        return "NORMAL_IGNITION"
    if latency_sec <= 3600:
        return "LATE_IGNITION"
    return "OUT_OF_WINDOW"


def classify_a3_ignition_quality_after_a2(row: Mapping[str, Any], after_a2_flag: bool) -> str:
    if not after_a2_flag:
        return "NO_IGNITION"
    raw = str(row.get("a3_preview_ignition_quality") or "").upper()
    if raw in {"STRONG_IGNITION", "MEDIUM_IGNITION", "WEAK_IGNITION"}:
        return raw

    latency_bucket = classify_a3_latency_bucket(True, parse_float(row.get("a3_preview_breakout_raw_latency_sec")))
    net_mfe_15m = parse_float(row.get("a3_preview_net_mfe_15m_r"))
    net_mae_15m = parse_float(row.get("a3_preview_net_mae_15m_r"))
    persistence_3m = parse_bool(row.get("a3_preview_persistence_3m_flag"))
    no_quick_return_3m = parse_bool(row.get("a3_preview_no_quick_return_3m_flag"))

    if (
        net_mfe_15m >= 1.0
        and net_mae_15m > -1.0
        and persistence_3m
        and no_quick_return_3m
        and latency_bucket in {"FAST_IGNITION", "NORMAL_IGNITION"}
    ):
        return "STRONG_IGNITION"
    if (
        net_mfe_15m >= 0.5
        and net_mae_15m > -1.5
        and persistence_3m
        and latency_bucket != "OUT_OF_WINDOW"
    ):
        return "MEDIUM_IGNITION"
    return "WEAK_IGNITION"

class ZoneA2StateClassifier:
    """Research-only A2_PRE_POOL lifecycle classifier for zone_truth rows."""

    def attach_a2_state(self, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        return [self.classify_row(row) for row in rows or []]

    def classify_row(self, row: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(row)
        eligible = parse_bool(result.get("a2_pre_pool_eligible"))
        reaction_text = self._reaction_text(result)

        clean_hold = eligible and (
            parse_bool(result.get("has_clean_hold")) or "CLEAN_HOLD" in reaction_text
        )
        failed_reclaim = eligible and (
            parse_bool(result.get("has_failed_reclaim"))
            or "FAILED_RECLAIM" in reaction_text
            or "SWEEP_NO_RECLAIM" in reaction_text
            or self._has_failed_token(reaction_text)
        )
        sweep = eligible and ("SWEEP" in reaction_text or parse_bool(result.get("has_swept_boundary")))
        reclaim = eligible and ("RECLAIM" in reaction_text or parse_bool(result.get("has_reclaimed_boundary")))
        retest = eligible and ("RETEST" in reaction_text or parse_bool(result.get("has_retested_inside_zone")))
        book_depth_state = self.classify_book_depth(result)
        context_alignment = self.classify_context_alignment(result)
        strong_flag, strong_tier, strong_reason = self.classify_strong_a1(result, eligible)

        if not eligible:
            state = "NOT_A2_PRE_POOL"
            reason = "a2_pre_pool_eligible=false"
        elif failed_reclaim:
            state = "A2_FAILED_RECLAIM"
            reason = "failed_reclaim_evidence"
        elif clean_hold:
            state = "A2_CLEAN_HOLD"
            reason = "clean_hold_evidence"
        elif book_depth_state == "BOOK_DEPTH_MISSING":
            state = "A2_BOOK_DEPTH_MISSING"
            reason = "book_depth_missing"
        elif book_depth_state == "BOOK_DEPTH_VALID":
            state = "A2_BOOK_DEPTH_VALID"
            reason = "book_depth_valid"
        else:
            state = "A2_PRE_POOL"
            reason = "iceberg_pie_count>=1"

        validation_score = self._validation_score(
            eligible=eligible,
            clean_hold=clean_hold,
            failed_reclaim=failed_reclaim,
            book_depth_state=book_depth_state,
            context_alignment=context_alignment,
        )
        computed_validated_candidate = (
            eligible
            and clean_hold
            and not failed_reclaim
            and book_depth_state == "BOOK_DEPTH_VALID"
            and context_alignment != "COUNTER_TREND"
        )
        validated_candidate = computed_validated_candidate

        event_ts = parse_float(result.get("reaction_event_ts"))
        state_ts = self._state_ts(result)
        observe_priority, priority_reason, block_reason = self._observe_priority(
            eligible=eligible,
            failed_reclaim=failed_reclaim,
            book_depth_state=book_depth_state,
            context_alignment=context_alignment,
            validated_candidate=validated_candidate,
        )
        risk_tier, risk_reason = self._risk_tier(
            eligible=eligible,
            clean_hold=clean_hold,
            failed_reclaim=failed_reclaim,
            book_depth_state=book_depth_state,
            context_alignment=context_alignment,
        )
        lifecycle = self._lifecycle_metrics(
            result,
            clean_hold=clean_hold,
            failed_reclaim=failed_reclaim,
            state_ts=state_ts,
        )
        sweep_quality = self._sweep_reclaim_quality(
            eligible=eligible,
            clean_hold=clean_hold,
            failed_reclaim=failed_reclaim,
            sweep=sweep,
            reclaim=reclaim,
            retest=retest,
        )
        compression = self._compression_metrics(
            result,
            eligible=eligible,
            clean_hold=clean_hold,
            failed_reclaim=failed_reclaim,
            book_depth_state=book_depth_state,
        )
        ready_for_a3, ready_reason, a3_watch_priority = self._a3_watch_readiness(
            eligible=eligible,
            clean_hold=clean_hold,
            failed_reclaim=failed_reclaim,
            book_depth_state=book_depth_state,
            context_alignment=context_alignment,
            observe_priority=observe_priority,
            strong_tier=strong_tier,
        )
        a3_breakout_after_a2 = ready_for_a3 and parse_bool(result.get("a3_preview_breakout_raw_flag"))
        result.update(
            {
                "a2_state": state,
                "a2_state_reason": reason,
                "a2_state_ts": state_ts,
                "a2_failed_reason": reason if state == "A2_FAILED_RECLAIM" else "",
                "a2_book_depth_state": book_depth_state,
                "a2_context_alignment": context_alignment,
                "a2_clean_hold_flag": clean_hold,
                "a2_failed_reclaim_flag": failed_reclaim,
                "a2_sweep_flag": sweep,
                "a2_reclaim_flag": reclaim,
                "a2_retest_flag": retest,
                "a2_validated_candidate_flag": validated_candidate,
                "a2_validation_score": validation_score,
                "a2_observe_priority": observe_priority,
                "a2_priority_reason": priority_reason,
                "a2_block_reason": block_reason,
                "a2_risk_tier": risk_tier,
                "a2_risk_reason": risk_reason,
                "a2_reaction_latency_sec": lifecycle["a2_reaction_latency_sec"],
                "a2_time_to_clean_hold_sec": lifecycle["a2_time_to_clean_hold_sec"],
                "a2_time_to_failed_reclaim_sec": lifecycle["a2_time_to_failed_reclaim_sec"],
                "a2_hold_duration_sec": lifecycle["a2_hold_duration_sec"],
                "a2_zone_age_sec": lifecycle["a2_zone_age_sec"],
                "a2_sweep_reclaim_quality": sweep_quality,
                "a2_reclaim_success_flag": reclaim and not failed_reclaim,
                "a2_retest_success_flag": retest and not failed_reclaim,
                "a2_post_zone_range_15m_u": compression["a2_post_zone_range_15m_u"],
                "a2_post_zone_range_1h_u": compression["a2_post_zone_range_1h_u"],
                "a2_post_zone_range_4h_u": compression["a2_post_zone_range_4h_u"],
                "a2_compression_ratio_15m": compression["a2_compression_ratio_15m"],
                "a2_compression_ratio_1h": compression["a2_compression_ratio_1h"],
                "a2_compression_state": compression["a2_compression_state"],
                "a2_compression_reason": compression["a2_compression_reason"],
                "a2_ready_for_a3_watch_flag": ready_for_a3,
                "a2_ready_for_a3_reason": ready_reason,
                "a3_watch_priority": a3_watch_priority,
                "a3_preview_breakout_after_a2_flag": a3_breakout_after_a2,
                "a3_preview_breakout_after_a2_latency_sec": parse_float(result.get("a3_preview_breakout_raw_latency_sec")) if a3_breakout_after_a2 else 0.0,
                "a3_preview_latency_bucket": classify_a3_latency_bucket(a3_breakout_after_a2, parse_float(result.get("a3_preview_breakout_raw_latency_sec"))),
                "a3_preview_ignition_quality": classify_a3_ignition_quality_after_a2(result, a3_breakout_after_a2),
                "strong_a1_raw_flag": strong_flag,
                "strong_a1_tier": strong_tier,
                "strong_a1_reason": strong_reason,
                "reaction_event_ts_valid": parse_bool(
                    result.get("reaction_event_ts_valid"),
                    default=event_ts > 0,
                ),
                "reaction_event_ts_outside_kline_range": parse_bool(
                    result.get("reaction_event_ts_outside_kline_range")
                ),
            }
        )
        return result

    @staticmethod
    def classify_book_depth(row: Mapping[str, Any]) -> str:
        existing = str(row.get("a2_book_depth_state") or "").upper()
        if existing in {"BOOK_DEPTH_VALID", "BOOK_DEPTH_MISSING", "BOOK_DEPTH_UNKNOWN"}:
            return existing
        if "relevant_book_depth_available" in row and row.get("relevant_book_depth_available") not in (None, ""):
            return "BOOK_DEPTH_VALID" if parse_bool(row.get("relevant_book_depth_available")) else "BOOK_DEPTH_MISSING"

        direction = str(row.get("direction") or "").upper()
        fields = {
            "BUY": ("bid_depth_near_zone", "bid_depth_near_sweep"),
            "SELL": ("ask_depth_near_zone", "ask_depth_near_sweep"),
        }.get(direction, ())
        present = False
        for field_name in fields:
            if field_name not in row or row.get(field_name) in (None, ""):
                continue
            present = True
            if parse_float(row.get(field_name)) > 0:
                return "BOOK_DEPTH_VALID"
        return "BOOK_DEPTH_MISSING" if present else "BOOK_DEPTH_UNKNOWN"

    @staticmethod
    def classify_context_alignment(row: Mapping[str, Any]) -> str:
        existing = str(row.get("a2_context_alignment") or "").upper()
        if existing in {"ALIGNED", "COUNTER_TREND", "MIXED_OR_UNKNOWN"}:
            return existing
        direction = str(row.get("direction") or "").upper()
        trend = str(row.get("trend_alignment") or "").upper()
        if direction == "BUY" and "ALIGNED_UP" in trend:
            return "ALIGNED"
        if direction == "SELL" and "ALIGNED_DOWN" in trend:
            return "ALIGNED"
        if "COUNTER" in trend:
            return "COUNTER_TREND"
        if direction == "BUY" and "ALIGNED_DOWN" in trend:
            return "COUNTER_TREND"
        if direction == "SELL" and "ALIGNED_UP" in trend:
            return "COUNTER_TREND"
        return "MIXED_OR_UNKNOWN"

    @staticmethod
    def classify_strong_a1(row: Mapping[str, Any], eligible: bool | None = None) -> tuple[bool, str, str]:
        active_ok = parse_float(row.get("max_active_notional")) >= 1_500_000
        hidden_ok = parse_float(row.get("max_hidden_volume")) >= 2_000_000
        absorption_ok = parse_float(row.get("max_absorption_rate")) >= 0.70
        thickness_fields = (
            "max_start_thickness_usdt",
            "start_thickness_usdt",
            "max_local_depth_usdt",
        )
        present_thickness = [name for name in thickness_fields if row.get(name) not in (None, "")]
        thickness_ok = True
        if present_thickness:
            thickness_ok = max(parse_float(row.get(name)) for name in present_thickness) >= 1_000_000
        is_eligible = parse_bool(row.get("a2_pre_pool_eligible")) if eligible is None else bool(eligible)
        strong = is_eligible and active_ok and hidden_ok and absorption_ok and thickness_ok
        if not is_eligible:
            tier = "NON_A2"
        elif strong:
            tier = "STRONG_A1_RAW"
        else:
            tier = "NORMAL_A1"
        reasons = []
        if active_ok:
            reasons.append("active>=1500000")
        if hidden_ok:
            reasons.append("hidden>=2000000")
        if absorption_ok:
            reasons.append("absorption>=0.7")
        if present_thickness and thickness_ok:
            reasons.append("start_thickness>=1000000")
        return strong, tier, "|".join(reasons)

    @staticmethod
    def _reaction_text(row: Mapping[str, Any]) -> str:
        parts = [
            row.get("reaction_type"),
            row.get("a1_reaction_type"),
            row.get("final_reaction_type"),
            row.get("reaction_types"),
        ]
        return "|".join(str(part or "").upper() for part in parts)

    @staticmethod
    def _has_failed_token(reaction_text: str) -> bool:
        return any(part in {"FAILED", "A1_REACTION_FAILED"} or part.endswith("_FAILED") for part in reaction_text.split("|"))

    @staticmethod
    def _state_ts(row: Mapping[str, Any]) -> float:
        for name in ("reaction_event_ts", "final_reaction_ts", "frozen_ts", "best_pie_ts", "first_seen_ts"):
            value = parse_float(row.get(name))
            if value > 0:
                return value
        return 0.0

    @staticmethod
    def _base_ts(row: Mapping[str, Any]) -> float:
        for name in ("frozen_ts", "best_pie_ts", "first_seen_ts"):
            value = parse_float(row.get(name))
            if value > 0:
                return value
        return 0.0

    @staticmethod
    def _observe_priority(
        eligible: bool,
        failed_reclaim: bool,
        book_depth_state: str,
        context_alignment: str,
        validated_candidate: bool,
    ) -> tuple[str, str, str]:
        if not eligible:
            return "NON_A2", "not_a2_pre_pool", ""
        if failed_reclaim:
            return "A2_BLOCKED", "blocked:failed_reclaim", "FAILED_RECLAIM"
        if book_depth_state == "BOOK_DEPTH_MISSING":
            return "A2_BLOCKED", "blocked:book_depth_missing", "BOOK_DEPTH_MISSING"
        if context_alignment == "COUNTER_TREND":
            return "A2_BLOCKED", "blocked:counter_trend", "COUNTER_TREND"
        if validated_candidate and context_alignment == "ALIGNED":
            return "A2_HIGH", "validated_candidate|aligned", ""
        if validated_candidate and context_alignment == "MIXED_OR_UNKNOWN":
            return "A2_WATCH", "validated_candidate|mixed_context", ""
        return "A2_LOW", "a2_pre_pool_without_high_watch_evidence", ""

    @staticmethod
    def _risk_tier(
        eligible: bool,
        clean_hold: bool,
        failed_reclaim: bool,
        book_depth_state: str,
        context_alignment: str,
    ) -> tuple[str, str]:
        if not eligible:
            return "NON_A2", "not_a2_pre_pool"
        reasons: list[str] = []
        if failed_reclaim:
            reasons.append("failed_reclaim")
        if book_depth_state == "BOOK_DEPTH_MISSING":
            reasons.append("book_depth_missing")
        if context_alignment == "COUNTER_TREND":
            reasons.append("counter_trend")
        if reasons:
            return "HIGH_RISK", "|".join(reasons)

        if book_depth_state == "BOOK_DEPTH_UNKNOWN":
            reasons.append("book_depth_unknown")
        if context_alignment == "MIXED_OR_UNKNOWN":
            reasons.append("mixed_context")
        if not clean_hold:
            reasons.append("no_clean_hold")
        if reasons:
            return "MEDIUM_RISK", "|".join(reasons)

        if clean_hold and book_depth_state == "BOOK_DEPTH_VALID" and context_alignment != "COUNTER_TREND":
            reason = ["clean_hold", "book_depth_valid"]
            if context_alignment == "ALIGNED":
                reason.append("aligned")
            return "LOW_RISK", "|".join(reason)
        return "MEDIUM_RISK", "|".join(reasons or ["a2_pre_pool_uncertain"])

    @staticmethod
    def _lifecycle_metrics(
        row: Mapping[str, Any],
        clean_hold: bool,
        failed_reclaim: bool,
        state_ts: float,
    ) -> dict[str, float]:
        base_ts = ZoneA2StateClassifier._base_ts(row)
        reaction_ts = 0.0
        for name in ("reaction_event_ts", "final_reaction_ts"):
            value = parse_float(row.get(name))
            if value > 0:
                reaction_ts = value
                break
        if reaction_ts <= 0:
            reaction_ts = state_ts

        latency = max(0.0, reaction_ts - base_ts) if base_ts > 0 and reaction_ts > 0 else 0.0
        end_ts = max(
            parse_float(row.get("reaction_event_ts")),
            parse_float(row.get("final_reaction_ts")),
            parse_float(row.get("last_seen_ts")),
            state_ts,
        )
        zone_age = max(0.0, end_ts - base_ts) if base_ts > 0 and end_ts > 0 else 0.0
        return {
            "a2_reaction_latency_sec": round(latency, 6),
            "a2_time_to_clean_hold_sec": round(latency, 6) if clean_hold else 0.0,
            "a2_time_to_failed_reclaim_sec": round(latency, 6) if failed_reclaim else 0.0,
            "a2_hold_duration_sec": round(zone_age, 6) if clean_hold else 0.0,
            "a2_zone_age_sec": round(zone_age, 6),
        }

    @staticmethod
    def _sweep_reclaim_quality(
        eligible: bool,
        clean_hold: bool,
        failed_reclaim: bool,
        sweep: bool,
        reclaim: bool,
        retest: bool,
    ) -> str:
        if not eligible:
            return "NON_A2"
        if failed_reclaim:
            return "FAILED_RECLAIM"
        if clean_hold and not sweep:
            return "CLEAN_HOLD_NO_SWEEP"
        if sweep and not reclaim:
            return "SWEEP_NO_RECLAIM"
        if sweep and reclaim and not retest:
            return "SWEEP_RECLAIM_NO_RETEST"
        if sweep and reclaim and retest:
            return "SWEEP_RECLAIM_RETEST"
        return "NO_SWEEP"

    @staticmethod
    def _compression_metrics(
        row: Mapping[str, Any],
        eligible: bool,
        clean_hold: bool,
        failed_reclaim: bool,
        book_depth_state: str,
    ) -> dict[str, Any]:
        range_15m = parse_float(row.get("mfe_15m_u")) + abs(parse_float(row.get("mae_15m_u")))
        range_1h = parse_float(row.get("mfe_1h_u")) + abs(parse_float(row.get("mae_1h_u")))
        range_4h = parse_float(row.get("mfe_4h_u")) + abs(parse_float(row.get("mae_4h_u")))
        is_complete_15m = parse_bool(row.get("is_complete_15m"))
        zone_width = max(
            parse_float(row.get("zone_width")),
            parse_float(row.get("zone_upper")) - parse_float(row.get("zone_lower")),
            1.0,
        )
        mae_15m_ratio = abs(parse_float(row.get("mae_15m_u"))) / zone_width
        mfe_15m_ratio = parse_float(row.get("mfe_15m_u")) / zone_width
        ratio_15m = range_15m / zone_width
        ratio_1h = range_1h / zone_width

        if not eligible:
            state = "NON_A2"
            reason = "not_a2_pre_pool"
        elif not is_complete_15m:
            state = "INSUFFICIENT_FUTURE_DATA"
            reason = "incomplete_15m_forward_window"
        elif failed_reclaim or book_depth_state == "BOOK_DEPTH_MISSING" or mae_15m_ratio >= 3.0:
            state = "FAILED_EXPANSION"
            reason = "failed_reclaim_or_book_missing_or_mae_expansion"
        elif clean_hold and book_depth_state == "BOOK_DEPTH_VALID" and ratio_15m <= 4.0 and mae_15m_ratio <= 1.5:
            state = "COMPRESSING"
            reason = "clean_hold|book_depth_valid|range_15m<=4w|mae_15m<=1.5w"
        elif clean_hold and ratio_15m <= 6.0 and mae_15m_ratio <= 2.5:
            state = "RANGING"
            reason = "clean_hold|range_15m<=6w|mae_15m<=2.5w"
        elif mfe_15m_ratio >= 2.0 and mae_15m_ratio <= 2.5:
            state = "EXPANDING"
            reason = "mfe_15m>=2w|mae_15m<=2.5w"
        else:
            state = "UNKNOWN"
            reason = "no_compression_proxy_match"

        return {
            "a2_post_zone_range_15m_u": round(range_15m, 8),
            "a2_post_zone_range_1h_u": round(range_1h, 8),
            "a2_post_zone_range_4h_u": round(range_4h, 8),
            "a2_compression_ratio_15m": round(ratio_15m, 8),
            "a2_compression_ratio_1h": round(ratio_1h, 8),
            "a2_compression_state": state,
            "a2_compression_reason": reason,
        }

    @staticmethod
    def _a3_watch_readiness(
        eligible: bool,
        clean_hold: bool,
        failed_reclaim: bool,
        book_depth_state: str,
        context_alignment: str,
        observe_priority: str,
        strong_tier: str,
    ) -> tuple[bool, str, str]:
        ready = (
            eligible
            and clean_hold
            and book_depth_state == "BOOK_DEPTH_VALID"
            and not failed_reclaim
            and context_alignment != "COUNTER_TREND"
            and observe_priority in {"A2_HIGH", "A2_WATCH"}
        )
        if observe_priority == "A2_HIGH" and strong_tier == "STRONG_A1_RAW":
            priority = "HIGH"
        elif observe_priority == "A2_HIGH":
            priority = "MEDIUM"
        elif observe_priority == "A2_WATCH":
            priority = "LOW"
        else:
            priority = "NONE"
        if not ready:
            return False, "", "NONE"
        reasons = ["clean_hold", "book_depth_valid", "not_failed_reclaim"]
        if context_alignment == "ALIGNED":
            reasons.append("aligned")
        elif context_alignment == "MIXED_OR_UNKNOWN":
            reasons.append("mixed_context")
        if strong_tier == "STRONG_A1_RAW":
            reasons.append("strong_a1")
        return True, "|".join(reasons), priority

    @staticmethod
    def _validation_score(
        eligible: bool,
        clean_hold: bool,
        failed_reclaim: bool,
        book_depth_state: str,
        context_alignment: str,
    ) -> float:
        score = 0.0
        if eligible:
            score += 0.25
        if clean_hold:
            score += 0.35
        if book_depth_state == "BOOK_DEPTH_VALID":
            score += 0.20
        elif book_depth_state == "BOOK_DEPTH_UNKNOWN":
            score += 0.10
        if context_alignment == "ALIGNED":
            score += 0.15
        elif context_alignment == "MIXED_OR_UNKNOWN":
            score += 0.05
        if failed_reclaim:
            score -= 0.50
        return round(max(0.0, min(1.0, score)), 6)
