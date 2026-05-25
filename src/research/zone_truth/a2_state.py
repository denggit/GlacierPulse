#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Iterable, Mapping

from src.research.a1_edge.schema import parse_bool, parse_float


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
        validated_candidate = (
            eligible
            and clean_hold
            and not failed_reclaim
            and book_depth_state == "BOOK_DEPTH_VALID"
            and context_alignment != "COUNTER_TREND"
        )

        event_ts = parse_float(result.get("reaction_event_ts"))
        result.update(
            {
                "a2_state": state,
                "a2_state_reason": reason,
                "a2_state_ts": self._state_ts(result),
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
