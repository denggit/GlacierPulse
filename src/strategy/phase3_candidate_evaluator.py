#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""V6.2 Phase3 research candidate evaluator.

This module is research-only. It consumes Phase2 confirmed snapshots and never
calls trader, creates orders, or creates virtual positions.
"""

import logging
import time
from typing import Any, Dict, Optional, Set

from config import research_evaluator as research_config
from src.research.a1_frozen_metadata import (
    A1_COUNT_METADATA_FIELDS,
    A1_SCORE_METADATA_FIELDS,
    A1_STRING_METADATA_FIELDS,
)

logger = logging.getLogger(__name__)


class Phase3CandidateEvaluator:
    CANDIDATE_TYPES = {
        "SWEEP_RECLAIM": "SWEEP_RECLAIM_RETEST_ENTRY",
        "CLEAN_HOLD": "CLEAN_HOLD_LOW_RISK",
        "BELOW_ZONE_ABSORPTION": "BELOW_ZONE_ABSORPTION_ENTRY",
    }

    def __init__(self, real_trading_enabled: bool = research_config.PHASE3_REAL_TRADING_ENABLED):
        self.real_trading_enabled = bool(real_trading_enabled)
        self.seen_zone_ids: Set[str] = set()

    def evaluate(self, candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Backward-compatible alias for Phase2 confirmed candidate evaluation."""
        return self.evaluate_phase2_confirmed(candidate)

    def evaluate_phase2_confirmed(self, phase2_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Build one research candidate from one PHASE2_CONFIRMED snapshot."""
        try:
            if not isinstance(phase2_event, dict):
                return None
            if phase2_event.get("state") != "PHASE2_CONFIRMED":
                return self._build_and_log_result(
                    phase2_event=phase2_event,
                    candidate_type="UNKNOWN",
                    decision="REJECT_NOT_PHASE2_CONFIRMED",
                    decision_reason="input_state_is_not_phase2_confirmed",
                )

            zone_id = str(phase2_event.get("zone_id") or "").strip()
            if zone_id and zone_id in self.seen_zone_ids:
                return self._build_and_log_result(
                    phase2_event=phase2_event,
                    candidate_type=self.CANDIDATE_TYPES.get(str(phase2_event.get("phase2_type")), "UNKNOWN"),
                    decision="REJECT_DUPLICATE_ZONE",
                    decision_reason="zone_id_already_evaluated",
                )
            if zone_id:
                self.seen_zone_ids.add(zone_id)

            phase2_type = str(phase2_event.get("phase2_type") or "")
            candidate_type = self.CANDIDATE_TYPES.get(phase2_type)
            if not candidate_type:
                return self._build_and_log_result(
                    phase2_event=phase2_event,
                    candidate_type="UNKNOWN",
                    decision="REJECT_UNKNOWN_PHASE2_TYPE",
                    decision_reason="unsupported_phase2_type",
                )

            phase2_score = self._safe_float(phase2_event.get("phase2_total_score"), 0.0)
            absorption_score = self._safe_float(phase2_event.get("absorption_score"), 0.0)
            relevant_book_depth_available = bool(phase2_event.get("relevant_book_depth_available"))

            decision = "ACCEPT_RESEARCH_CANDIDATE"
            decision_reason = "phase2_confirmed_candidate_passed_risk_gate"
            if phase2_type == "SWEEP_RECLAIM":
                if phase2_score < float(research_config.PHASE3_MIN_SWEEP_RECLAIM_PHASE2_SCORE):
                    decision = "REJECT_TOO_LOW_PHASE2_SCORE"
                    decision_reason = "sweep_reclaim_phase2_score_below_threshold"
            elif phase2_type == "CLEAN_HOLD":
                if phase2_score < float(research_config.PHASE3_MIN_CLEAN_HOLD_PHASE2_SCORE):
                    decision = "REJECT_TOO_LOW_PHASE2_SCORE"
                    decision_reason = "clean_hold_phase2_score_below_threshold"
            elif phase2_type == "BELOW_ZONE_ABSORPTION":
                if phase2_score < float(research_config.PHASE3_MIN_BELOW_ZONE_PHASE2_SCORE):
                    decision = "REJECT_TOO_LOW_PHASE2_SCORE"
                    decision_reason = "below_zone_phase2_score_below_threshold"
                elif absorption_score < float(research_config.PHASE3_MIN_BELOW_ZONE_ABSORPTION_SCORE):
                    decision = "REJECT_TOO_LOW_ABSORPTION_SCORE"
                    decision_reason = "below_zone_absorption_score_below_threshold"
                elif (
                    not relevant_book_depth_available
                    and not bool(research_config.PHASE3_ALLOW_BELOW_ZONE_WITHOUT_BOOK_DEPTH)
                ):
                    decision = "WAIT_RECLAIM_OR_MORE_FLOW"
                    decision_reason = "below_zone_without_relevant_book_depth_wait_reclaim_or_more_flow"

            return self._build_and_log_result(
                phase2_event=phase2_event,
                candidate_type=candidate_type,
                decision=decision,
                decision_reason=decision_reason,
            )
        except Exception as exc:
            logger.exception(
                "[PHASE3-CANDIDATE-FAILED] zone_id=%s",
                phase2_event.get("zone_id") if isinstance(phase2_event, dict) else None,
            )
            result = self._minimal_result(
                phase2_event if isinstance(phase2_event, dict) else {},
                decision="REJECT_EXCEPTION",
                decision_reason=f"exception:{exc.__class__.__name__}",
            )
            self._log_candidate(result)
            return result

    def _build_and_log_result(
        self,
        phase2_event: Dict[str, Any],
        candidate_type: str,
        decision: str,
        decision_reason: str,
    ) -> Dict[str, Any]:
        direction = str(phase2_event.get("direction") or "").upper()
        phase2_type = str(phase2_event.get("phase2_type") or "")
        candidate_price = self._safe_float(phase2_event.get("last_price"), 0.0)
        suggested_stop = self._suggested_stop(
            phase2_event=phase2_event,
            direction=direction,
            phase2_type=phase2_type,
        )

        risk_distance_u = 0.0
        risk_distance_pct = 0.0
        total_loss_pct = 0.0
        raw_margin_usage_pct = 0.0
        final_margin_usage_pct = 0.0
        notional_equity_multiple = 0.0
        expected_account_loss_if_sl = 0.0

        stop_valid = False
        if candidate_price > 0 and suggested_stop > 0:
            if direction == "BUY":
                stop_valid = candidate_price > suggested_stop
            elif direction == "SELL":
                stop_valid = candidate_price < suggested_stop
            risk_distance_u = abs(candidate_price - suggested_stop)
            risk_distance_pct = risk_distance_u / candidate_price

        if decision == "ACCEPT_RESEARCH_CANDIDATE":
            if not stop_valid:
                decision = "REJECT_INVALID_STOP"
                decision_reason = "candidate_price_stop_direction_invalid"
            elif risk_distance_pct > float(research_config.PHASE3_MAX_RISK_DISTANCE_PCT):
                decision = "REJECT_TOO_FAR_FROM_STOP"
                decision_reason = "risk_distance_pct_above_phase3_limit"

        if stop_valid:
            total_loss_pct = (
                risk_distance_pct
                + float(research_config.PHASE3_ROUNDTRIP_FEE_PCT)
                + float(research_config.PHASE3_SLIPPAGE_BUFFER_PCT)
            )
            leverage = float(research_config.PHASE3_LEVERAGE)
            if total_loss_pct > 0 and leverage > 0:
                raw_margin_usage_pct = (
                    float(research_config.PHASE3_MAX_ACCOUNT_LOSS_PCT)
                    / (leverage * total_loss_pct)
                )
                final_margin_usage_pct = min(
                    raw_margin_usage_pct,
                    float(research_config.PHASE3_MAX_MARGIN_USAGE_PCT),
                )
                notional_equity_multiple = final_margin_usage_pct * leverage
                expected_account_loss_if_sl = final_margin_usage_pct * leverage * total_loss_pct

        if (
            decision == "ACCEPT_RESEARCH_CANDIDATE"
            and final_margin_usage_pct < float(research_config.PHASE3_MIN_MARGIN_USAGE_PCT)
        ):
            decision = "REJECT_MARGIN_TOO_SMALL"
            decision_reason = "final_margin_usage_pct_below_phase3_minimum"

        result = {
            "zone_id": str(phase2_event.get("zone_id") or ""),
            "direction": direction,
            "phase2_type": phase2_type,
            "candidate_type": candidate_type,
            "decision": decision,
            "decision_reason": decision_reason,
            "candidate_ts": self._candidate_ts(phase2_event),
            "candidate_price": candidate_price,
            "suggested_stop": suggested_stop,
            "risk_distance_u": risk_distance_u,
            "risk_distance_pct": risk_distance_pct,
            "total_loss_pct": total_loss_pct,
            "leverage": float(research_config.PHASE3_LEVERAGE),
            "max_account_loss_pct": float(research_config.PHASE3_MAX_ACCOUNT_LOSS_PCT),
            "roundtrip_fee_pct": float(research_config.PHASE3_ROUNDTRIP_FEE_PCT),
            "slippage_buffer_pct": float(research_config.PHASE3_SLIPPAGE_BUFFER_PCT),
            "raw_margin_usage_pct": raw_margin_usage_pct,
            "final_margin_usage_pct": final_margin_usage_pct,
            "notional_equity_multiple": notional_equity_multiple,
            "expected_account_loss_if_sl": expected_account_loss_if_sl,
            "phase2_total_score": self._safe_float(phase2_event.get("phase2_total_score"), 0.0),
            "absorption_score": self._safe_float(phase2_event.get("absorption_score"), 0.0),
            "pressure_decay_score": self._safe_float(phase2_event.get("pressure_decay_score"), 0.0),
            "reclaim_score": self._safe_float(phase2_event.get("reclaim_score"), 0.0),
            "retest_score": self._safe_float(phase2_event.get("retest_score"), 0.0),
            "book_absorption_score": self._safe_float(phase2_event.get("book_absorption_score"), 0.0),
            "relevant_book_depth_available": bool(phase2_event.get("relevant_book_depth_available")),
            "reload_score": self._safe_float(phase2_event.get("reload_score"), 0.0),
            "has_swept_boundary": bool(phase2_event.get("has_swept_boundary")),
            "has_absorbed_after_sweep": bool(phase2_event.get("has_absorbed_after_sweep")),
            "has_reclaimed_boundary": bool(phase2_event.get("has_reclaimed_boundary")),
            "has_retested_inside_zone": bool(phase2_event.get("has_retested_inside_zone")),
        }
        result.update(self._a1_metadata_from_phase2_event(phase2_event))
        self._log_candidate(result)
        if self.real_trading_enabled:
            logger.warning("[PHASE3-REAL-TRADING-BLOCKED] reason=v62_research_candidate_only")
        return result

    def _suggested_stop(self, phase2_event: Dict[str, Any], direction: str, phase2_type: str) -> float:
        sweep_extreme = self._safe_float(phase2_event.get("sweep_extreme"), 0.0)
        frozen_low = self._safe_float(phase2_event.get("frozen_low"), 0.0)
        frozen_high = self._safe_float(phase2_event.get("frozen_high"), 0.0)
        if phase2_type == "SWEEP_RECLAIM":
            event_stop = self._safe_float(phase2_event.get("suggested_stop"), 0.0)
            if event_stop > 0:
                return event_stop
        if phase2_type == "CLEAN_HOLD":
            if direction == "BUY":
                return frozen_low - float(research_config.PHASE3_CLEAN_HOLD_STOP_BUFFER_USDT)
            if direction == "SELL":
                return frozen_high + float(research_config.PHASE3_CLEAN_HOLD_STOP_BUFFER_USDT)
        if phase2_type == "BELOW_ZONE_ABSORPTION":
            buffer_usdt = float(research_config.PHASE3_BELOW_ZONE_STOP_BUFFER_USDT)
        else:
            buffer_usdt = float(research_config.PHASE3_STOP_BUFFER_USDT)
        if direction == "BUY" and sweep_extreme > 0:
            return sweep_extreme - buffer_usdt
        if direction == "SELL" and sweep_extreme > 0:
            return sweep_extreme + buffer_usdt
        return 0.0

    def _minimal_result(
        self,
        phase2_event: Dict[str, Any],
        decision: str,
        decision_reason: str,
    ) -> Dict[str, Any]:
        result = {
            "zone_id": str(phase2_event.get("zone_id") or ""),
            "direction": str(phase2_event.get("direction") or "").upper(),
            "phase2_type": str(phase2_event.get("phase2_type") or ""),
            "candidate_type": "UNKNOWN",
            "decision": decision,
            "decision_reason": decision_reason,
            "candidate_ts": self._candidate_ts(phase2_event),
            "candidate_price": self._safe_float(phase2_event.get("last_price"), 0.0),
            "suggested_stop": self._safe_float(phase2_event.get("suggested_stop"), 0.0),
            "risk_distance_u": 0.0,
            "risk_distance_pct": 0.0,
            "total_loss_pct": 0.0,
            "leverage": float(research_config.PHASE3_LEVERAGE),
            "max_account_loss_pct": float(research_config.PHASE3_MAX_ACCOUNT_LOSS_PCT),
            "roundtrip_fee_pct": float(research_config.PHASE3_ROUNDTRIP_FEE_PCT),
            "slippage_buffer_pct": float(research_config.PHASE3_SLIPPAGE_BUFFER_PCT),
            "raw_margin_usage_pct": 0.0,
            "final_margin_usage_pct": 0.0,
            "notional_equity_multiple": 0.0,
            "expected_account_loss_if_sl": 0.0,
            "phase2_total_score": self._safe_float(phase2_event.get("phase2_total_score"), 0.0),
            "absorption_score": self._safe_float(phase2_event.get("absorption_score"), 0.0),
            "pressure_decay_score": self._safe_float(phase2_event.get("pressure_decay_score"), 0.0),
            "reclaim_score": self._safe_float(phase2_event.get("reclaim_score"), 0.0),
            "retest_score": self._safe_float(phase2_event.get("retest_score"), 0.0),
            "book_absorption_score": self._safe_float(phase2_event.get("book_absorption_score"), 0.0),
            "relevant_book_depth_available": bool(phase2_event.get("relevant_book_depth_available")),
            "reload_score": self._safe_float(phase2_event.get("reload_score"), 0.0),
            "has_swept_boundary": bool(phase2_event.get("has_swept_boundary")),
            "has_absorbed_after_sweep": bool(phase2_event.get("has_absorbed_after_sweep")),
            "has_reclaimed_boundary": bool(phase2_event.get("has_reclaimed_boundary")),
            "has_retested_inside_zone": bool(phase2_event.get("has_retested_inside_zone")),
        }
        result.update(self._a1_metadata_from_phase2_event(phase2_event))
        return result

    def _a1_metadata_from_phase2_event(self, phase2_event: Dict[str, Any]) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {}
        for field in A1_STRING_METADATA_FIELDS:
            metadata[field] = self._safe_str(phase2_event.get(field), "")
        for field in A1_COUNT_METADATA_FIELDS:
            metadata[field] = self._safe_int(phase2_event.get(field), 0)
        for field in A1_SCORE_METADATA_FIELDS:
            metadata[field] = self._safe_float(phase2_event.get(field), 0.0)
        return metadata

    def _log_candidate(self, result: Dict[str, Any]) -> None:
        logger.info(
            "[PHASE3-CANDIDATE] zone_id=%s direction=%s phase2_type=%s candidate_type=%s decision=%s decision_reason=%s candidate_ts=%s candidate_price=%s suggested_stop=%s risk_distance_u=%s risk_distance_pct=%s total_loss_pct=%s leverage=%s max_account_loss_pct=%s roundtrip_fee_pct=%s slippage_buffer_pct=%s raw_margin_usage_pct=%s final_margin_usage_pct=%s notional_equity_multiple=%s expected_account_loss_if_sl=%s phase2_total_score=%s absorption_score=%s pressure_decay_score=%s reclaim_score=%s retest_score=%s book_absorption_score=%s relevant_book_depth_available=%s reload_score=%s has_swept_boundary=%s has_absorbed_after_sweep=%s has_reclaimed_boundary=%s has_retested_inside_zone=%s frozen_reason=%s frozen_state=%s iceberg_count=%s high_count=%s net_score=%s",
            result.get("zone_id"),
            result.get("direction"),
            result.get("phase2_type"),
            result.get("candidate_type"),
            result.get("decision"),
            result.get("decision_reason"),
            result.get("candidate_ts"),
            result.get("candidate_price"),
            result.get("suggested_stop"),
            result.get("risk_distance_u"),
            result.get("risk_distance_pct"),
            result.get("total_loss_pct"),
            result.get("leverage"),
            result.get("max_account_loss_pct"),
            result.get("roundtrip_fee_pct"),
            result.get("slippage_buffer_pct"),
            result.get("raw_margin_usage_pct"),
            result.get("final_margin_usage_pct"),
            result.get("notional_equity_multiple"),
            result.get("expected_account_loss_if_sl"),
            result.get("phase2_total_score"),
            result.get("absorption_score"),
            result.get("pressure_decay_score"),
            result.get("reclaim_score"),
            result.get("retest_score"),
            result.get("book_absorption_score"),
            result.get("relevant_book_depth_available"),
            result.get("reload_score"),
            result.get("has_swept_boundary"),
            result.get("has_absorbed_after_sweep"),
            result.get("has_reclaimed_boundary"),
            result.get("has_retested_inside_zone"),
            result.get("frozen_reason"),
            result.get("frozen_state"),
            result.get("iceberg_count"),
            result.get("high_count"),
            result.get("net_score"),
        )

    def _candidate_ts(self, phase2_event: Dict[str, Any]) -> float:
        candidate_ts = self._safe_float(
            phase2_event.get("confirmed_ts", phase2_event.get("candidate_ts")),
            0.0,
        )
        return candidate_ts if candidate_ts > 0 else time.time()

    @staticmethod
    def _safe_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError, OverflowError):
            return int(default)

    @staticmethod
    def _safe_str(value: Any, default: str = "") -> str:
        if value is None:
            return default
        return str(value)
