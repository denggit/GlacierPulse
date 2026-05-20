#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""V6.2 Phase3 research-only virtual position outcome evaluator."""

import logging
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Set

from config import research_evaluator as cfg
from src.strategy.a1_metadata import (
    A1_COUNT_METADATA_FIELDS,
    A1_SCORE_METADATA_FIELDS,
    A1_STRING_METADATA_FIELDS,
)

logger = logging.getLogger(__name__)


class Phase3OutcomeEvaluator:
    CANONICAL_CANDIDATE_TYPES = {
        "SWEEP_RECLAIM_RETEST_ENTRY",
        "CLEAN_HOLD_LOW_RISK",
        "BELOW_ZONE_ABSORPTION_ENTRY",
    }
    PHASE2_TO_CANDIDATE_TYPE = {
        "SWEEP_RECLAIM": "SWEEP_RECLAIM_RETEST_ENTRY",
        "CLEAN_HOLD": "CLEAN_HOLD_LOW_RISK",
        "BELOW_ZONE_ABSORPTION": "BELOW_ZONE_ABSORPTION_ENTRY",
    }
    REQUIRED_FIELDS = (
        "position_id",
        "zone_id",
        "direction",
        "phase2_type",
        "close_reason",
        "realized_pnl_u",
        "realized_r_multiple",
    )

    def __init__(self):
        self.closed_positions: Deque[Dict[str, Any]] = deque(
            maxlen=max(1, int(cfg.PHASE3_OUTCOME_MAX_CLOSED_POSITIONS))
        )
        self.keep_recent_events = bool(cfg.PHASE3_OUTCOME_KEEP_RECENT_EVENTS)
        self.total_closed = 0
        self.total_win = 0
        self.total_loss = 0
        self.total_breakeven_or_flat = 0
        self.cumulative_realized_pnl_u = 0.0
        self.cumulative_realized_r = 0.0
        self.cumulative_positive_r = 0.0
        self.cumulative_negative_r_abs = 0.0
        self.cumulative_mfe_r = 0.0
        self.cumulative_mae_r = 0.0
        self.cumulative_mae_abs_r = 0.0
        self.cumulative_holding_seconds = 0.0
        self.max_win_r = 0.0
        self.max_loss_r = 0.0
        self.last_summary_log_ts = 0.0
        self.group_stats: Dict[str, Dict[str, Any]] = {}
        self.dedup_enabled = bool(cfg.PHASE3_OUTCOME_DEDUP_ENABLED)
        self.processed_position_ids: Deque[str] = deque(
            maxlen=max(1, int(cfg.PHASE3_OUTCOME_DEDUP_MAX_IDS))
        )
        self.processed_position_id_set: Set[str] = set()
        self.total_duplicate_skipped = 0
        self.outcomes = self.closed_positions

    def record(self, outcome: Dict[str, Any]) -> None:
        self.on_virtual_position_closed(outcome)

    def on_virtual_position_closed(self, closed_position: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            if not isinstance(closed_position, dict):
                self._log_skip("input_not_dict", closed_position)
                return None
            if str(closed_position.get("status", "")).upper() != "CLOSED":
                self._log_skip("status_not_closed", closed_position)
                return None
            missing = [field for field in self.REQUIRED_FIELDS if field not in closed_position]
            if missing:
                self._log_skip("missing_required_fields:%s" % ",".join(missing), closed_position)
                return None
            position_id = str(closed_position.get("position_id", ""))
            if not self._mark_processed_position_id(position_id):
                return None

            outcome = self._normalize_closed_position(closed_position)
            if self.keep_recent_events:
                self.closed_positions.append(outcome)
            self._update_global_stats(outcome)
            self._update_group_stats(outcome)
            self._log_outcome(outcome)
            self._maybe_log_summary(self._as_float(outcome.get("close_ts"), time.time()))
            return outcome
        except Exception:
            logger.exception("[PHASE3-OUTCOME-FAILED] closed_position=%s", closed_position)
            return None

    def summary(self) -> Dict[str, Any]:
        return {
            "global": self._global_summary(),
            "groups": {
                key: self._summarize_group(raw_group)
                for key, raw_group in sorted(self.group_stats.items())
            },
            "recent_closed_count": len(self.closed_positions),
            "total_closed": self.total_closed,
            "sample_size": self.total_closed,
            "total_duplicate_skipped": self.total_duplicate_skipped,
            "dedup_enabled": self.dedup_enabled,
            "processed_position_ids_count": len(self.processed_position_id_set),
        }

    def _normalize_closed_position(self, closed_position: Dict[str, Any]) -> Dict[str, Any]:
        realized_r = self._as_float(closed_position.get("realized_r_multiple"))
        max_favorable_r = self._as_float(closed_position.get("max_favorable_r"))
        max_adverse_r = self._as_float(closed_position.get("max_adverse_r"))
        if abs(max_adverse_r) > 0:
            mfe_mae_ratio = max_favorable_r / abs(max_adverse_r)
        else:
            mfe_mae_ratio = max_favorable_r

        direction = str(closed_position.get("direction", "")).upper()
        if direction == "BUY":
            direction = "LONG"
        elif direction == "SELL":
            direction = "SHORT"

        open_ts = self._as_float(closed_position.get("open_ts"))
        close_ts = self._as_float(closed_position.get("close_ts"))
        holding_seconds = self._as_float(
            closed_position.get("holding_seconds"),
            max(0.0, close_ts - open_ts) if close_ts and open_ts else 0.0,
        )
        support_zone_ids = closed_position.get("support_zone_ids")
        support_zone_ids_count = self._as_int(
            closed_position.get("support_zone_ids_count"),
            len(support_zone_ids) if isinstance(support_zone_ids, list) else 0,
        )

        phase2_type = str(closed_position.get("phase2_type", ""))
        position_id = str(closed_position.get("position_id", ""))
        candidate_type = self._normalize_candidate_type(
            closed_position.get("candidate_type"),
            phase2_type=phase2_type,
            position_id=position_id,
        )

        outcome = {
            "position_id": position_id,
            "zone_id": str(closed_position.get("zone_id", "")),
            "direction": direction,
            "phase2_type": phase2_type,
            "candidate_type": candidate_type,
            "open_ts": open_ts,
            "close_ts": close_ts,
            "holding_seconds": holding_seconds,
            "open_price": self._as_float(closed_position.get("open_price")),
            "close_price": self._as_float(closed_position.get("close_price")),
            "close_reason": str(closed_position.get("close_reason", "")),
            "initial_stop": self._as_float(closed_position.get("initial_stop")),
            "dynamic_stop": self._as_float(closed_position.get("dynamic_stop")),
            "exit_stop_used": self._as_float(closed_position.get("exit_stop_used")),
            "take_profit_price": self._as_float(closed_position.get("take_profit_price")),
            "realized_pnl_u": self._as_float(closed_position.get("realized_pnl_u")),
            "realized_pnl_pct_on_equity": self._as_float(closed_position.get("realized_pnl_pct_on_equity")),
            "realized_r_multiple": realized_r,
            "max_favorable_u": self._as_float(closed_position.get("max_favorable_u")),
            "max_adverse_u": self._as_float(closed_position.get("max_adverse_u")),
            "max_favorable_r": max_favorable_r,
            "max_adverse_r": max_adverse_r,
            "mfe_mae_ratio": mfe_mae_ratio,
            "virtual_equity_at_open": self._as_float(closed_position.get("virtual_equity_at_open")),
            "virtual_margin_usdt": self._as_float(closed_position.get("virtual_margin_usdt")),
            "virtual_notional_usdt": self._as_float(closed_position.get("virtual_notional_usdt")),
            "virtual_size_eth": self._as_float(closed_position.get("virtual_size_eth")),
            "leverage": self._as_float(closed_position.get("leverage")),
            "final_margin_usage_pct": self._as_float(closed_position.get("final_margin_usage_pct")),
            "breakeven_activated": self._as_bool(closed_position.get("breakeven_activated")),
            "trailing_activated": self._as_bool(closed_position.get("trailing_activated")),
            "stop_update_count": self._as_int(closed_position.get("stop_update_count")),
            "support_update_count": self._as_int(closed_position.get("support_update_count")),
            "support_zone_ids_count": support_zone_ids_count,
            "last_stop_update_reason": str(closed_position.get("last_stop_update_reason", "")),
            "phase2_total_score": self._as_float(closed_position.get("phase2_total_score")),
            "absorption_score": self._as_float(closed_position.get("absorption_score")),
            "pressure_decay_score": self._as_float(closed_position.get("pressure_decay_score")),
            "reclaim_score": self._as_float(closed_position.get("reclaim_score")),
            "retest_score": self._as_float(closed_position.get("retest_score")),
            "book_absorption_score": self._as_float(closed_position.get("book_absorption_score")),
            "relevant_book_depth_available": self._as_bool(closed_position.get("relevant_book_depth_available")),
            "reload_score": self._as_float(closed_position.get("reload_score")),
            "has_swept_boundary": self._as_bool(closed_position.get("has_swept_boundary")),
            "has_absorbed_after_sweep": self._as_bool(closed_position.get("has_absorbed_after_sweep")),
            "has_reclaimed_boundary": self._as_bool(closed_position.get("has_reclaimed_boundary")),
            "has_retested_inside_zone": self._as_bool(closed_position.get("has_retested_inside_zone")),
        }
        outcome.update(self._a1_metadata_from_closed_position(closed_position))
        outcome["is_win"] = realized_r > 0.0
        outcome["is_loss"] = realized_r < 0.0
        outcome["is_flat"] = realized_r == 0.0
        outcome["outcome_bucket"] = self._outcome_bucket(realized_r)
        return outcome

    def _update_global_stats(self, outcome: Dict[str, Any]) -> None:
        realized_r = self._as_float(outcome.get("realized_r_multiple"))
        realized_pnl_u = self._as_float(outcome.get("realized_pnl_u"))
        self.total_closed += 1
        if outcome.get("is_win"):
            self.total_win += 1
        elif outcome.get("is_loss"):
            self.total_loss += 1
        else:
            self.total_breakeven_or_flat += 1
        self.cumulative_realized_pnl_u += realized_pnl_u
        self.cumulative_realized_r += realized_r
        if realized_r > 0:
            self.cumulative_positive_r += realized_r
        elif realized_r < 0:
            self.cumulative_negative_r_abs += abs(realized_r)
        self.cumulative_mfe_r += self._as_float(outcome.get("max_favorable_r"))
        mae_r = self._as_float(outcome.get("max_adverse_r"))
        self.cumulative_mae_r += mae_r
        self.cumulative_mae_abs_r += abs(mae_r)
        self.cumulative_holding_seconds += self._as_float(outcome.get("holding_seconds"))
        self.max_win_r = max(self.max_win_r, realized_r)
        self.max_loss_r = min(self.max_loss_r, realized_r)

    def _group_keys(self, outcome: Dict[str, Any]) -> List[str]:
        phase2_type = outcome.get("phase2_type")
        candidate_type = outcome.get("candidate_type")
        direction = outcome.get("direction")
        close_reason = outcome.get("close_reason")
        frozen_reason = str(outcome.get("frozen_reason") or "")
        frozen_state = str(outcome.get("frozen_state") or "")
        breakeven = bool(outcome.get("breakeven_activated"))
        trailing = bool(outcome.get("trailing_activated"))
        support_used = self._as_int(outcome.get("support_update_count")) > 0
        keys = [
            "ALL",
            f"phase2_type={phase2_type}",
            f"candidate_type={candidate_type}",
            f"direction={direction}",
            f"close_reason={close_reason}",
            f"breakeven={breakeven}",
            f"trailing={trailing}",
            f"support_used={support_used}",
            f"phase2_type={phase2_type}|direction={direction}",
            f"phase2_type={phase2_type}|close_reason={close_reason}",
            f"phase2_type={phase2_type}|breakeven={breakeven}",
            f"phase2_type={phase2_type}|trailing={trailing}",
            f"phase2_type={phase2_type}|support_used={support_used}",
        ]
        if frozen_reason:
            keys.extend(
                [
                    f"frozen_reason={frozen_reason}",
                    f"phase2_type={phase2_type}|frozen_reason={frozen_reason}",
                    f"candidate_type={candidate_type}|frozen_reason={frozen_reason}",
                    f"direction={direction}|frozen_reason={frozen_reason}",
                ]
            )
        if frozen_state:
            keys.extend(
                [
                    f"frozen_state={frozen_state}",
                    f"phase2_type={phase2_type}|frozen_state={frozen_state}",
                ]
            )
        return [key for key in keys if key and not key.endswith("=None")]

    def _update_group_stats(self, outcome: Dict[str, Any]) -> None:
        for key in self._group_keys(outcome):
            raw_group = self.group_stats.setdefault(key, self._empty_group())
            realized_r = self._as_float(outcome.get("realized_r_multiple"))
            realized_pnl_u = self._as_float(outcome.get("realized_pnl_u"))
            raw_group["count"] += 1
            raw_group["win_count"] += 1 if outcome.get("is_win") else 0
            raw_group["loss_count"] += 1 if outcome.get("is_loss") else 0
            raw_group["flat_count"] += 1 if outcome.get("is_flat") else 0
            raw_group["realized_r_sum"] += realized_r
            raw_group["realized_pnl_u_sum"] += realized_pnl_u
            raw_group["positive_r_sum"] += max(0.0, realized_r)
            raw_group["negative_r_sum"] += min(0.0, realized_r)
            raw_group["max_win_r"] = max(raw_group["max_win_r"], realized_r)
            raw_group["max_loss_r"] = min(raw_group["max_loss_r"], realized_r)
            raw_group["mfe_r_sum"] += self._as_float(outcome.get("max_favorable_r"))
            mae_r = self._as_float(outcome.get("max_adverse_r"))
            raw_group["mae_r_sum"] += mae_r
            raw_group["mae_abs_r_sum"] += abs(mae_r)
            raw_group["holding_seconds_sum"] += self._as_float(outcome.get("holding_seconds"))
            raw_group["stop_loss_count"] += 1 if outcome.get("close_reason") == "STOP_LOSS" else 0
            raw_group["take_profit_count"] += 1 if outcome.get("close_reason") == "TAKE_PROFIT_R_MULTIPLE" else 0
            raw_group["breakeven_count"] += 1 if outcome.get("breakeven_activated") else 0
            raw_group["trailing_count"] += 1 if outcome.get("trailing_activated") else 0
            raw_group["support_used_count"] += 1 if self._as_int(outcome.get("support_update_count")) > 0 else 0

    def _summarize_group(self, raw_group: Dict[str, Any]) -> Dict[str, Any]:
        count = self._as_int(raw_group.get("count"))
        positive_r_sum = self._as_float(raw_group.get("positive_r_sum"))
        negative_r_sum = self._as_float(raw_group.get("negative_r_sum"))
        if negative_r_sum == 0:
            profit_factor_r = 999.0 if positive_r_sum > 0 else 0.0
        else:
            profit_factor_r = positive_r_sum / abs(negative_r_sum)
        return {
            "count": count,
            "win_count": self._as_int(raw_group.get("win_count")),
            "loss_count": self._as_int(raw_group.get("loss_count")),
            "flat_count": self._as_int(raw_group.get("flat_count")),
            "win_rate": self._ratio(raw_group.get("win_count"), count),
            "avg_realized_r": self._ratio(raw_group.get("realized_r_sum"), count),
            "avg_realized_pnl_u": self._ratio(raw_group.get("realized_pnl_u_sum"), count),
            "total_realized_pnl_u": self._as_float(raw_group.get("realized_pnl_u_sum")),
            "profit_factor_r": profit_factor_r,
            "max_win_r": self._as_float(raw_group.get("max_win_r")) if count else 0.0,
            "max_loss_r": self._as_float(raw_group.get("max_loss_r")) if count else 0.0,
            "avg_mfe_r": self._ratio(raw_group.get("mfe_r_sum"), count),
            "avg_mae_r": self._ratio(raw_group.get("mae_r_sum"), count),
            "avg_mae_abs_r": self._ratio(raw_group.get("mae_abs_r_sum"), count),
            "avg_holding_seconds": self._ratio(raw_group.get("holding_seconds_sum"), count),
            "stop_loss_count": self._as_int(raw_group.get("stop_loss_count")),
            "take_profit_count": self._as_int(raw_group.get("take_profit_count")),
            "breakeven_count": self._as_int(raw_group.get("breakeven_count")),
            "trailing_count": self._as_int(raw_group.get("trailing_count")),
            "support_used_count": self._as_int(raw_group.get("support_used_count")),
            "sample_size_too_small": count < max(1, int(cfg.PHASE3_OUTCOME_MIN_GROUP_SAMPLE_SIZE)),
        }

    def _log_outcome(self, outcome: Dict[str, Any]) -> None:
        logger.info(
            "[PHASE3-OUTCOME] position_id=%s zone_id=%s direction=%s phase2_type=%s candidate_type=%s "
            "close_reason=%s open_ts=%.3f close_ts=%.3f holding_seconds=%.3f open_price=%.6f "
            "close_price=%.6f realized_pnl_u=%.6f realized_pnl_pct_on_equity=%.6f "
            "realized_r_multiple=%.6f max_favorable_r=%.6f max_adverse_r=%.6f mfe_mae_ratio=%.6f "
            "outcome_bucket=%s breakeven_activated=%s trailing_activated=%s stop_update_count=%d "
            "support_update_count=%d support_zone_ids_count=%d last_stop_update_reason=%s "
            "phase2_total_score=%.6f absorption_score=%.6f relevant_book_depth_available=%s reload_score=%.6f "
            "frozen_reason=%s frozen_state=%s iceberg_count=%d high_count=%d net_score=%.6f",
            outcome["position_id"], outcome["zone_id"], outcome["direction"], outcome["phase2_type"],
            outcome["candidate_type"], outcome["close_reason"], outcome["open_ts"], outcome["close_ts"],
            outcome["holding_seconds"], outcome["open_price"], outcome["close_price"], outcome["realized_pnl_u"],
            outcome["realized_pnl_pct_on_equity"], outcome["realized_r_multiple"], outcome["max_favorable_r"],
            outcome["max_adverse_r"], outcome["mfe_mae_ratio"], outcome["outcome_bucket"],
            outcome["breakeven_activated"], outcome["trailing_activated"], outcome["stop_update_count"],
            outcome["support_update_count"], outcome["support_zone_ids_count"], outcome["last_stop_update_reason"],
            outcome["phase2_total_score"], outcome["absorption_score"], outcome["relevant_book_depth_available"],
            outcome["reload_score"], outcome["frozen_reason"], outcome["frozen_state"],
            outcome["iceberg_count"], outcome["high_count"], outcome["net_score"],
        )

    def _maybe_log_summary(self, now_ts: float) -> None:
        interval = max(0.0, float(cfg.PHASE3_OUTCOME_SUMMARY_LOG_INTERVAL_SEC))
        if self.last_summary_log_ts > 0 and now_ts - self.last_summary_log_ts < interval:
            return
        if self.last_summary_log_ts <= 0 and now_ts < interval:
            return
        self.last_summary_log_ts = now_ts
        summary = self.summary()
        global_summary = summary["global"]
        groups = summary["groups"]
        best_group = self._best_group(groups, prefix=None, reverse=True)
        worst_group = self._best_group(groups, prefix=None, reverse=False)
        best_phase2 = self._best_group(groups, prefix="phase2_type=", reverse=True)
        worst_phase2 = self._best_group(groups, prefix="phase2_type=", reverse=False)
        logger.info(
            "[PHASE3-OUTCOME-SUMMARY] total_closed=%d win_rate=%.6f avg_realized_r=%.6f "
            "total_realized_pnl_u=%.6f profit_factor_r=%.6f max_win_r=%.6f max_loss_r=%.6f "
            "avg_mfe_r=%.6f avg_mae_r=%.6f avg_mae_abs_r=%.6f duplicate_skipped=%d "
            "recent_closed_count=%d sample_size=%d best_group_by_avg_r=%s worst_group_by_avg_r=%s "
            "best_phase2_type=%s worst_phase2_type=%s",
            global_summary["total_closed"], global_summary["win_rate"], global_summary["avg_realized_r"],
            global_summary["total_realized_pnl_u"], global_summary["profit_factor_r"],
            global_summary["max_win_r"], global_summary["max_loss_r"], global_summary["avg_mfe_r"],
            global_summary["avg_mae_r"], global_summary["avg_mae_abs_r"], self.total_duplicate_skipped,
            summary["recent_closed_count"], summary["sample_size"], best_group, worst_group, best_phase2,
            worst_phase2,
        )

    def _global_summary(self) -> Dict[str, Any]:
        total = self.total_closed
        if self.cumulative_negative_r_abs == 0:
            profit_factor_r = 999.0 if self.cumulative_positive_r > 0 else 0.0
        else:
            profit_factor_r = self.cumulative_positive_r / self.cumulative_negative_r_abs
        return {
            "total_closed": total,
            "win_count": self.total_win,
            "loss_count": self.total_loss,
            "flat_count": self.total_breakeven_or_flat,
            "win_rate": self._ratio(self.total_win, total),
            "avg_realized_r": self._ratio(self.cumulative_realized_r, total),
            "total_realized_pnl_u": self.cumulative_realized_pnl_u,
            "profit_factor_r": profit_factor_r,
            "max_win_r": self.max_win_r if total else 0.0,
            "max_loss_r": self.max_loss_r if total else 0.0,
            "avg_mfe_r": self._ratio(self.cumulative_mfe_r, total),
            "avg_mae_r": self._ratio(self.cumulative_mae_r, total),
            "avg_mae_abs_r": self._ratio(self.cumulative_mae_abs_r, total),
            "avg_holding_seconds": self._ratio(self.cumulative_holding_seconds, total),
            "total_duplicate_skipped": self.total_duplicate_skipped,
        }

    @staticmethod
    def _empty_group() -> Dict[str, Any]:
        return {
            "count": 0,
            "win_count": 0,
            "loss_count": 0,
            "flat_count": 0,
            "realized_r_sum": 0.0,
            "realized_pnl_u_sum": 0.0,
            "positive_r_sum": 0.0,
            "negative_r_sum": 0.0,
            "max_win_r": 0.0,
            "max_loss_r": 0.0,
            "mfe_r_sum": 0.0,
            "mae_r_sum": 0.0,
            "mae_abs_r_sum": 0.0,
            "holding_seconds_sum": 0.0,
            "stop_loss_count": 0,
            "take_profit_count": 0,
            "breakeven_count": 0,
            "trailing_count": 0,
            "support_used_count": 0,
        }

    @staticmethod
    def _outcome_bucket(realized_r: float) -> str:
        if realized_r >= 1.5:
            return "BIG_WIN"
        if realized_r >= 0.5:
            return "SMALL_WIN"
        if realized_r > -0.25:
            return "FLAT_OR_TINY_LOSS"
        if realized_r > -1.0:
            return "CONTROLLED_LOSS"
        return "FULL_R_LOSS_OR_WORSE"

    @staticmethod
    def _best_group(
        groups: Dict[str, Dict[str, Any]],
        prefix: Optional[str],
        reverse: bool,
        exclude_all: bool = True,
        require_min_sample_size: bool = True,
    ) -> str:
        candidates = Phase3OutcomeEvaluator._group_candidates(
            groups=groups,
            prefix=prefix,
            exclude_all=exclude_all,
            require_min_sample_size=require_min_sample_size,
        )
        if not candidates and require_min_sample_size:
            candidates = Phase3OutcomeEvaluator._group_candidates(
                groups=groups,
                prefix=prefix,
                exclude_all=exclude_all,
                require_min_sample_size=False,
            )
        if not candidates:
            return ""
        candidates.sort(key=lambda item: item[1], reverse=reverse)
        return candidates[0][0]

    @staticmethod
    def _group_candidates(
        groups: Dict[str, Dict[str, Any]],
        prefix: Optional[str],
        exclude_all: bool,
        require_min_sample_size: bool,
    ) -> List[Any]:
        candidates = []
        for key, group in groups.items():
            if exclude_all and key == "ALL":
                continue
            if prefix is not None:
                if not key.startswith(prefix) or "|" in key:
                    continue
            if require_min_sample_size and group.get("sample_size_too_small"):
                continue
            candidates.append((key, group.get("avg_realized_r", 0.0)))
        return candidates

    def _normalize_candidate_type(self, value: Any, phase2_type: str = "", position_id: str = "") -> str:
        raw_candidate_type = str(value or "").strip()
        normalized_phase2_type = str(phase2_type or "").strip()
        if (not raw_candidate_type or raw_candidate_type == "RESEARCH") and normalized_phase2_type:
            inferred = self.PHASE2_TO_CANDIDATE_TYPE.get(normalized_phase2_type)
            if inferred:
                return inferred
        if raw_candidate_type in self.CANONICAL_CANDIDATE_TYPES:
            return raw_candidate_type

        normalized_candidate_type = raw_candidate_type or str(cfg.PHASE3_OUTCOME_UNKNOWN_CANDIDATE_TYPE)
        if bool(cfg.PHASE3_OUTCOME_WARN_NON_CANONICAL_CANDIDATE_TYPE):
            logger.warning(
                "[PHASE3-OUTCOME-WARN] reason=non_canonical_candidate_type "
                "raw_candidate_type=%s normalized_candidate_type=%s phase2_type=%s position_id=%s",
                raw_candidate_type,
                normalized_candidate_type,
                normalized_phase2_type,
                position_id,
            )
        return normalized_candidate_type

    def _a1_metadata_from_closed_position(self, closed_position: Dict[str, Any]) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {}
        for field in A1_STRING_METADATA_FIELDS:
            metadata[field] = self._as_str(closed_position.get(field), "")
        for field in A1_COUNT_METADATA_FIELDS:
            metadata[field] = self._as_int(closed_position.get(field), 0)
        for field in A1_SCORE_METADATA_FIELDS:
            metadata[field] = self._as_float(closed_position.get(field), 0.0)
        return metadata

    def _mark_processed_position_id(self, position_id: str) -> bool:
        if not self.dedup_enabled:
            return True
        if not position_id:
            return True
        if position_id in self.processed_position_id_set:
            self.total_duplicate_skipped += 1
            logger.info("[PHASE3-OUTCOME-SKIP] reason=duplicate_position_id position_id=%s", position_id)
            return False
        if len(self.processed_position_ids) == self.processed_position_ids.maxlen:
            oldest_position_id = self.processed_position_ids.popleft()
            self.processed_position_id_set.discard(oldest_position_id)
        self.processed_position_ids.append(position_id)
        self.processed_position_id_set.add(position_id)
        return True

    def _log_skip(self, reason: str, closed_position: Any) -> None:
        logger.info("[PHASE3-OUTCOME-SKIP] reason=%s closed_position=%s", reason, closed_position)

    @staticmethod
    def _ratio(numerator: Any, denominator: Any) -> float:
        denom = Phase3OutcomeEvaluator._as_float(denominator)
        if denom == 0:
            return 0.0
        return Phase3OutcomeEvaluator._as_float(numerator) / denom

    @staticmethod
    def _as_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _as_int(value: Any, default: int = 0) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError, OverflowError):
            return int(default)

    @staticmethod
    def _as_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @staticmethod
    def _as_str(value: Any, default: str = "") -> str:
        if value is None:
            return default
        return str(value)
