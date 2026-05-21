#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Research runtime monitor for safety checks, heartbeat logs, and run summaries.

The log tags may include V62 while this monitor is used by the V6.2 research
chain, but the module itself is version-neutral and reusable.
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

from config import research_evaluator as cfg
from src.utils.log_noise import suppressed_log_counter

logger = logging.getLogger(__name__)


class ResearchRuntimeMonitor:
    def __init__(
        self,
        phase1_engine: Any,
        label: str = "",
    ):
        self.phase1_engine = phase1_engine
        self.label = label
        self.started_ts = time.time()
        self.last_heartbeat_ts = 0.0
        self.heartbeat_count = 0
        self.safety_check_passed = False
        self.safety_issues: List[str] = []

    def run_startup_safety_check(self) -> Dict[str, Any]:
        status = self._collect_component_status()
        issues: List[str] = []
        research_only_required = bool(getattr(cfg, "V62_REQUIRE_RESEARCH_ONLY_MODE", True))
        real_execution_enabled = bool(getattr(cfg, "REAL_EXECUTION_ENABLED", False))
        phase3_real_trading_enabled = bool(getattr(cfg, "PHASE3_REAL_TRADING_ENABLED", False))
        virtual_shadow_mode = bool(getattr(cfg, "VIRTUAL_SHADOW_MODE", False))
        virtual_manager_active = bool(status.get("virtual_position_manager_active"))

        if research_only_required:
            if real_execution_enabled:
                issues.append("REAL_EXECUTION_ENABLED=True")
            if phase3_real_trading_enabled:
                issues.append("PHASE3_REAL_TRADING_ENABLED=True")
        if real_execution_enabled and not virtual_shadow_mode and virtual_manager_active:
            issues.append("virtual_position_manager_active_with_real_execution_without_shadow")

        self.safety_issues = issues
        self.safety_check_passed = not issues

        result = {
            "label": self.label,
            "research_only_required": research_only_required,
            "real_execution_enabled": real_execution_enabled,
            "phase3_real_trading_enabled": phase3_real_trading_enabled,
            "virtual_position_manager_enabled": bool(getattr(cfg, "VIRTUAL_POSITION_MANAGER_ENABLED", False)),
            "virtual_shadow_mode": virtual_shadow_mode,
            "virtual_manager_active": virtual_manager_active,
            "phase2_enabled": bool(getattr(cfg, "PHASE2_ORDERFLOW_EVALUATOR_ENABLED", False)),
            "phase3_candidate_enabled": bool(getattr(cfg, "PHASE3_CANDIDATE_EVALUATOR_ENABLED", False)),
            "phase3_outcome_enabled": bool(getattr(cfg, "PHASE3_OUTCOME_EVALUATOR_ENABLED", False)),
            "issues": list(issues),
        }

        if issues:
            logger.error(
                "[V62-SAFETY-CHECK-FAILED] %s",
                self._format_kv(
                    {
                        "label": self.label,
                        "issues": ",".join(issues),
                        "real_execution_enabled": real_execution_enabled,
                        "phase3_real_trading_enabled": phase3_real_trading_enabled,
                        "virtual_shadow_mode": virtual_shadow_mode,
                        "virtual_manager_active": virtual_manager_active,
                    }
                ),
            )
        elif bool(getattr(cfg, "V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED", True)):
            logger.info(
                "[V62-SAFETY-CHECK-PASSED] %s",
                self._format_kv(
                    {
                        "label": self.label,
                        "research_only_required": research_only_required,
                        "real_execution_enabled": real_execution_enabled,
                        "phase3_real_trading_enabled": phase3_real_trading_enabled,
                        "virtual_position_manager_enabled": result["virtual_position_manager_enabled"],
                        "virtual_shadow_mode": virtual_shadow_mode,
                        "virtual_manager_active": virtual_manager_active,
                        "phase2_enabled": result["phase2_enabled"],
                        "phase3_candidate_enabled": result["phase3_candidate_enabled"],
                        "phase3_outcome_enabled": result["phase3_outcome_enabled"],
                    }
                ),
            )
        return result

    def log_component_status(self) -> Dict[str, Any]:
        status = self._collect_component_status()
        if bool(getattr(cfg, "V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED", True)):
            logger.info("[V62-COMPONENT-STATUS] %s", self._format_kv(status))
        return status

    def log_config_snapshot(self) -> Dict[str, Any]:
        snapshot = self._collect_config_snapshot()
        if bool(getattr(cfg, "V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED", True)):
            logger.info("[V62-CONFIG-SNAPSHOT] %s", self._format_kv(snapshot))
        return snapshot

    def maybe_log_heartbeat(self, now_ts: Optional[float] = None) -> Optional[Dict[str, Any]]:
        if not bool(getattr(cfg, "V62_INTEGRATION_HEARTBEAT_ENABLED", True)):
            return None
        now = self._safe_float(now_ts, time.time()) if now_ts is not None else time.time()
        interval = max(0.0, self._safe_float(getattr(cfg, "V62_INTEGRATION_HEARTBEAT_INTERVAL_SEC", 300.0), 300.0))
        if self.last_heartbeat_ts and now - self.last_heartbeat_ts < interval:
            return None

        try:
            self.heartbeat_count += 1
            self.last_heartbeat_ts = now
            summary = self._heartbeat_summary(now)
            if bool(getattr(cfg, "V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED", True)):
                logger.info("[V62-HEARTBEAT] %s", self._format_kv(summary))
            return summary
        except Exception:
            logger.exception("[V62-HEARTBEAT-FAILED] label=%s", self.label)
            return None

    def summary(self) -> Dict[str, Any]:
        now = time.time()
        return {
            "label": self.label,
            "started_ts": self.started_ts,
            "uptime_sec": max(0.0, now - self.started_ts),
            "heartbeat_count": self.heartbeat_count,
            "safety_check_passed": self.safety_check_passed,
            "safety_issues": list(self.safety_issues),
            "component_status": self._safe_collect(self._collect_component_status),
            "config_snapshot": self._safe_collect(self._collect_config_snapshot),
            "phase2_summary": self._phase2_summary(),
            "phase3_candidate_summary": self._phase3_candidate_summary(),
            "virtual_position_summary": self._virtual_position_summary(),
            "outcome_summary": self._outcome_summary(),
        }

    def log_final_summary(self) -> Dict[str, Any]:
        if not bool(getattr(cfg, "V62_ENABLE_FINAL_RUN_SUMMARY", True)):
            return {}
        summary = self.summary()
        virtual_summary = summary.get("virtual_position_summary") or {}
        outcome_summary = self._flatten_outcome_summary(summary.get("outcome_summary") or {})
        final = {
            "label": self.label,
            "uptime_sec": summary.get("uptime_sec", 0.0),
            "heartbeat_count": self.heartbeat_count,
            "safety_check_passed": self.safety_check_passed,
            "safety_issues_count": len(self.safety_issues),
            "virtual_total_opened": virtual_summary.get("total_opened", 0),
            "virtual_total_closed": virtual_summary.get("total_closed", 0),
            "virtual_equity_usdt": virtual_summary.get("virtual_equity_usdt", 0.0),
            "outcome_total_closed": outcome_summary.get("total_closed", 0),
            "outcome_win_rate": outcome_summary.get("win_rate", 0.0),
            "outcome_avg_realized_r": outcome_summary.get("avg_realized_r", 0.0),
            "outcome_profit_factor_r": outcome_summary.get("profit_factor_r", 0.0),
            "outcome_total_realized_pnl_u": outcome_summary.get("total_realized_pnl_u", 0.0),
            "real_execution_enabled": bool(getattr(cfg, "REAL_EXECUTION_ENABLED", False)),
            "phase3_real_trading_enabled": bool(getattr(cfg, "PHASE3_REAL_TRADING_ENABLED", False)),
        }
        if bool(getattr(cfg, "V62_LOG_SAFETY_AND_HEARTBEAT_ENABLED", True)):
            logger.info("[V62-FINAL-SUMMARY] %s", self._format_kv(final))
        return final

    def _heartbeat_summary(self, now_ts: float) -> Dict[str, Any]:
        phase2_summary = self._phase2_summary()
        candidate_summary = self._phase3_candidate_summary()
        virtual_summary = self._virtual_position_summary()
        outcome_summary = self._flatten_outcome_summary(self._outcome_summary())

        recorder_summary = self._a1_reaction_research_summary()
        summary = {
            "label": self.label,
            "uptime_sec": max(0.0, now_ts - self.started_ts),
            "heartbeat_count": self.heartbeat_count,
            "safety_check_passed": self.safety_check_passed,
            "phase2_active_zones": phase2_summary.get("active_zones_count", 0),
            "phase2_confirmed_queue": phase2_summary.get("confirmed_events_queue_count", 0),
            "phase2_total_registered_zones": phase2_summary.get("total_registered_zones", 0),
            "phase2_total_confirmed_zones": phase2_summary.get("total_confirmed_zones", 0),
            "phase2_total_failed_zones": phase2_summary.get("total_failed_zones", 0),
            "phase2_total_timeout_zones": phase2_summary.get("total_timeout_zones", 0),
            "phase3_total_candidates": candidate_summary.get("total_candidates", 0),
            "phase3_accepted_candidates": candidate_summary.get("accepted_candidates", 0),
            "phase3_wait_candidates": candidate_summary.get("wait_candidates", 0),
            "phase3_rejected_candidates": candidate_summary.get("rejected_candidates", 0),
            "a1_reaction_research_recorder_active": recorder_summary.get("active", False),
            "a1_reaction_research_total_events": recorder_summary.get("total_events", 0),
            "a1_reaction_research_total_confirmed": recorder_summary.get("total_confirmed", 0),
            "a1_reaction_research_total_failed": recorder_summary.get("total_failed", 0),
            "a1_reaction_research_total_timeout": recorder_summary.get("total_timeout", 0),
            "a1_reaction_research_total_no_response": recorder_summary.get("total_no_response", 0),
            "a1_reaction_research_total_missed_fast_move": recorder_summary.get("total_missed_fast_move", 0),
            "a1_reaction_research_total_sweep_no_reclaim": recorder_summary.get("total_sweep_no_reclaim", 0),
            "a1_reaction_research_total_reclaim_no_retest": recorder_summary.get("total_reclaim_no_retest", 0),
            "virtual_active_position_exists": virtual_summary.get("active_position_exists", False),
            "virtual_equity_usdt": virtual_summary.get("virtual_equity_usdt", 0.0),
            "virtual_total_opened": virtual_summary.get("total_opened", 0),
            "virtual_total_closed": virtual_summary.get("total_closed", 0),
            "virtual_total_skipped": virtual_summary.get("total_skipped", 0),
            "virtual_total_rejected": virtual_summary.get("total_rejected", 0),
            "virtual_active_dynamic_stop": virtual_summary.get("active_dynamic_stop", 0.0),
            "virtual_active_breakeven_activated": virtual_summary.get("active_breakeven_activated", False),
            "virtual_active_trailing_activated": virtual_summary.get("active_trailing_activated", False),
            "virtual_active_support_update_count": virtual_summary.get("active_support_update_count", 0),
            "outcome_total_closed": outcome_summary.get("total_closed", 0),
            "outcome_win_rate": outcome_summary.get("win_rate", 0.0),
            "outcome_avg_realized_r": outcome_summary.get("avg_realized_r", 0.0),
            "outcome_total_realized_pnl_u": outcome_summary.get("total_realized_pnl_u", 0.0),
            "outcome_profit_factor_r": outcome_summary.get("profit_factor_r", 0.0),
            "outcome_avg_mfe_r": outcome_summary.get("avg_mfe_r", 0.0),
            "outcome_avg_mae_r": outcome_summary.get("avg_mae_r", 0.0),
            "outcome_avg_mae_abs_r": outcome_summary.get("avg_mae_abs_r", 0.0),
            "outcome_total_duplicate_skipped": outcome_summary.get("total_duplicate_skipped", 0),
            "real_execution_enabled": bool(getattr(cfg, "REAL_EXECUTION_ENABLED", False)),
            "phase3_real_trading_enabled": bool(getattr(cfg, "PHASE3_REAL_TRADING_ENABLED", False)),
        }
        summary.update(self._suppressed_log_summary())

        if bool(getattr(cfg, "V62_HEARTBEAT_INCLUDE_GROUP_SUMMARY", True)):
            summary.update(self._outcome_group_highlights(self._outcome_summary()))
        return summary


    def _a1_reaction_research_summary(self) -> Dict[str, Any]:
        recorder = getattr(self.phase1_engine, "a1_reaction_event_recorder", None)
        default = {"active": False, "total_events": 0, "total_confirmed": 0, "total_failed": 0, "total_timeout": 0, "total_no_response": 0, "total_missed_fast_move": 0, "total_sweep_no_reclaim": 0, "total_reclaim_no_retest": 0, "by_a1_reaction_type": {}, "by_event_kind": {}}
        if recorder is None:
            return default
        try:
            summary_fn = getattr(recorder, "summary", None)
            if callable(summary_fn):
                summary = dict(summary_fn())
                summary.setdefault("active", bool(getattr(recorder, "enabled", True)))
                for k,v in default.items(): summary.setdefault(k,v)
                return summary
            default["active"] = True
            return default
        except Exception:
            logger.exception("[V62-SUMMARY-FAILED] component=a1_reaction_research")
            return default
    def _collect_component_status(self) -> Dict[str, Any]:
        engine = self.phase1_engine
        a1_reaction_active = getattr(engine, "a1_reaction_evaluator", None) is not None
        candidate_risk_active = getattr(engine, "candidate_risk_evaluator", None) is not None
        execution_outcome_active = getattr(engine, "execution_outcome_evaluator", None) is not None
        a1_zone_tracker_active = getattr(engine, "zone_tracker", None) is not None
        a1_outcome_evaluator_active = getattr(engine, "outcome_evaluator", None) is not None
        return {
            "label": self.label,
            "a1_reaction_evaluator_active": a1_reaction_active,
            "candidate_risk_evaluator_active": candidate_risk_active,
            "execution_outcome_evaluator_active": execution_outcome_active,
            "a1_zone_tracker_active": a1_zone_tracker_active,
            "a1_outcome_evaluator_active": a1_outcome_evaluator_active,
            # Legacy compatibility only: downstream dashboards still grep these
            # V6.2/V6.3 field names while V6.3.8.x moves code to A1 semantics.
            "phase2_orderflow_evaluator_active": a1_reaction_active,
            "phase3_candidate_evaluator_active": candidate_risk_active,
            "virtual_position_manager_active": getattr(engine, "virtual_position_manager", None) is not None,
            "a1_to_virtual_chain_enabled": bool(getattr(cfg, "A1_REACTION_TO_VIRTUAL_POSITION_ENABLED", False)),
            "phase3_outcome_evaluator_active": execution_outcome_active,
            "iceberg_zone_tracker_active": a1_zone_tracker_active,
            "iceberg_outcome_evaluator_active": a1_outcome_evaluator_active,
            "real_execution_enabled": bool(getattr(cfg, "REAL_EXECUTION_ENABLED", False)),
            "phase3_real_trading_enabled": bool(getattr(cfg, "PHASE3_REAL_TRADING_ENABLED", False)),
            "virtual_shadow_mode": bool(getattr(cfg, "VIRTUAL_SHADOW_MODE", False)),
        }

    def _collect_config_snapshot(self) -> Dict[str, Any]:
        keys = (
            "PHASE2_ORDERFLOW_EVALUATOR_ENABLED",
            "PHASE3_CANDIDATE_EVALUATOR_ENABLED",
            "PHASE3_OUTCOME_EVALUATOR_ENABLED",
            "VIRTUAL_POSITION_MANAGER_ENABLED",
            "A1_REACTION_TO_VIRTUAL_POSITION_ENABLED",
            "REAL_EXECUTION_ENABLED",
            "PHASE3_REAL_TRADING_ENABLED",
            "VIRTUAL_SHADOW_MODE",
            "PHASE2_ZONE_TTL_SECONDS",
            "MAX_ACTIVE_PHASE2_ZONES",
            "PHASE3_MAX_ACCOUNT_LOSS_PCT",
            "PHASE3_LEVERAGE",
            "PHASE3_MAX_MARGIN_USAGE_PCT",
            "VIRTUAL_INITIAL_EQUITY_USDT",
            "VIRTUAL_TAKE_PROFIT_R_MULTIPLE",
            "VIRTUAL_BREAKEVEN_ENABLED",
            "VIRTUAL_TRAILING_ENABLED",
            "VIRTUAL_SUPPORT_UPDATE_ENABLED",
            "PHASE3_OUTCOME_MAX_CLOSED_POSITIONS",
            "PHASE3_OUTCOME_SUMMARY_LOG_INTERVAL_SEC",
            "V62_INTEGRATION_HEARTBEAT_INTERVAL_SEC",
            "V62_SHADOW_RUN_LABEL",
            "V62_LOG_PROFILE",
            "LOG_TO_CONSOLE",
            "LOG_TO_FILE",
            "LOG_DIR",
            "LOG_FILE_NAME",
            "V62_LOG_PENDING_ICEBERG_ENABLED",
            "V62_LOG_IGNORE_ICEBERG_ENABLED",
            "V62_LOG_SPOOFING_WITHDRAWAL_ENABLED",
            "V62_LOG_SETTLED_ICEBERG_ENABLED",
            "V62_LOG_PHASE1_QUALITY_ENABLED",
            "V62_LOG_CANCEL_ICEBERG_ENABLED",
            "V62_LOG_PENDING_DROP_ENABLED",
            "V62_LOG_A1_ZONE_NEW_ENABLED",
            "V62_LOG_A1_ZONE_STRESSED_ENABLED",
            "V62_LOG_A1_ZONE_FROZEN_ENABLED",
            "V62_LOG_PHASE2_STATE_ENABLED",
            "V62_LOG_PHASE3_CANDIDATE_ENABLED",
            "V62_LOG_VIRTUAL_POSITION_UPDATE_ENABLED",
            "V62_LOG_VIRTUAL_POSITION_SKIP_ENABLED",
            "V62_LOG_VIRTUAL_POSITION_BLOCKED_ENABLED",
            "V62_LOG_PHASE3_OUTCOME_ENABLED",
        )
        snapshot = {key: getattr(cfg, key, None) for key in keys}
        snapshot.update(
            {
                "LOG_TO_CONSOLE": os.environ.get("LOG_TO_CONSOLE", "true"),
                "LOG_TO_FILE": os.environ.get("LOG_TO_FILE", "true"),
                "LOG_DIR": os.environ.get("LOG_DIR", "logs"),
                "LOG_FILE_NAME": os.environ.get("LOG_FILE_NAME", "app.log"),
            }
        )
        return snapshot

    def _suppressed_log_summary(self) -> Dict[str, Any]:
        snapshot = suppressed_log_counter.snapshot_and_reset()
        keys = (
            "suppressed_pending_iceberg_count",
            "suppressed_ignore_iceberg_count",
            "suppressed_spoofing_withdrawal_count",
            "suppressed_zone_new_count",
            "suppressed_virtual_update_count",
            "suppressed_settled_iceberg_count",
            "suppressed_phase1_quality_count",
            "suppressed_cancel_iceberg_count",
            "suppressed_zone_stressed_count",
            "suppressed_virtual_skip_count",
            "suppressed_pending_drop_count",
        )
        return {key: int(snapshot.get(key, 0)) for key in keys}

    def _phase2_summary(self) -> Dict[str, Any]:
        evaluator = getattr(self.phase1_engine, "a1_reaction_evaluator", None)
        if evaluator is None:
            return {}
        try:
            active_zones = getattr(evaluator, "active_zones", None)
            confirmed_events = getattr(evaluator, "confirmed_events", None)
            return {
                "active": True,
                "active_zones_count": len(active_zones) if active_zones is not None else None,
                "confirmed_events_queue_count": len(confirmed_events) if confirmed_events is not None else None,
                "total_registered_zones": self._first_attr(evaluator, ("total_registered_zones", "total_registered"), 0),
                "total_confirmed_zones": self._first_attr(evaluator, ("total_confirmed_zones", "total_confirmed"), 0),
                "total_failed_zones": self._first_attr(evaluator, ("total_failed_zones", "total_failed"), 0),
                "total_timeout_zones": self._first_attr(evaluator, ("total_timeout_zones", "total_timeout"), 0),
            }
        except Exception:
            logger.exception("[V62-SUMMARY-FAILED] component=phase2")
            return {}

    def _phase3_candidate_summary(self) -> Dict[str, Any]:
        evaluator = getattr(self.phase1_engine, "candidate_risk_evaluator", None)
        if evaluator is None:
            return {}
        try:
            summary_fn = getattr(evaluator, "summary", None)
            if callable(summary_fn):
                summary = summary_fn()
                return dict(summary) if isinstance(summary, dict) else {"active": True}
            return {
                "active": True,
                "total_candidates": self._first_attr(evaluator, ("total_candidates",), 0),
                "accepted_candidates": self._first_attr(evaluator, ("accepted_candidates",), 0),
                "wait_candidates": self._first_attr(evaluator, ("wait_candidates",), 0),
                "rejected_candidates": self._first_attr(evaluator, ("rejected_candidates",), 0),
            }
        except Exception:
            logger.exception("[V62-SUMMARY-FAILED] component=phase3_candidate")
            return {}

    def _virtual_position_summary(self) -> Dict[str, Any]:
        manager = getattr(self.phase1_engine, "virtual_position_manager", None)
        if manager is None:
            return {}
        try:
            summary_fn = getattr(manager, "summary", None)
            if callable(summary_fn):
                summary = summary_fn()
                return dict(summary) if isinstance(summary, dict) else {"active": True}
            return {"active": True}
        except Exception:
            logger.exception("[V62-SUMMARY-FAILED] component=virtual_position")
            return {}

    def _outcome_summary(self) -> Dict[str, Any]:
        evaluator = getattr(self.phase1_engine, "execution_outcome_evaluator", None)
        if evaluator is None:
            return {}
        try:
            summary_fn = getattr(evaluator, "summary", None)
            if callable(summary_fn):
                summary = summary_fn()
                return dict(summary) if isinstance(summary, dict) else {"active": True}
            return {"active": True}
        except Exception:
            logger.exception("[V62-SUMMARY-FAILED] component=outcome")
            return {}

    def _flatten_outcome_summary(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(summary, dict):
            return {}
        global_summary = summary.get("global")
        if isinstance(global_summary, dict):
            flattened = dict(global_summary)
            flattened.setdefault("total_duplicate_skipped", summary.get("total_duplicate_skipped", 0))
            return flattened
        return dict(summary)

    def _outcome_group_highlights(self, outcome_summary: Dict[str, Any]) -> Dict[str, Any]:
        groups = outcome_summary.get("groups") if isinstance(outcome_summary, dict) else None
        if not isinstance(groups, dict):
            return {}
        result: Dict[str, Any] = {}
        best_phase2 = self._select_group_by_avg_r(
            groups,
            prefix="phase2_type=",
            reverse=True,
            exclude_combined=True,
        )
        worst_phase2 = self._select_group_by_avg_r(
            groups,
            prefix="phase2_type=",
            reverse=False,
            exclude_combined=True,
        )
        best_frozen_reason = self._select_group_by_avg_r(
            groups,
            prefix="frozen_reason=",
            reverse=True,
            exclude_combined=True,
        )
        worst_frozen_reason = self._select_group_by_avg_r(
            groups,
            prefix="frozen_reason=",
            reverse=False,
            exclude_combined=True,
        )
        best_group = self._select_group_by_avg_r(groups, reverse=True)
        worst_group = self._select_group_by_avg_r(groups, reverse=False)

        if best_phase2:
            result["outcome_best_phase2_type"] = best_phase2
        if worst_phase2:
            result["outcome_worst_phase2_type"] = worst_phase2
        if best_frozen_reason:
            result["outcome_best_frozen_reason"] = best_frozen_reason
        if worst_frozen_reason:
            result["outcome_worst_frozen_reason"] = worst_frozen_reason
        if best_group:
            result["outcome_best_group_by_avg_r"] = best_group
        if worst_group:
            result["outcome_worst_group_by_avg_r"] = worst_group
        return result

    def _select_group_by_avg_r(
        self,
        groups: Dict[str, Any],
        prefix: Optional[str] = None,
        reverse: bool = True,
        exclude_all: bool = True,
        exclude_combined: bool = False,
        require_min_sample_size: bool = True,
    ) -> str:
        candidates = self._group_candidates(
            groups=groups,
            prefix=prefix,
            exclude_all=exclude_all,
            exclude_combined=exclude_combined,
            require_min_sample_size=require_min_sample_size,
        )
        if not candidates and require_min_sample_size:
            candidates = self._group_candidates(
                groups=groups,
                prefix=prefix,
                exclude_all=exclude_all,
                exclude_combined=exclude_combined,
                require_min_sample_size=False,
            )
        if not candidates:
            return ""
        return sorted(candidates, key=lambda item: item[1], reverse=reverse)[0][0]

    def _group_candidates(
        self,
        groups: Dict[str, Any],
        prefix: Optional[str],
        exclude_all: bool,
        exclude_combined: bool,
        require_min_sample_size: bool,
    ) -> List[Any]:
        candidates = []
        for key, group in groups.items():
            key_text = str(key)
            if not isinstance(group, dict):
                continue
            if exclude_all and key_text == "ALL":
                continue
            if prefix is not None and not key_text.startswith(prefix):
                continue
            if exclude_combined and "|" in key_text:
                continue
            if require_min_sample_size and group.get("sample_size_too_small"):
                continue
            candidates.append((key_text, self._safe_float(group.get("avg_realized_r"), 0.0)))
        return candidates

    def _safe_collect(self, fn: Any) -> Dict[str, Any]:
        try:
            result = fn()
            return result if isinstance(result, dict) else {}
        except Exception:
            logger.exception("[V62-SUMMARY-FAILED]")
            return {}

    @staticmethod
    def _first_attr(obj: Any, names: Any, default: Any = None) -> Any:
        for name in names:
            if hasattr(obj, name):
                return getattr(obj, name)
        return default

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _format_kv(data: Dict[str, Any]) -> str:
        parts = []
        for key, value in data.items():
            if isinstance(value, float):
                parts.append(f"{key}={value:.6f}")
            else:
                parts.append(f"{key}={value}")
        return " ".join(parts)
