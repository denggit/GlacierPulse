#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""V6.2 research-only virtual position manager with conservative stop handling."""

import logging
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any, Deque, Dict, List, Optional

from config import research_evaluator as cfg
from src.research.a1_frozen_metadata import (
    A1_COUNT_METADATA_FIELDS,
    A1_SCORE_METADATA_FIELDS,
    A1_STRING_METADATA_FIELDS,
)
from src.utils.log_noise import suppressed_log_counter

logger = logging.getLogger(__name__)


@dataclass
class ResearchVirtualPosition:
    position_id: str
    zone_id: str
    direction: str
    phase2_type: str
    candidate_type: str
    status: str
    open_ts: float
    open_price: float
    current_price: float
    suggested_stop: float
    initial_stop: float
    dynamic_stop: float
    best_price: float
    take_profit_price: float
    close_ts: Optional[float] = None
    close_price: Optional[float] = None
    close_reason: Optional[str] = None
    breakeven_activated: bool = False
    breakeven_ts: float = 0.0
    trailing_activated: bool = False
    trailing_ts: float = 0.0
    stop_update_count: int = 0
    support_update_count: int = 0
    support_zone_ids: List[str] = field(default_factory=list)
    last_stop_update_reason: str = ""
    last_stop_update_ts: float = 0.0
    last_support_update_ts: float = 0.0
    exit_stop_used: float = 0.0
    risk_distance_u: float = 0.0
    risk_distance_pct: float = 0.0
    total_loss_pct: float = 0.0
    leverage: float = 1.0
    final_margin_usage_pct: float = 0.0
    notional_equity_multiple: float = 0.0
    expected_account_loss_if_sl: float = 0.0
    virtual_equity_at_open: float = 0.0
    virtual_margin_usdt: float = 0.0
    virtual_notional_usdt: float = 0.0
    virtual_size_eth: float = 0.0
    unrealized_pnl_u: float = 0.0
    unrealized_pnl_pct_on_equity: float = 0.0
    unrealized_r_multiple: float = 0.0
    realized_pnl_u: float = 0.0
    realized_pnl_pct_on_equity: float = 0.0
    realized_r_multiple: float = 0.0
    max_favorable_u: float = 0.0
    max_adverse_u: float = 0.0
    max_favorable_r: float = 0.0
    max_adverse_r: float = 0.0
    phase2_total_score: float = 0.0
    absorption_score: float = 0.0
    pressure_decay_score: float = 0.0
    reclaim_score: float = 0.0
    retest_score: float = 0.0
    book_absorption_score: float = 0.0
    relevant_book_depth_available: bool = False
    reload_score: float = 0.0
    has_swept_boundary: bool = False
    has_absorbed_after_sweep: bool = False
    has_reclaimed_boundary: bool = False
    has_retested_inside_zone: bool = False
    frozen_reason: str = ""
    frozen_state: str = ""
    frozen_event_id: str = ""
    event_count: int = 0
    iceberg_count: int = 0
    ignore_count: int = 0
    spoof_count: int = 0
    cancel_count: int = 0
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0
    positive_score: float = 0.0
    negative_score: float = 0.0
    net_score: float = 0.0


class ResearchVirtualPositionManager:
    def __init__(self):
        self.active_position: Optional[ResearchVirtualPosition] = None
        self.closed_positions: Deque[Dict[str, Any]] = deque(maxlen=max(1, int(cfg.VIRTUAL_MAX_CLOSED_POSITIONS)))
        self.closed_events: Deque[Dict[str, Any]] = deque(maxlen=max(1, int(cfg.PHASE3_OUTCOME_MAX_CLOSED_POSITIONS)))
        self.virtual_equity_usdt: float = float(cfg.VIRTUAL_INITIAL_EQUITY_USDT)
        self.total_opened = 0
        self.total_closed = 0
        self.total_rejected = 0
        self.total_skipped = 0
        self.cumulative_realized_pnl_u = 0.0
        self.cumulative_win_count = 0
        self.cumulative_loss_count = 0
        self.cumulative_realized_r_sum = 0.0
        self.cumulative_realized_r_count = 0
        self.cumulative_max_win_r = 0.0
        self.cumulative_max_loss_r = 0.0
        self.last_update_log_ts = 0.0

    def on_candidate(self, candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            return self._on_candidate(candidate)
        except Exception:
            logger.exception("[VIRTUAL-POSITION-CANDIDATE-FAILED] candidate=%s", candidate)
            return None

    def _on_candidate(self, candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(candidate, dict):
            return None
        if self.active_position is not None:
            if candidate.get("decision") != "ACCEPT_RESEARCH_CANDIDATE":
                return None
            candidate_direction = self._candidate_direction(candidate)
            if candidate_direction == self.active_position.direction:
                return self._apply_support_update(candidate)
            self.total_skipped += 1
            self._log_virtual_skip(
                zone_id=candidate.get("zone_id"),
                direction=candidate.get("direction"),
                candidate_type=candidate.get("candidate_type"),
                reason="opposite_direction_active_position_exists",
                active_position_id=self.active_position.position_id,
            )
            return None
        if candidate.get("decision") != "ACCEPT_RESEARCH_CANDIDATE":
            return None

        direction = self._candidate_direction(candidate)
        open_price = self._as_float(candidate.get("candidate_price", candidate.get("last_price")))
        suggested_stop = self._as_float(candidate.get("suggested_stop"))
        risk_distance_u = self._as_float(candidate.get("risk_distance_u", candidate.get("risk_to_stop_u")))
        final_margin_usage_pct = self._as_float(candidate.get("final_margin_usage_pct"))
        leverage = self._as_float(candidate.get("leverage"), 1.0)
        if not direction or open_price <= 0 or suggested_stop <= 0 or risk_distance_u <= 0 or final_margin_usage_pct <= 0:
            self.total_rejected += 1
            self._log_virtual_skip(
                zone_id=candidate.get("zone_id"),
                direction=candidate.get("direction"),
                candidate_type=candidate.get("candidate_type"),
                reason="invalid_candidate_fields",
                active_position_id=None,
            )
            return None
        if (direction == "LONG" and open_price <= suggested_stop) or (direction == "SHORT" and open_price >= suggested_stop):
            self.total_rejected += 1
            self._log_virtual_skip(
                zone_id=candidate.get("zone_id"),
                direction=candidate.get("direction"),
                candidate_type=candidate.get("candidate_type"),
                reason="invalid_stop_direction",
                active_position_id=None,
            )
            return None

        virtual_margin_usdt = self.virtual_equity_usdt * final_margin_usage_pct
        virtual_notional_usdt = virtual_margin_usdt * leverage
        virtual_size_eth = virtual_notional_usdt / open_price
        take_profit_price = open_price + risk_distance_u * cfg.VIRTUAL_TAKE_PROFIT_R_MULTIPLE if direction == "LONG" else open_price - risk_distance_u * cfg.VIRTUAL_TAKE_PROFIT_R_MULTIPLE

        candidate_ts = self._as_float(candidate.get("candidate_ts", candidate.get("ts")), time.time())
        a1_metadata = self._a1_metadata_from_candidate(candidate)

        self.total_opened += 1
        position = ResearchVirtualPosition(
            position_id=f"vp-{self.total_opened}", zone_id=str(candidate.get("zone_id", "")), direction=direction,
            phase2_type=str(candidate.get("phase2_type", "")), candidate_type=str(candidate.get("candidate_type", "")), status="OPEN",
            open_ts=candidate_ts, open_price=open_price, current_price=open_price,
            suggested_stop=suggested_stop, initial_stop=suggested_stop, dynamic_stop=suggested_stop,
            best_price=open_price, take_profit_price=take_profit_price, risk_distance_u=risk_distance_u,
            risk_distance_pct=self._as_float(candidate.get("risk_distance_pct", candidate.get("risk_to_stop_pct"))),
            total_loss_pct=self._as_float(candidate.get("total_loss_pct")), leverage=leverage,
            final_margin_usage_pct=final_margin_usage_pct, notional_equity_multiple=final_margin_usage_pct * leverage,
            expected_account_loss_if_sl=self._as_float(candidate.get("expected_account_loss_if_sl")),
            virtual_equity_at_open=self.virtual_equity_usdt, virtual_margin_usdt=virtual_margin_usdt,
            virtual_notional_usdt=virtual_notional_usdt, virtual_size_eth=virtual_size_eth,
            phase2_total_score=self._as_float(candidate.get("phase2_total_score")), absorption_score=self._as_float(candidate.get("absorption_score")),
            pressure_decay_score=self._as_float(candidate.get("pressure_decay_score")), reclaim_score=self._as_float(candidate.get("reclaim_score")),
            retest_score=self._as_float(candidate.get("retest_score")), book_absorption_score=self._as_float(candidate.get("book_absorption_score")),
            relevant_book_depth_available=bool(candidate.get("relevant_book_depth_available", False)), reload_score=self._as_float(candidate.get("reload_score")),
            has_swept_boundary=bool(candidate.get("has_swept_boundary", False)), has_absorbed_after_sweep=bool(candidate.get("has_absorbed_after_sweep", False)),
            has_reclaimed_boundary=bool(candidate.get("has_reclaimed_boundary", False)), has_retested_inside_zone=bool(candidate.get("has_retested_inside_zone", False)),
            **a1_metadata,
        )
        self.active_position = position
        if bool(getattr(cfg, "V62_LOG_VIRTUAL_POSITION_OPEN_ENABLED", True)):
            logger.info("[VIRTUAL-POSITION-OPEN] position_id=%s zone_id=%s direction=%s phase2_type=%s candidate_type=%s open_ts=%.3f open_price=%.6f suggested_stop=%.6f initial_stop=%.6f dynamic_stop=%.6f best_price=%.6f breakeven_activated=%s trailing_activated=%s take_profit_price=%.6f risk_distance_u=%.6f risk_distance_pct=%.6f leverage=%.6f final_margin_usage_pct=%.6f virtual_equity_at_open=%.6f virtual_margin_usdt=%.6f virtual_notional_usdt=%.6f virtual_size_eth=%.12f expected_account_loss_if_sl=%.6f phase2_total_score=%.6f absorption_score=%.6f relevant_book_depth_available=%s reload_score=%.6f frozen_reason=%s frozen_state=%s iceberg_count=%d high_count=%d net_score=%.6f", position.position_id, position.zone_id, position.direction, position.phase2_type, position.candidate_type, position.open_ts, position.open_price, position.suggested_stop, position.initial_stop, position.dynamic_stop, position.best_price, position.breakeven_activated, position.trailing_activated, position.take_profit_price, position.risk_distance_u, position.risk_distance_pct, position.leverage, position.final_margin_usage_pct, position.virtual_equity_at_open, position.virtual_margin_usdt, position.virtual_notional_usdt, position.virtual_size_eth, position.expected_account_loss_if_sl, position.phase2_total_score, position.absorption_score, position.relevant_book_depth_available, position.reload_score, position.frozen_reason, position.frozen_state, position.iceberg_count, position.high_count, position.net_score)
        return asdict(position)

    def on_price(self, price: float, ts: Optional[float] = None) -> Optional[Dict[str, Any]]:
        try:
            return self._on_price(price, ts)
        except Exception:
            logger.exception("[VIRTUAL-POSITION-PRICE-FAILED] price=%s ts=%s", price, ts)
            return None

    def _on_price(self, price: float, ts: Optional[float] = None) -> Optional[Dict[str, Any]]:
        pos = self.active_position
        if pos is None:
            return None
        now_ts = self._as_float(ts, time.time())
        old_dynamic_stop = pos.dynamic_stop
        pos.current_price = float(price)
        self._update_unrealized_stats(pos)

        if pos.direction == "LONG" and pos.current_price <= old_dynamic_stop:
            return self._close_position(pos.current_price, now_ts, "STOP_LOSS", exit_stop_used_override=old_dynamic_stop)
        if pos.direction == "SHORT" and pos.current_price >= old_dynamic_stop:
            return self._close_position(pos.current_price, now_ts, "STOP_LOSS", exit_stop_used_override=old_dynamic_stop)

        if pos.direction == "LONG":
            pos.best_price = max(pos.best_price, pos.current_price)
        else:
            pos.best_price = min(pos.best_price, pos.current_price)

        self._maybe_apply_breakeven(pos, now_ts)
        self._maybe_apply_trailing(pos, now_ts)

        close_reason = None
        if pos.direction == "LONG" and pos.current_price >= pos.take_profit_price:
            close_reason = "TAKE_PROFIT_R_MULTIPLE"
        elif pos.direction == "SHORT" and pos.current_price <= pos.take_profit_price:
            close_reason = "TAKE_PROFIT_R_MULTIPLE"
        if close_reason:
            return self._close_position(pos.current_price, now_ts, close_reason)

        if now_ts - self.last_update_log_ts >= max(0.0, float(cfg.VIRTUAL_UPDATE_LOG_MIN_INTERVAL_SEC)):
            self.last_update_log_ts = now_ts
            if bool(getattr(cfg, "V62_LOG_VIRTUAL_POSITION_UPDATE_ENABLED", True)):
                logger.info("[VIRTUAL-POSITION-UPDATE] position_id=%s zone_id=%s direction=%s ts=%.3f current_price=%.6f open_price=%.6f suggested_stop=%.6f initial_stop=%.6f dynamic_stop=%.6f best_price=%.6f breakeven_activated=%s trailing_activated=%s stop_update_count=%d support_update_count=%d take_profit_price=%.6f unrealized_pnl_u=%.6f unrealized_pnl_pct_on_equity=%.6f unrealized_r_multiple=%.6f max_favorable_u=%.6f max_adverse_u=%.6f max_favorable_r=%.6f max_adverse_r=%.6f virtual_equity_usdt=%.6f", pos.position_id, pos.zone_id, pos.direction, now_ts, pos.current_price, pos.open_price, pos.suggested_stop, pos.initial_stop, pos.dynamic_stop, pos.best_price, pos.breakeven_activated, pos.trailing_activated, pos.stop_update_count, pos.support_update_count, pos.take_profit_price, pos.unrealized_pnl_u, pos.unrealized_pnl_pct_on_equity, pos.unrealized_r_multiple, pos.max_favorable_u, pos.max_adverse_u, pos.max_favorable_r, pos.max_adverse_r, self.virtual_equity_usdt)
            else:
                suppressed_log_counter.inc("suppressed_virtual_update_count")
        return asdict(pos)

    def _update_unrealized_stats(self, pos: ResearchVirtualPosition) -> None:
        if pos.direction == "LONG":
            pos.unrealized_pnl_u = pos.virtual_size_eth * (pos.current_price - pos.open_price)
            pos.unrealized_r_multiple = (
                (pos.current_price - pos.open_price) / pos.risk_distance_u
                if pos.risk_distance_u
                else 0.0
            )
        else:
            pos.unrealized_pnl_u = pos.virtual_size_eth * (pos.open_price - pos.current_price)
            pos.unrealized_r_multiple = (
                (pos.open_price - pos.current_price) / pos.risk_distance_u
                if pos.risk_distance_u
                else 0.0
            )
        pos.unrealized_pnl_pct_on_equity = pos.unrealized_pnl_u / pos.virtual_equity_at_open if pos.virtual_equity_at_open else 0.0
        pos.max_favorable_u = max(pos.max_favorable_u, pos.unrealized_pnl_u)
        pos.max_adverse_u = min(pos.max_adverse_u, pos.unrealized_pnl_u)
        pos.max_favorable_r = max(pos.max_favorable_r, pos.unrealized_r_multiple)
        pos.max_adverse_r = min(pos.max_adverse_r, pos.unrealized_r_multiple)

    def _maybe_apply_breakeven(self, pos: ResearchVirtualPosition, now_ts: float) -> bool:
        if not bool(cfg.VIRTUAL_BREAKEVEN_ENABLED):
            return False
        if pos.breakeven_activated:
            return False
        if pos.unrealized_r_multiple < float(cfg.VIRTUAL_BREAKEVEN_TRIGGER_R):
            return False
        if pos.direction == "LONG":
            new_stop = pos.open_price + pos.risk_distance_u * float(cfg.VIRTUAL_BREAKEVEN_OFFSET_R)
        else:
            new_stop = pos.open_price - pos.risk_distance_u * float(cfg.VIRTUAL_BREAKEVEN_OFFSET_R)
        if not self._is_stop_improvement(pos, new_stop):
            return False

        old_stop = pos.dynamic_stop
        pos.dynamic_stop = new_stop
        pos.breakeven_activated = True
        pos.breakeven_ts = now_ts
        pos.stop_update_count += 1
        pos.last_stop_update_reason = "BREAKEVEN"
        pos.last_stop_update_ts = now_ts
        self._log_stop_update(pos, now_ts, "BREAKEVEN", old_stop, new_stop, pos.zone_id)
        return True

    def _maybe_apply_trailing(self, pos: ResearchVirtualPosition, now_ts: float) -> bool:
        if not bool(cfg.VIRTUAL_TRAILING_ENABLED):
            return False
        if pos.max_favorable_r < float(cfg.VIRTUAL_TRAILING_TRIGGER_R):
            return False
        if pos.direction == "LONG":
            new_stop = pos.best_price - pos.risk_distance_u * float(cfg.VIRTUAL_TRAILING_DISTANCE_R)
        else:
            new_stop = pos.best_price + pos.risk_distance_u * float(cfg.VIRTUAL_TRAILING_DISTANCE_R)
        if not self._is_stop_improvement(pos, new_stop):
            return False

        old_stop = pos.dynamic_stop
        pos.dynamic_stop = new_stop
        pos.trailing_activated = True
        if pos.trailing_ts <= 0:
            pos.trailing_ts = now_ts
        pos.stop_update_count += 1
        pos.last_stop_update_reason = "TRAILING"
        pos.last_stop_update_ts = now_ts
        self._log_stop_update(pos, now_ts, "TRAILING", old_stop, new_stop, pos.zone_id)
        return True

    def _apply_support_update(self, candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pos = self.active_position
        if pos is None:
            return None
        if not bool(cfg.VIRTUAL_SUPPORT_UPDATE_ENABLED):
            self.total_skipped += 1
            self._log_virtual_skip(
                zone_id=candidate.get("zone_id"),
                direction=candidate.get("direction"),
                candidate_type=candidate.get("candidate_type"),
                reason="support_update_disabled",
                active_position_id=pos.position_id,
            )
            return None
        if bool(cfg.VIRTUAL_SUPPORT_REQUIRE_SAME_DIRECTION) and self._candidate_direction(candidate) != pos.direction:
            self.total_skipped += 1
            self._log_virtual_skip(
                zone_id=candidate.get("zone_id"),
                direction=candidate.get("direction"),
                candidate_type=candidate.get("candidate_type"),
                reason="support_direction_mismatch",
                active_position_id=pos.position_id,
            )
            return None

        phase2_total_score = self._as_float(candidate.get("phase2_total_score"))
        if phase2_total_score < float(cfg.VIRTUAL_SUPPORT_MIN_PHASE2_SCORE):
            self.total_skipped += 1
            self._log_virtual_skip(
                zone_id=candidate.get("zone_id"),
                direction=candidate.get("direction"),
                candidate_type=candidate.get("candidate_type"),
                reason="support_phase2_score_too_low",
                active_position_id=pos.position_id,
                phase2_total_score=phase2_total_score,
                min_phase2_score=float(cfg.VIRTUAL_SUPPORT_MIN_PHASE2_SCORE),
            )
            return None

        candidate_ts = self._as_float(candidate.get("candidate_ts", candidate.get("ts")), time.time())
        support_zone_id = str(candidate.get("zone_id", ""))
        pos.support_update_count += 1
        pos.last_support_update_ts = candidate_ts
        if support_zone_id:
            pos.support_zone_ids.append(support_zone_id)
            max_zone_ids = max(0, int(cfg.VIRTUAL_SUPPORT_MAX_ZONE_IDS))
            if max_zone_ids == 0:
                pos.support_zone_ids = []
            elif len(pos.support_zone_ids) > max_zone_ids:
                pos.support_zone_ids = pos.support_zone_ids[-max_zone_ids:]

        candidate_stop = self._as_float(candidate.get("suggested_stop"))
        stop_improved = False
        if candidate_stop > 0 and self._is_stop_improvement(pos, candidate_stop):
            if (pos.direction == "LONG" and candidate_stop < pos.current_price) or (pos.direction == "SHORT" and candidate_stop > pos.current_price):
                old_stop = pos.dynamic_stop
                pos.dynamic_stop = candidate_stop
                pos.stop_update_count += 1
                pos.last_stop_update_reason = "SUPPORT_CANDIDATE"
                pos.last_stop_update_ts = candidate_ts
                stop_improved = True
                self._log_stop_update(pos, candidate_ts, "SUPPORT_CANDIDATE", old_stop, candidate_stop, support_zone_id)

        if bool(getattr(cfg, "V62_LOG_VIRTUAL_SUPPORT_UPDATE_ENABLED", True)):
            logger.info("[VIRTUAL-SUPPORT-UPDATE] position_id=%s zone_id=%s support_zone_id=%s direction=%s candidate_type=%s phase2_type=%s candidate_ts=%.3f candidate_price=%.6f candidate_stop=%.6f phase2_total_score=%.6f support_update_count=%d support_zone_ids_count=%d dynamic_stop=%.6f stop_improved=%s", pos.position_id, pos.zone_id, support_zone_id, pos.direction, candidate.get("candidate_type"), candidate.get("phase2_type"), candidate_ts, self._as_float(candidate.get("candidate_price", candidate.get("last_price"))), candidate_stop, phase2_total_score, pos.support_update_count, len(pos.support_zone_ids), pos.dynamic_stop, stop_improved)
        return asdict(pos)

    def _is_stop_improvement(self, pos: ResearchVirtualPosition, new_stop: float) -> bool:
        min_improvement = max(0.0, float(cfg.VIRTUAL_STOP_UPDATE_MIN_IMPROVEMENT_U))
        if pos.direction == "LONG":
            return new_stop > pos.dynamic_stop + min_improvement
        return new_stop < pos.dynamic_stop - min_improvement

    def _log_virtual_skip(
        self,
        zone_id: Any,
        direction: Any,
        candidate_type: Any,
        reason: str,
        active_position_id: Any = None,
        **extra: Any,
    ) -> None:
        if not bool(getattr(cfg, "V62_LOG_VIRTUAL_POSITION_SKIP_ENABLED", True)):
            suppressed_log_counter.inc("suppressed_virtual_skip_count")
            return
        extra_text = " ".join(f"{key}={value}" for key, value in extra.items())
        message = (
            "[VIRTUAL-POSITION-SKIP] zone_id=%s direction=%s candidate_type=%s "
            "reason=%s active_position_id=%s"
        )
        if extra_text:
            message = f"{message} {extra_text}"
        logger.info(message, zone_id, direction, candidate_type, reason, active_position_id)

    def _log_stop_update(self, pos: ResearchVirtualPosition, now_ts: float, reason: str, old_stop: float, new_stop: float, zone_id: str) -> None:
        if not bool(getattr(cfg, "V62_LOG_VIRTUAL_STOP_UPDATE_ENABLED", True)):
            return
        logger.info("[VIRTUAL-STOP-UPDATE] position_id=%s zone_id=%s direction=%s ts=%.3f reason=%s old_stop=%.6f new_stop=%.6f initial_stop=%.6f dynamic_stop=%.6f open_price=%.6f current_price=%.6f best_price=%.6f unrealized_r_multiple=%.6f max_favorable_r=%.6f stop_update_count=%d breakeven_activated=%s trailing_activated=%s", pos.position_id, zone_id, pos.direction, now_ts, reason, old_stop, new_stop, pos.initial_stop, pos.dynamic_stop, pos.open_price, pos.current_price, pos.best_price, pos.unrealized_r_multiple, pos.max_favorable_r, pos.stop_update_count, pos.breakeven_activated, pos.trailing_activated)

    def _close_position(
        self,
        close_price: float,
        close_ts: float,
        close_reason: str,
        exit_stop_used_override: Optional[float] = None,
    ) -> Dict[str, Any]:
        pos = self.active_position
        if pos is None:
            return {}
        before = self.virtual_equity_usdt
        pos.close_price = close_price
        pos.close_ts = close_ts
        pos.close_reason = close_reason if close_reason in {"STOP_LOSS", "TAKE_PROFIT_R_MULTIPLE", "MANUAL_RESET", "REPLACED", "UNKNOWN"} else "UNKNOWN"
        pos.status = "CLOSED"
        if pos.close_reason == "STOP_LOSS" and exit_stop_used_override is not None:
            pos.exit_stop_used = float(exit_stop_used_override)
        else:
            pos.exit_stop_used = pos.dynamic_stop
        if pos.direction == "LONG":
            pos.realized_pnl_u = pos.virtual_size_eth * (pos.close_price - pos.open_price)
            pos.realized_r_multiple = (pos.close_price - pos.open_price) / pos.risk_distance_u
        else:
            pos.realized_pnl_u = pos.virtual_size_eth * (pos.open_price - pos.close_price)
            pos.realized_r_multiple = (pos.open_price - pos.close_price) / pos.risk_distance_u
        pos.realized_pnl_pct_on_equity = pos.realized_pnl_u / pos.virtual_equity_at_open if pos.virtual_equity_at_open else 0.0
        self.virtual_equity_usdt += pos.realized_pnl_u
        snapshot = asdict(pos)
        self.closed_positions.append(snapshot)
        self.closed_events.append(snapshot)
        self.active_position = None
        self.total_closed += 1
        self.cumulative_realized_pnl_u += pos.realized_pnl_u
        if pos.realized_pnl_u > 0:
            self.cumulative_win_count += 1
        elif pos.realized_pnl_u < 0:
            self.cumulative_loss_count += 1
        self.cumulative_realized_r_sum += pos.realized_r_multiple
        self.cumulative_realized_r_count += 1
        self.cumulative_max_win_r = max(self.cumulative_max_win_r, pos.realized_r_multiple)
        self.cumulative_max_loss_r = min(self.cumulative_max_loss_r, pos.realized_r_multiple)
        if bool(getattr(cfg, "V62_LOG_VIRTUAL_POSITION_CLOSE_ENABLED", True)):
            logger.info("[VIRTUAL-POSITION-CLOSE] position_id=%s zone_id=%s direction=%s phase2_type=%s candidate_type=%s open_ts=%.3f close_ts=%.3f open_price=%.6f close_price=%.6f close_reason=%s suggested_stop=%.6f initial_stop=%.6f dynamic_stop=%.6f exit_stop_used=%.6f breakeven_activated=%s trailing_activated=%s stop_update_count=%d support_update_count=%d last_stop_update_reason=%s take_profit_price=%.6f realized_pnl_u=%.6f realized_pnl_pct_on_equity=%.6f realized_r_multiple=%.6f max_favorable_u=%.6f max_adverse_u=%.6f max_favorable_r=%.6f max_adverse_r=%.6f virtual_equity_before=%.6f virtual_equity_after=%.6f virtual_margin_usdt=%.6f virtual_notional_usdt=%.6f virtual_size_eth=%.12f frozen_reason=%s frozen_state=%s iceberg_count=%d high_count=%d net_score=%.6f", pos.position_id, pos.zone_id, pos.direction, pos.phase2_type, pos.candidate_type, pos.open_ts, pos.close_ts, pos.open_price, pos.close_price, pos.close_reason, pos.suggested_stop, pos.initial_stop, pos.dynamic_stop, pos.exit_stop_used, pos.breakeven_activated, pos.trailing_activated, pos.stop_update_count, pos.support_update_count, pos.last_stop_update_reason, pos.take_profit_price, pos.realized_pnl_u, pos.realized_pnl_pct_on_equity, pos.realized_r_multiple, pos.max_favorable_u, pos.max_adverse_u, pos.max_favorable_r, pos.max_adverse_r, before, self.virtual_equity_usdt, pos.virtual_margin_usdt, pos.virtual_notional_usdt, pos.virtual_size_eth, pos.frozen_reason, pos.frozen_state, pos.iceberg_count, pos.high_count, pos.net_score)
        return snapshot

    def get_active_position(self) -> Optional[Dict[str, Any]]:
        return asdict(self.active_position) if self.active_position else None

    def get_closed_positions(self) -> List[Dict[str, Any]]:
        return list(self.closed_positions)

    def pop_closed_events(self) -> List[Dict[str, Any]]:
        events = list(self.closed_events)
        self.closed_events.clear()
        return events

    def summary(self) -> Dict[str, Any]:
        closed = list(self.closed_positions)
        closed_total = self.cumulative_win_count + self.cumulative_loss_count
        avg_realized_r = (self.cumulative_realized_r_sum / self.cumulative_realized_r_count) if self.cumulative_realized_r_count > 0 else 0.0
        max_win_r = self.cumulative_max_win_r if self.total_closed > 0 else 0.0
        max_loss_r = self.cumulative_max_loss_r if self.total_closed > 0 else 0.0
        active = self.active_position
        return {
            "virtual_equity_usdt": self.virtual_equity_usdt,
            "total_opened": self.total_opened,
            "total_closed": self.total_closed,
            "total_rejected": self.total_rejected,
            "total_skipped": self.total_skipped,
            "active_position_exists": active is not None,
            "active_dynamic_stop": active.dynamic_stop if active else 0.0,
            "active_initial_stop": active.initial_stop if active else 0.0,
            "active_best_price": active.best_price if active else 0.0,
            "active_breakeven_activated": active.breakeven_activated if active else False,
            "active_trailing_activated": active.trailing_activated if active else False,
            "active_stop_update_count": active.stop_update_count if active else 0,
            "active_support_update_count": active.support_update_count if active else 0,
            "closed_positions_count": len(closed),
            "closed_positions_maxlen": self.closed_positions.maxlen,
            "cumulative_realized_pnl_u": self.cumulative_realized_pnl_u,
            "total_realized_pnl_u": self.cumulative_realized_pnl_u,
            "win_count": self.cumulative_win_count,
            "loss_count": self.cumulative_loss_count,
            "win_rate": (self.cumulative_win_count / closed_total) if closed_total > 0 else 0.0,
            "avg_realized_r": avg_realized_r,
            "max_win_r": max_win_r,
            "max_loss_r": max_loss_r,
        }

    @staticmethod
    def _candidate_direction(candidate: Dict[str, Any]) -> str:
        raw_direction = str(candidate.get("direction", "")).upper()
        if raw_direction == "BUY":
            return "LONG"
        if raw_direction == "SELL":
            return "SHORT"
        return ""

    def _a1_metadata_from_candidate(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {}
        for field in A1_STRING_METADATA_FIELDS:
            metadata[field] = self._as_str(candidate.get(field), "")
        for field in A1_COUNT_METADATA_FIELDS:
            metadata[field] = self._as_int(candidate.get(field), 0)
        for field in A1_SCORE_METADATA_FIELDS:
            metadata[field] = self._as_float(candidate.get(field), 0.0)
        return metadata

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
    def _as_str(value: Any, default: str = "") -> str:
        if value is None:
            return default
        return str(value)
