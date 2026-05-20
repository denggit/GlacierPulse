#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""V6.2.6 research-only virtual position manager."""

import logging
import time
from collections import deque
from dataclasses import asdict, dataclass
from typing import Any, Deque, Dict, List, Optional

from config import research_evaluator as cfg

logger = logging.getLogger(__name__)


@dataclass
class VirtualPosition:
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
    take_profit_price: float
    close_ts: Optional[float] = None
    close_price: Optional[float] = None
    close_reason: Optional[str] = None
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


class VirtualPositionManager:
    def __init__(self):
        self.active_position: Optional[VirtualPosition] = None
        self.closed_positions: Deque[Dict[str, Any]] = deque(maxlen=max(1, int(cfg.VIRTUAL_MAX_CLOSED_POSITIONS)))
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
        if not isinstance(candidate, dict):
            return None
        if candidate.get("decision") != "ACCEPT_RESEARCH_CANDIDATE":
            return None
        if self.active_position is not None:
            self.total_skipped += 1
            logger.info("[VIRTUAL-POSITION-SKIP] zone_id=%s direction=%s candidate_type=%s reason=active_position_exists active_position_id=%s", candidate.get("zone_id"), candidate.get("direction"), candidate.get("candidate_type"), self.active_position.position_id)
            return None

        raw_direction = str(candidate.get("direction", "")).upper()
        direction = "LONG" if raw_direction == "BUY" else "SHORT" if raw_direction == "SELL" else ""
        open_price = self._as_float(candidate.get("candidate_price", candidate.get("last_price")))
        suggested_stop = self._as_float(candidate.get("suggested_stop"))
        risk_distance_u = self._as_float(candidate.get("risk_distance_u", candidate.get("risk_to_stop_u")))
        final_margin_usage_pct = self._as_float(candidate.get("final_margin_usage_pct"))
        leverage = self._as_float(candidate.get("leverage"), 1.0)
        if not direction or open_price <= 0 or suggested_stop <= 0 or risk_distance_u <= 0 or final_margin_usage_pct <= 0:
            self.total_rejected += 1
            logger.info("[VIRTUAL-POSITION-SKIP] zone_id=%s direction=%s candidate_type=%s reason=invalid_candidate_fields active_position_id=%s", candidate.get("zone_id"), candidate.get("direction"), candidate.get("candidate_type"), None)
            return None
        if (direction == "LONG" and open_price <= suggested_stop) or (direction == "SHORT" and open_price >= suggested_stop):
            self.total_rejected += 1
            logger.info("[VIRTUAL-POSITION-SKIP] zone_id=%s direction=%s candidate_type=%s reason=invalid_stop_direction active_position_id=%s", candidate.get("zone_id"), candidate.get("direction"), candidate.get("candidate_type"), None)
            return None

        virtual_margin_usdt = self.virtual_equity_usdt * final_margin_usage_pct
        virtual_notional_usdt = virtual_margin_usdt * leverage
        virtual_size_eth = virtual_notional_usdt / open_price
        take_profit_price = open_price + risk_distance_u * cfg.VIRTUAL_TAKE_PROFIT_R_MULTIPLE if direction == "LONG" else open_price - risk_distance_u * cfg.VIRTUAL_TAKE_PROFIT_R_MULTIPLE

        candidate_ts = self._as_float(candidate.get("candidate_ts", candidate.get("ts")), time.time())

        self.total_opened += 1
        position = VirtualPosition(
            position_id=f"vp-{self.total_opened}", zone_id=str(candidate.get("zone_id", "")), direction=direction,
            phase2_type=str(candidate.get("phase2_type", "")), candidate_type=str(candidate.get("candidate_type", "")), status="OPEN",
            open_ts=candidate_ts, open_price=open_price, current_price=open_price,
            suggested_stop=suggested_stop, take_profit_price=take_profit_price, risk_distance_u=risk_distance_u,
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
        )
        self.active_position = position
        logger.info("[VIRTUAL-POSITION-OPEN] position_id=%s zone_id=%s direction=%s phase2_type=%s candidate_type=%s open_ts=%.3f open_price=%.6f suggested_stop=%.6f take_profit_price=%.6f risk_distance_u=%.6f risk_distance_pct=%.6f leverage=%.6f final_margin_usage_pct=%.6f virtual_equity_at_open=%.6f virtual_margin_usdt=%.6f virtual_notional_usdt=%.6f virtual_size_eth=%.12f expected_account_loss_if_sl=%.6f phase2_total_score=%.6f absorption_score=%.6f relevant_book_depth_available=%s reload_score=%.6f", position.position_id, position.zone_id, position.direction, position.phase2_type, position.candidate_type, position.open_ts, position.open_price, position.suggested_stop, position.take_profit_price, position.risk_distance_u, position.risk_distance_pct, position.leverage, position.final_margin_usage_pct, position.virtual_equity_at_open, position.virtual_margin_usdt, position.virtual_notional_usdt, position.virtual_size_eth, position.expected_account_loss_if_sl, position.phase2_total_score, position.absorption_score, position.relevant_book_depth_available, position.reload_score)
        return asdict(position)

    def on_price(self, price: float, ts: Optional[float] = None) -> Optional[Dict[str, Any]]:
        pos = self.active_position
        if pos is None:
            return None
        now_ts = self._as_float(ts, time.time())
        pos.current_price = float(price)
        if pos.direction == "LONG":
            pos.unrealized_pnl_u = pos.virtual_size_eth * (pos.current_price - pos.open_price)
            pos.unrealized_r_multiple = (pos.current_price - pos.open_price) / pos.risk_distance_u
        else:
            pos.unrealized_pnl_u = pos.virtual_size_eth * (pos.open_price - pos.current_price)
            pos.unrealized_r_multiple = (pos.open_price - pos.current_price) / pos.risk_distance_u
        pos.unrealized_pnl_pct_on_equity = pos.unrealized_pnl_u / pos.virtual_equity_at_open if pos.virtual_equity_at_open else 0.0
        pos.max_favorable_u = max(pos.max_favorable_u, pos.unrealized_pnl_u)
        pos.max_adverse_u = min(pos.max_adverse_u, pos.unrealized_pnl_u)
        pos.max_favorable_r = max(pos.max_favorable_r, pos.unrealized_r_multiple)
        pos.max_adverse_r = min(pos.max_adverse_r, pos.unrealized_r_multiple)

        close_reason = None
        if pos.direction == "LONG" and pos.current_price <= pos.suggested_stop:
            close_reason = "STOP_LOSS"
        elif pos.direction == "SHORT" and pos.current_price >= pos.suggested_stop:
            close_reason = "STOP_LOSS"
        elif pos.direction == "LONG" and pos.current_price >= pos.take_profit_price:
            close_reason = "TAKE_PROFIT_R_MULTIPLE"
        elif pos.direction == "SHORT" and pos.current_price <= pos.take_profit_price:
            close_reason = "TAKE_PROFIT_R_MULTIPLE"
        if close_reason:
            return self._close_position(pos.current_price, now_ts, close_reason)

        if now_ts - self.last_update_log_ts >= max(0.0, float(cfg.VIRTUAL_UPDATE_LOG_MIN_INTERVAL_SEC)):
            self.last_update_log_ts = now_ts
            logger.info("[VIRTUAL-POSITION-UPDATE] position_id=%s zone_id=%s direction=%s ts=%.3f current_price=%.6f open_price=%.6f suggested_stop=%.6f take_profit_price=%.6f unrealized_pnl_u=%.6f unrealized_pnl_pct_on_equity=%.6f unrealized_r_multiple=%.6f max_favorable_u=%.6f max_adverse_u=%.6f max_favorable_r=%.6f max_adverse_r=%.6f virtual_equity_usdt=%.6f", pos.position_id, pos.zone_id, pos.direction, now_ts, pos.current_price, pos.open_price, pos.suggested_stop, pos.take_profit_price, pos.unrealized_pnl_u, pos.unrealized_pnl_pct_on_equity, pos.unrealized_r_multiple, pos.max_favorable_u, pos.max_adverse_u, pos.max_favorable_r, pos.max_adverse_r, self.virtual_equity_usdt)
        return asdict(pos)

    def _close_position(self, close_price: float, close_ts: float, close_reason: str) -> Dict[str, Any]:
        pos = self.active_position
        if pos is None:
            return {}
        before = self.virtual_equity_usdt
        pos.close_price = close_price
        pos.close_ts = close_ts
        pos.close_reason = close_reason if close_reason in {"STOP_LOSS", "TAKE_PROFIT_R_MULTIPLE", "MANUAL_RESET", "REPLACED", "UNKNOWN"} else "UNKNOWN"
        pos.status = "CLOSED"
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
        logger.info("[VIRTUAL-POSITION-CLOSE] position_id=%s zone_id=%s direction=%s phase2_type=%s candidate_type=%s open_ts=%.3f close_ts=%.3f open_price=%.6f close_price=%.6f close_reason=%s suggested_stop=%.6f take_profit_price=%.6f realized_pnl_u=%.6f realized_pnl_pct_on_equity=%.6f realized_r_multiple=%.6f max_favorable_u=%.6f max_adverse_u=%.6f max_favorable_r=%.6f max_adverse_r=%.6f virtual_equity_before=%.6f virtual_equity_after=%.6f virtual_margin_usdt=%.6f virtual_notional_usdt=%.6f virtual_size_eth=%.12f", pos.position_id, pos.zone_id, pos.direction, pos.phase2_type, pos.candidate_type, pos.open_ts, pos.close_ts, pos.open_price, pos.close_price, pos.close_reason, pos.suggested_stop, pos.take_profit_price, pos.realized_pnl_u, pos.realized_pnl_pct_on_equity, pos.realized_r_multiple, pos.max_favorable_u, pos.max_adverse_u, pos.max_favorable_r, pos.max_adverse_r, before, self.virtual_equity_usdt, pos.virtual_margin_usdt, pos.virtual_notional_usdt, pos.virtual_size_eth)
        return snapshot

    def get_active_position(self) -> Optional[Dict[str, Any]]:
        return asdict(self.active_position) if self.active_position else None

    def get_closed_positions(self) -> List[Dict[str, Any]]:
        return list(self.closed_positions)

    def summary(self) -> Dict[str, Any]:
        closed = list(self.closed_positions)
        closed_total = self.cumulative_win_count + self.cumulative_loss_count
        avg_realized_r = (self.cumulative_realized_r_sum / self.cumulative_realized_r_count) if self.cumulative_realized_r_count > 0 else 0.0
        max_win_r = self.cumulative_max_win_r if self.total_closed > 0 else 0.0
        max_loss_r = self.cumulative_max_loss_r if self.total_closed > 0 else 0.0
        return {
            "virtual_equity_usdt": self.virtual_equity_usdt,
            "total_opened": self.total_opened,
            "total_closed": self.total_closed,
            "total_rejected": self.total_rejected,
            "total_skipped": self.total_skipped,
            "active_position_exists": self.active_position is not None,
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
    def _as_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)
