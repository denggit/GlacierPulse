#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 4/28/2026
@File       : phase1_zone_engine.py
@Description: 机构级扫损与冰山点火引擎 (V3 Pending Event Manager)
"""

import collections
import logging
import time
from typing import Dict, Any, Optional

from config import research_evaluator as cfg
from config.research_evaluator import (
    PHASE2_ORDERFLOW_EVALUATOR_ENABLED,
    PHASE3_CANDIDATE_EVALUATOR_ENABLED,
    PHASE3_OUTCOME_EVALUATOR_ENABLED,
    VIRTUAL_POSITION_MANAGER_ENABLED,
    REAL_EXECUTION_ENABLED,
    VIRTUAL_SHADOW_MODE,
    V62_INTEGRATION_HEARTBEAT_ENABLED,
    V62_LOG_COMPONENT_STATUS_ON_START,
    V62_LOG_CONFIG_SNAPSHOT_ON_START,
    V62_SHADOW_RUN_LABEL,
    V62_STARTUP_SAFETY_CHECK_ENABLED,
)
from src.strategy.iceberg.zone_tracker import IcebergZoneTracker
from src.strategy.iceberg.outcome_evaluator import IcebergOutcomeEvaluator
from src.strategy.a1_reaction.reaction_evaluator import A1ReactionEvaluator
from src.strategy.execution_research.candidate_evaluator import (
    ExecutionResearchCandidateEvaluator,
)
from src.strategy.phase3_trade_outcome_evaluator import Phase3OutcomeEvaluator
from src.monitoring.research_runtime_monitor import ResearchRuntimeMonitor
from src.strategy.virtual_position_manager import VirtualPositionManager
from src.utils.log_noise import suppressed_log_counter

logger = logging.getLogger(__name__)


class Phase1Engine:
    def __init__(self, market_context, iceberg_detector):
        self.ctx = market_context
        self.iceberg_radar = iceberg_detector

        self.min_event_start_notional_usdt = 300_000
        self.min_event_merge_notional_usdt = 20_000
        self.accumulate_window_ms = 100
        self.merge_price_tolerance = 0.5
        self.local_zone_width = 1.5
        self.min_local_depth_usdt = 300_000
        self.max_pending_events = 100
        self.min_book_updates_after_cutoff = 2
        self.max_wait_ms = 700
        self.max_price_deviation = 2.0

        self.pending_events = collections.deque()
        self._event_seq = 0
        self.zone_tracker = IcebergZoneTracker()
        self.outcome_evaluator = IcebergOutcomeEvaluator()
        self.phase2_orderflow_evaluator = (
            A1ReactionEvaluator()
            if PHASE2_ORDERFLOW_EVALUATOR_ENABLED
            else None
        )
        self.phase3_candidate_evaluator = (
            ExecutionResearchCandidateEvaluator()
            if PHASE3_CANDIDATE_EVALUATOR_ENABLED
            else None
        )
        self.phase3_trade_outcome_evaluator = (
            Phase3OutcomeEvaluator()
            if PHASE3_OUTCOME_EVALUATOR_ENABLED
            else None
        )
        virtual_should_run = (
            VIRTUAL_POSITION_MANAGER_ENABLED
            and ((not REAL_EXECUTION_ENABLED) or VIRTUAL_SHADOW_MODE)
        )
        self.virtual_position_manager = VirtualPositionManager() if virtual_should_run else None
        self.research_runtime_monitor = (
            ResearchRuntimeMonitor(
                phase1_engine=self,
                label=V62_SHADOW_RUN_LABEL,
            )
            if (V62_STARTUP_SAFETY_CHECK_ENABLED or V62_INTEGRATION_HEARTBEAT_ENABLED)
            else None
        )
        if self.research_runtime_monitor:
            try:
                if V62_STARTUP_SAFETY_CHECK_ENABLED:
                    self.research_runtime_monitor.run_startup_safety_check()
                if V62_LOG_COMPONENT_STATUS_ON_START:
                    self.research_runtime_monitor.log_component_status()
                if V62_LOG_CONFIG_SNAPSHOT_ON_START:
                    self.research_runtime_monitor.log_config_snapshot()
            except Exception:
                logger.exception("[V62-MONITOR-FAILED] stage=startup")

    def process_tick(self, trade_data: Dict[str, Any]) -> Optional[Dict]:
        return self.on_trade(trade_data)

    def on_trade(self, trade_data: Dict[str, Any]) -> Optional[Dict]:
        """V3.5: 创建/合并 ACCUMULATING PendingIcebergEvent，不下单、不结算。"""
        price = float(trade_data['price'])
        size = float(trade_data['size'])
        side = str(trade_data['side']).lower()
        trade_ts = float(trade_data['ts'])
        recv_ts = float(trade_data.get('recv_ts', time.time()))
        try:
            self.outcome_evaluator.on_price(price=price, ts=trade_ts)
        except Exception:
            logger.exception("[ICEBERG-ZONE-OUTCOME] evaluator_on_price_failed")
        self._update_phase2_orderflow(trade_data=trade_data, price=price, trade_ts=trade_ts)
        if self.virtual_position_manager:
            try:
                self.virtual_position_manager.on_price(price=price, ts=trade_ts)
                self._drain_virtual_position_closed_events()
            except Exception:
                logger.exception("[VIRTUAL-POSITION-FAILED] stage=on_price")
        self._maybe_log_research_runtime_heartbeat(trade_ts)

        active_notional = price * size
        if active_notional < self.min_event_merge_notional_usdt:
            return None

        if side == 'sell':
            direction = 'BUY'
            local_book = self.ctx.bids
            zone_lower, zone_upper = price - self.local_zone_width, price
        elif side == 'buy':
            direction = 'SELL'
            local_book = self.ctx.asks
            zone_lower, zone_upper = price, price + self.local_zone_width
        else:
            return None

        merged = self._try_merge_accumulating_event(
            direction=direction,
            side=side,
            price=price,
            size=size,
            active_notional=active_notional,
            trade_ts=trade_ts,
            recv_ts=recv_ts,
        )
        if merged:
            return None

        if active_notional < self.min_event_start_notional_usdt:
            return None

        start_thickness_usdt = self._calc_local_depth_usdt(local_book, zone_lower, zone_upper)
        if start_thickness_usdt < self.min_local_depth_usdt:
            return None

        event = {
            'event_id': self._next_event_id(),
            'direction': direction,
            'trigger_price': price,
            'trigger_ts': trade_ts,
            'trigger_recv_ts': recv_ts,
            'accumulate_until_recv_ts': recv_ts + self.accumulate_window_ms / 1000.0,
            'active_notional': active_notional,
            'active_size': size,
            'side': side,
            'zone_lower': zone_lower,
            'zone_upper': zone_upper,
            'start_thickness_usdt': start_thickness_usdt,
            'book_updates_after_cutoff': 0,
            'trade_count': 1,
            'min_trade_price': price,
            'max_trade_price': price,
            'last_trade_ts': trade_ts,
            'last_trade_recv_ts': recv_ts,
            'status': 'ACCUMULATING',
        }
        self._append_pending_event(event)

        if bool(getattr(cfg, "V62_LOG_PENDING_ICEBERG_ENABLED", True)):
            logger.info(
                "[PENDING-ICEBERG] id=%s direction=%s price=%.2f active=%.0fU trades=%d depth=%.0fU zone=[%.2f, %.2f] cutoff=%dms pending=%d",
                event['event_id'],
                direction,
                price,
                active_notional,
                event['trade_count'],
                start_thickness_usdt,
                zone_lower,
                zone_upper,
                self.accumulate_window_ms,
                len(self.pending_events),
            )
        else:
            suppressed_log_counter.inc("suppressed_pending_iceberg_count")
        return None

    def _try_merge_accumulating_event(
        self,
        direction: str,
        side: str,
        price: float,
        size: float,
        active_notional: float,
        trade_ts: float,
        recv_ts: float,
    ) -> bool:
        for event in reversed(self.pending_events):
            if event.get("status") != "ACCUMULATING":
                continue
            if recv_ts > float(event.get("accumulate_until_recv_ts", 0.0)):
                continue
            if event.get("direction") != direction or event.get("side") != side:
                continue

            zone_lower = float(event.get("zone_lower", 0.0)) - self.merge_price_tolerance
            zone_upper = float(event.get("zone_upper", 0.0)) + self.merge_price_tolerance
            if not (zone_lower <= price <= zone_upper):
                continue

            event["active_notional"] += active_notional
            event["active_size"] += size
            event["trade_count"] += 1
            event["last_trade_ts"] = trade_ts
            event["last_trade_recv_ts"] = recv_ts
            event["min_trade_price"] = min(float(event["min_trade_price"]), price)
            event["max_trade_price"] = max(float(event["max_trade_price"]), price)

            logger.debug(
                "[PENDING-MERGE] id=%s direction=%s price=%.2f add=%.0fU total=%.0fU trades=%d",
                event.get("event_id"),
                direction,
                price,
                active_notional,
                event["active_notional"],
                event["trade_count"],
            )
            return True

        return False

    def _next_event_id(self) -> str:
        self._event_seq += 1
        return f"pie-{self._event_seq}"

    def _append_pending_event(self, event: Dict[str, Any]):
        if len(self.pending_events) >= self.max_pending_events:
            dropped = self.pending_events.popleft()
            if bool(getattr(cfg, "V62_LOG_PENDING_DROP_ENABLED", True)):
                logger.warning(
                    "[PENDING-DROP] reason=max_pending_events dropped_event_id=%s",
                    dropped.get('event_id', 'unknown'),
                )
            else:
                suppressed_log_counter.inc("suppressed_pending_drop_count")
        self.pending_events.append(event)

    def on_book_update(self, book_data: Dict[str, Any]) -> Optional[Dict]:
        book_ts = float(book_data.get("ts") or 0)
        book_recv_ts = float(book_data.get("recv_ts") or time.time())
        current_price = float(getattr(self.ctx, "current_price", 0.0) or 0.0)
        self.zone_tracker.expire_old_zones(book_recv_ts)
        self._update_phase2_book(book_data=book_data)

        remaining_events = []
        candidate_signals = []

        for event in self.pending_events:
            status = event.get("status")

            if status == "ACCUMULATING":
                if book_recv_ts >= float(event.get("accumulate_until_recv_ts", 0.0)):
                    event["status"] = "WAITING_BOOK"
                    event["cutoff_recv_ts"] = float(event.get("accumulate_until_recv_ts", 0.0))
                    logger.debug(
                        "[PENDING-CUTOFF] id=%s direction=%s active=%.0fU trades=%d",
                        event.get("event_id"),
                        event.get("direction"),
                        float(event.get("active_notional", 0.0)),
                        int(event.get("trade_count", 0)),
                    )
                else:
                    remaining_events.append(event)
                    continue

            if event.get("status") != "WAITING_BOOK":
                remaining_events.append(event)
                continue

            wait_ms = (book_recv_ts - float(event.get("trigger_recv_ts", book_recv_ts))) * 1000.0
            if wait_ms > self.max_wait_ms:
                if bool(getattr(cfg, "V62_LOG_CANCEL_ICEBERG_ENABLED", True)):
                    logger.info(
                        "[CANCEL-ICEBERG] id=%s reason=TIMEOUT wait=%.1fms",
                        event.get("event_id"),
                        wait_ms,
                    )
                else:
                    suppressed_log_counter.inc("suppressed_cancel_iceberg_count")
                self._update_iceberg_zone(
                    event=event,
                    result="CANCEL",
                    current_price=current_price,
                    wait_ms=wait_ms,
                    cancel_reason="TIMEOUT",
                    settle_ts=book_ts,
                    settle_recv_ts=book_recv_ts,
                )
                continue

            direction = str(event.get("direction"))
            trigger_price = float(event.get("trigger_price", 0.0))
            if direction == "BUY":
                if current_price > 0 and current_price < trigger_price - self.max_price_deviation:
                    if bool(getattr(cfg, "V62_LOG_CANCEL_ICEBERG_ENABLED", True)):
                        logger.info(
                            "[CANCEL-ICEBERG] id=%s reason=PRICE_BROKE_DOWN current=%.2f trigger=%.2f",
                            event.get("event_id"),
                            current_price,
                            trigger_price,
                        )
                    else:
                        suppressed_log_counter.inc("suppressed_cancel_iceberg_count")
                    self._update_iceberg_zone(
                        event=event,
                        result="CANCEL",
                        current_price=current_price,
                        wait_ms=wait_ms,
                        cancel_reason="PRICE_BROKE_DOWN",
                        settle_ts=book_ts,
                        settle_recv_ts=book_recv_ts,
                    )
                    continue
            elif direction == "SELL":
                if current_price > trigger_price + self.max_price_deviation:
                    if bool(getattr(cfg, "V62_LOG_CANCEL_ICEBERG_ENABLED", True)):
                        logger.info(
                            "[CANCEL-ICEBERG] id=%s reason=PRICE_BROKE_UP current=%.2f trigger=%.2f",
                            event.get("event_id"),
                            current_price,
                            trigger_price,
                        )
                    else:
                        suppressed_log_counter.inc("suppressed_cancel_iceberg_count")
                    self._update_iceberg_zone(
                        event=event,
                        result="CANCEL",
                        current_price=current_price,
                        wait_ms=wait_ms,
                        cancel_reason="PRICE_BROKE_UP",
                        settle_ts=book_ts,
                        settle_recv_ts=book_recv_ts,
                    )
                    continue

            if book_ts > 0 and book_ts < float(event.get("last_trade_ts", 0.0)):
                remaining_events.append(event)
                continue

            event["book_updates_after_cutoff"] = int(event.get("book_updates_after_cutoff", 0)) + 1
            if event["book_updates_after_cutoff"] < self.min_book_updates_after_cutoff:
                remaining_events.append(event)
                continue

            if direction == "BUY":
                end_thickness_usdt = self._calc_local_depth_usdt(
                    self.ctx.bids,
                    float(event.get("zone_lower", 0.0)),
                    float(event.get("zone_upper", 0.0)),
                )
                book_reduction = float(event.get("start_thickness_usdt", 0.0)) - end_thickness_usdt
                signal = self.iceberg_radar.detect_buy_iceberg(float(event.get("active_notional", 0.0)), book_reduction)
            else:
                end_thickness_usdt = self._calc_local_depth_usdt(
                    self.ctx.asks,
                    float(event.get("zone_lower", 0.0)),
                    float(event.get("zone_upper", 0.0)),
                )
                book_reduction = float(event.get("start_thickness_usdt", 0.0)) - end_thickness_usdt
                signal = self.iceberg_radar.detect_sell_iceberg(float(event.get("active_notional", 0.0)), book_reduction)

            hidden_volume = float(signal.get("hidden_volume", 0.0))
            absorption_rate = float(signal.get("absorption_rate", 0.0))
            active_volume = float(signal.get("active_volume", 0.0))
            confidence = float(signal.get("confidence", 0.0))
            is_spoofing_withdrawal = signal.get("behavior") == "SPOOFING_WITHDRAWAL"
            phase1_quality = None
            if signal.get("is_iceberg") and not is_spoofing_withdrawal:
                phase1_quality = self._classify_phase1_quality(signal)

            if is_spoofing_withdrawal:
                if bool(getattr(cfg, "V62_LOG_SPOOFING_WITHDRAWAL_ENABLED", True)):
                    logger.info(
                        "[SPOOFING-WITHDRAWAL] id=%s direction=%s active=%.0fU book_reduction=%.0fU hidden=%.0fU absorption=%.1f%% behavior=%s",
                        event.get("event_id"),
                        direction,
                        float(event.get("active_notional", 0.0)),
                        book_reduction,
                        hidden_volume,
                        absorption_rate * 100.0,
                        signal.get("behavior"),
                    )
                else:
                    suppressed_log_counter.inc("suppressed_spoofing_withdrawal_count")
                self._update_iceberg_zone(
                    event=event,
                    result="SPOOFING",
                    signal=signal,
                    current_price=current_price,
                    book_reduction=book_reduction,
                    wait_ms=wait_ms,
                    settle_ts=book_ts,
                    settle_recv_ts=book_recv_ts,
                )
            else:
                is_iceberg = bool(signal.get("is_iceberg"))
                log_tag = "[SETTLED-ICEBERG]" if is_iceberg else "[IGNORE-ICEBERG]"
                if is_iceberg:
                    should_log_event = bool(getattr(cfg, "V62_LOG_SETTLED_ICEBERG_ENABLED", True))
                    suppressed_key = "suppressed_settled_iceberg_count"
                else:
                    should_log_event = bool(getattr(cfg, "V62_LOG_IGNORE_ICEBERG_ENABLED", True))
                    suppressed_key = "suppressed_ignore_iceberg_count"
                if should_log_event:
                    logger.info(
                        "%s id=%s direction=%s wait=%.1fms updates=%d active=%.0fU book_reduction=%.0fU hidden=%.0fU absorption=%.1f%% trades=%d behavior=%s",
                        log_tag,
                        event.get("event_id"),
                        direction,
                        wait_ms,
                        int(event.get("book_updates_after_cutoff", 0)),
                        float(event.get("active_notional", 0.0)),
                        book_reduction,
                        hidden_volume,
                        absorption_rate * 100.0,
                        int(event.get("trade_count", 0)),
                        signal.get("behavior"),
                    )
                else:
                    suppressed_log_counter.inc(suppressed_key)
                self._update_iceberg_zone(
                    event=event,
                    result="ICEBERG" if signal.get("is_iceberg") else "IGNORE",
                    signal=signal,
                    quality=phase1_quality,
                    current_price=current_price,
                    book_reduction=book_reduction,
                    wait_ms=wait_ms,
                    settle_ts=book_ts,
                    settle_recv_ts=book_recv_ts,
                )

            if signal.get("is_iceberg") and not is_spoofing_withdrawal:
                enriched_signal = dict(signal)
                enriched_signal.update(
                    {
                        "event_type": "ICEBERG_ABSORPTION",
                        "signal_level": "PHASE1",
                        "event_id": event.get("event_id"),
                        "direction": direction,
                        "trigger_price": trigger_price,
                        "zone_lower": float(event.get("zone_lower", 0.0)),
                        "zone_upper": float(event.get("zone_upper", 0.0)),
                        "wait_ms": wait_ms,
                        "book_updates_after_cutoff": int(event.get("book_updates_after_cutoff", 0)),
                        "duration": wait_ms / 1000.0,
                        "start_thickness_usdt": float(event.get("start_thickness_usdt", 0.0)),
                        "end_thickness_usdt": end_thickness_usdt,
                        "book_reduction": book_reduction,
                        "trade_count": int(event.get("trade_count", 0)),
                        "min_trade_price": float(event.get("min_trade_price", 0.0)),
                        "max_trade_price": float(event.get("max_trade_price", 0.0)),
                        "last_trade_ts": float(event.get("last_trade_ts", 0.0)),
                        "last_trade_recv_ts": float(event.get("last_trade_recv_ts", 0.0)),
                        "phase1_quality": phase1_quality,
                        "hidden_notional": hidden_volume,
                        "absorption_ratio": absorption_rate,
                    }
                )
                enriched_signal["hidden_volume"] = hidden_volume
                enriched_signal["absorption_rate"] = absorption_rate
                if bool(getattr(cfg, "V62_LOG_PHASE1_QUALITY_ENABLED", True)):
                    logger.info(
                        "[PHASE1-QUALITY] id=%s quality=%s hidden=%.0fU absorption=%.1f%% active=%.0fU confidence=%.2f",
                        event.get("event_id"),
                        phase1_quality,
                        hidden_volume,
                        absorption_rate * 100.0,
                        active_volume,
                        confidence,
                    )
                else:
                    suppressed_log_counter.inc("suppressed_phase1_quality_count")
                if direction == "BUY":
                    enriched_signal["min_price"] = float(event.get("zone_lower", 0.0))
                elif direction == "SELL":
                    enriched_signal["max_price"] = float(event.get("zone_upper", 0.0))
                candidate_signals.append(enriched_signal)

        self.pending_events = collections.deque(remaining_events)
        finalized_zones = self.zone_tracker.drain_finalized_zones()
        for zone in finalized_zones:
            try:
                self.outcome_evaluator.finalize_zone(
                    zone,
                    now_ts=book_ts or book_recv_ts,
                    current_price=current_price,
                )
            except Exception:
                logger.exception(
                    "[ICEBERG-ZONE-OUTCOME] evaluator_finalize_failed id=%s",
                    zone.get("zone_id"),
                )

        if not candidate_signals:
            return None
        return max(candidate_signals, key=lambda s: float(s.get("confidence", 0.0)))

    def _update_iceberg_zone(
        self,
        event: Dict[str, Any],
        result: str,
        signal: Optional[Dict[str, Any]] = None,
        quality: Optional[str] = None,
        current_price: float = 0.0,
        book_reduction: float = 0.0,
        wait_ms: float = 0.0,
        cancel_reason: Optional[str] = None,
        settle_ts: float = 0.0,
        settle_recv_ts: float = 0.0,
    ) -> Optional[Dict[str, Any]]:
        zone_event = self._build_iceberg_impact_event(
            event=event,
            result=result,
            signal=signal,
            quality=quality,
            book_reduction=book_reduction,
            wait_ms=wait_ms,
            cancel_reason=cancel_reason,
            settle_ts=settle_ts,
            settle_recv_ts=settle_recv_ts,
        )
        zone = self.zone_tracker.update(zone_event, current_price=current_price)
        if zone:
            try:
                self.outcome_evaluator.upsert_zone(
                    zone,
                    now_ts=settle_ts or settle_recv_ts or time.time(),
                    current_price=current_price,
                )
            except Exception:
                logger.exception(
                    "[ICEBERG-ZONE-OUTCOME] evaluator_upsert_failed id=%s",
                    zone.get("zone_id"),
                )
            self._register_phase2_frozen_zone(zone)
        return zone

    def _register_phase2_frozen_zone(self, zone: Dict[str, Any]) -> None:
        if not self.phase2_orderflow_evaluator or not zone.get("is_frozen"):
            return
        try:
            public_zone = IcebergZoneTracker._public_zone(zone)
            self.phase2_orderflow_evaluator.register_frozen_zone(public_zone)
        except Exception:
            logger.exception(
                "[PHASE2-REGISTER-FAILED] zone_id=%s",
                zone.get("zone_id"),
            )

    def _update_phase2_orderflow(self, trade_data: Dict[str, Any], price: float, trade_ts: float) -> None:
        if not self.phase2_orderflow_evaluator:
            return
        try:
            enriched_trade = dict(trade_data)
            enriched_trade.setdefault("price", price)
            enriched_trade.setdefault("ts", trade_ts)
            self.phase2_orderflow_evaluator.on_trade(enriched_trade)
            self._drain_phase2_confirmed_events()
        except Exception:
            logger.exception("[PHASE2-ORDERFLOW-FAILED]")

    def _update_phase2_book(self, book_data: Dict[str, Any]) -> None:
        if not self.phase2_orderflow_evaluator:
            return
        try:
            phase2_book_data = dict(book_data) if isinstance(book_data, dict) else {}
            ctx_bids = getattr(self.ctx, "bids", None)
            ctx_asks = getattr(self.ctx, "asks", None)
            if ctx_bids:
                phase2_book_data["bids"] = ctx_bids
            if ctx_asks:
                phase2_book_data["asks"] = ctx_asks
            self.phase2_orderflow_evaluator.on_book_update(phase2_book_data)
            self._drain_phase2_confirmed_events()
        except Exception:
            logger.exception("[PHASE2-BOOK-FAILED]")

    def _drain_phase2_confirmed_events(self) -> None:
        if not self.phase2_orderflow_evaluator or not self.phase3_candidate_evaluator:
            return
        try:
            pop_events = getattr(self.phase2_orderflow_evaluator, "pop_confirmed_events", None)
            if not callable(pop_events):
                return
            phase2_events = pop_events()
        except Exception:
            logger.exception("[PHASE3-CANDIDATE-FAILED] stage=pop_confirmed_events")
            return

        for event in phase2_events or []:
            try:
                result = self.phase3_candidate_evaluator.evaluate_phase2_confirmed(event)
                if result and self.virtual_position_manager:
                    try:
                        self.virtual_position_manager.on_candidate(result)
                        self._drain_virtual_position_closed_events()
                    except Exception:
                        logger.exception("[VIRTUAL-POSITION-FAILED] stage=on_candidate zone_id=%s", event.get("zone_id") if isinstance(event, dict) else None)
            except Exception:
                logger.exception(
                    "[PHASE3-CANDIDATE-FAILED] zone_id=%s",
                    event.get("zone_id") if isinstance(event, dict) else None,
                )

    def _drain_virtual_position_closed_events(self) -> None:
        if not self.virtual_position_manager or not self.phase3_trade_outcome_evaluator:
            return
        try:
            pop_events = getattr(self.virtual_position_manager, "pop_closed_events", None)
            if not callable(pop_events):
                return
            closed_events = pop_events()
            for closed_event in closed_events or []:
                try:
                    self.phase3_trade_outcome_evaluator.on_virtual_position_closed(closed_event)
                except Exception:
                    logger.exception("[PHASE3-OUTCOME-FAILED] position_id=%s", closed_event.get("position_id") if isinstance(closed_event, dict) else None)
        except Exception:
            logger.exception("[PHASE3-OUTCOME-FAILED] stage=pop_closed_events")

    def _maybe_log_research_runtime_heartbeat(self, now_ts: float) -> None:
        if not self.research_runtime_monitor:
            return
        try:
            self.research_runtime_monitor.maybe_log_heartbeat(now_ts)
        except Exception:
            logger.exception("[V62-MONITOR-FAILED] stage=heartbeat")

    def log_research_runtime_final_summary(self) -> Optional[Dict[str, Any]]:
        if not self.research_runtime_monitor:
            return None
        try:
            return self.research_runtime_monitor.log_final_summary()
        except Exception:
            logger.exception("[V62-MONITOR-FAILED] stage=final_summary")
            return None

    def _build_iceberg_impact_event(
        self,
        event: Dict[str, Any],
        result: str,
        signal: Optional[Dict[str, Any]] = None,
        quality: Optional[str] = None,
        book_reduction: float = 0.0,
        wait_ms: float = 0.0,
        cancel_reason: Optional[str] = None,
        settle_ts: float = 0.0,
        settle_recv_ts: float = 0.0,
    ) -> Dict[str, Any]:
        signal = signal or {}
        recv_ts = self._safe_float(settle_recv_ts, time.time()) or time.time()
        ts = (
            self._safe_float(settle_ts, 0.0)
            or self._safe_float(event.get("last_trade_ts"), 0.0)
            or self._safe_float(event.get("trigger_ts"), 0.0)
            or recv_ts
        )
        active_volume = self._safe_float(signal.get("active_volume", event.get("active_notional", 0.0)), 0.0)

        return {
            "event_id": str(event.get("event_id", "")),
            "ts": ts,
            "recv_ts": recv_ts,
            "direction": str(event.get("direction", "")),
            "result": result,
            "quality": quality,
            "trigger_price": self._safe_float(event.get("trigger_price"), 0.0),
            "zone_lower": self._safe_float(event.get("zone_lower"), 0.0),
            "zone_upper": self._safe_float(event.get("zone_upper"), 0.0),
            "active_volume": active_volume,
            "hidden_volume": self._safe_float(signal.get("hidden_volume"), 0.0),
            "absorption_rate": self._safe_float(signal.get("absorption_rate"), 0.0),
            "confidence": self._safe_float(signal.get("confidence"), 0.0),
            "book_reduction": self._safe_float(book_reduction, 0.0),
            "trade_count": self._safe_int(event.get("trade_count"), 0),
            "wait_ms": self._safe_float(wait_ms, 0.0),
            "behavior": str(signal.get("behavior") or ("CANCEL" if result == "CANCEL" else "")),
            "cancel_reason": cancel_reason,
        }

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(default)

    def _classify_phase1_quality(self, signal: Dict[str, Any]) -> str:
        hidden_volume = float(signal.get("hidden_volume", 0.0))
        absorption_rate = float(signal.get("absorption_rate", 0.0))
        active_volume = float(signal.get("active_volume", 0.0))
        confidence = float(signal.get("confidence", 0.0))

        if (
            hidden_volume >= 2_000_000
            and absorption_rate >= 0.80
            and active_volume >= 2_000_000
            and confidence >= 0.85
        ):
            return "HIGH"

        if (
            hidden_volume >= 1_000_000
            and absorption_rate >= 0.70
            and active_volume >= 1_000_000
            and confidence >= 0.75
        ):
            return "MEDIUM"

        return "LOW"

    @staticmethod
    def _calc_local_depth_usdt(book_levels: Dict[float, float], zone_lower: float, zone_upper: float) -> float:
        depth = 0.0
        for raw_price, raw_size in book_levels.items():
            p = float(raw_price)
            if zone_lower <= p <= zone_upper:
                depth += p * float(raw_size)
        return depth
