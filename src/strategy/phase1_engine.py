#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 4/28/2026
@File       : phase1_engine.py
@Description: 机构级扫损与冰山点火引擎 (V3 Pending Event Manager)
"""

import collections
import logging
import time
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class Phase1Engine:
    def __init__(self, market_context, iceberg_detector):
        self.ctx = market_context
        self.iceberg_radar = iceberg_detector

        self.min_event_start_notional_usdt = 150_000
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

    def process_tick(self, trade_data: Dict[str, Any]) -> Optional[Dict]:
        return self.on_trade(trade_data)

    def on_trade(self, trade_data: Dict[str, Any]) -> Optional[Dict]:
        """V3.5: 创建/合并 ACCUMULATING PendingIcebergEvent，不下单、不结算。"""
        price = float(trade_data['price'])
        size = float(trade_data['size'])
        side = str(trade_data['side']).lower()
        trade_ts = float(trade_data['ts'])
        recv_ts = float(trade_data.get('recv_ts', time.time()))

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
            logger.warning(
                "[PENDING-DROP] reason=max_pending_events dropped_event_id=%s",
                dropped.get('event_id', 'unknown'),
            )
        self.pending_events.append(event)

    def on_book_update(self, book_data: Dict[str, Any]) -> Optional[Dict]:
        book_ts = float(book_data.get("ts") or 0)
        book_recv_ts = float(book_data.get("recv_ts") or time.time())
        current_price = float(getattr(self.ctx, "current_price", 0.0) or 0.0)

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
                logger.info(
                    "[CANCEL-ICEBERG] id=%s reason=TIMEOUT wait=%.1fms",
                    event.get("event_id"),
                    wait_ms,
                )
                continue

            direction = str(event.get("direction"))
            trigger_price = float(event.get("trigger_price", 0.0))
            if direction == "BUY":
                if current_price > 0 and current_price < trigger_price - self.max_price_deviation:
                    logger.info(
                        "[CANCEL-ICEBERG] id=%s reason=PRICE_BROKE_DOWN current=%.2f trigger=%.2f",
                        event.get("event_id"),
                        current_price,
                        trigger_price,
                    )
                    continue
            elif direction == "SELL":
                if current_price > trigger_price + self.max_price_deviation:
                    logger.info(
                        "[CANCEL-ICEBERG] id=%s reason=PRICE_BROKE_UP current=%.2f trigger=%.2f",
                        event.get("event_id"),
                        current_price,
                        trigger_price,
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
            is_spoofing_withdrawal = signal.get("behavior") == "SPOOFING_WITHDRAWAL"
            if is_spoofing_withdrawal:
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
                log_tag = "[SETTLED-ICEBERG]" if signal.get("is_iceberg") else "[IGNORE-ICEBERG]"
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
                    }
                )
                enriched_signal["hidden_volume"] = hidden_volume
                enriched_signal["absorption_rate"] = absorption_rate
                if direction == "BUY":
                    enriched_signal["min_price"] = float(event.get("zone_lower", 0.0))
                elif direction == "SELL":
                    enriched_signal["max_price"] = float(event.get("zone_upper", 0.0))
                candidate_signals.append(enriched_signal)

        self.pending_events = collections.deque(remaining_events)

        if not candidate_signals:
            return None
        return max(candidate_signals, key=lambda s: float(s.get("confidence", 0.0)))

    @staticmethod
    def _calc_local_depth_usdt(book_levels: Dict[float, float], zone_lower: float, zone_upper: float) -> float:
        depth = 0.0
        for raw_price, raw_size in book_levels.items():
            p = float(raw_price)
            if zone_lower <= p <= zone_upper:
                depth += p * float(raw_size)
        return depth
