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
            'book_updates_seen': 0,  # 兼容统计：V4 结算核心应使用 book_updates_after_cutoff
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

    @staticmethod
    def _calc_local_depth_usdt(book_levels: Dict[float, float], zone_lower: float, zone_upper: float) -> float:
        depth = 0.0
        for raw_price, raw_size in book_levels.items():
            p = float(raw_price)
            if zone_lower <= p <= zone_upper:
                depth += p * float(raw_size)
        return depth
