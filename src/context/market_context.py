#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 4/17/2026 11:08 PM
@File       : market_context.py
@Description: 
"""
# src/context/market_context.py

import collections
import time
from typing import List, Dict, Any, Optional

class MarketContext:
    """
    市场状态管家 (The Single Source of Truth) - USDT 机构级资金计价版
    """
    def __init__(self, target_notional_usdt: float = 1_500_000.0, level_ttl_days: int = 7):
        # ==========================================
        # 1. 结构化流动性池 (Liquidity Pools)
        # ==========================================
        self.sell_side_liquidity: Dict[float, Dict] = {} # 支撑位/波段低点 (SSL)
        self.buy_side_liquidity: Dict[float, Dict] = {}  # 阻力位/波段高点 (BSL)
        
        self.level_ttl_seconds = level_ttl_days * 24 * 3600
        self.merge_threshold = 1.0 

        # ==========================================
        # 2. 微观动能与盘口 (USDT 名义价值版)
        # ==========================================
        self.current_price: float = 0.0
        # CVD 依然使用 ETH 数量作为动能差衡量标准（金融业惯例）
        self.current_cvd: float = 0.0
        
        # 核心：替换为目标 USDT 价值 (默认 150 万美金)
        self.target_notional_usdt = target_notional_usdt
        # USDT 资金累加器
        self.current_notional_accumulator = 0.0
        
        # 将微观视野拉长，缓存提升到 300 根
        self.volume_bars = collections.deque(maxlen=300)
        
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}

    # ==========================================
    # [数据吸入层]
    # ==========================================

    def apply_trade(self, trade_data: Dict[str, Any]) -> None:
        """处理逐笔成交，按 USDT 价值推进 K 线，并检查是否消耗了防线"""
        price = float(trade_data['price'])
        
        # [修复 Bug]: 这里原来是 'sz'，现在对齐 okx_stream 传过来的 'size'
        size = float(trade_data['size'])
        side = trade_data['side']
        
        self.current_price = price

        # 1. 更新动能数据 (O(1))
        delta = size if side == 'buy' else -size
        self.current_cvd += delta
        
        # 2. 转化为 USDT 名义资金，交给 K 线生成器
        trade_notional = price * size
        self._update_volume_bars(price, trade_notional)

        # 3. 检查防线消耗 (Mitigation)
        self._mitigate_levels(price)

    def apply_book_delta(self, book_data: Dict[str, Any]) -> None:
        """同步本地订单簿字典 (O(1))"""
        for item in book_data.get('bids', []):
            p, q = float(item[0]), float(item[1])
            if q == 0: self.bids.pop(p, None)
            else: self.bids[p] = q

        for item in book_data.get('asks', []):
            p, q = float(item[0]), float(item[1])
            if q == 0: self.asks.pop(p, None)
            else: self.asks[p] = q

    # ==========================================
    # [防线管理与微观组装逻辑]
    # ==========================================

    def _update_volume_bars(self, price: float, trade_notional: float):
        """内部方法：维护 USDT 等值资金 K 线"""
        self.current_notional_accumulator += trade_notional
        
        # 如果资金池蓄满了（例如达到了 150 万 USDT）
        if self.current_notional_accumulator >= self.target_notional_usdt:
            self.volume_bars.append({
                "close": price,
                "cvd": self.current_cvd,
                "time": time.time()
            })
            # 扣除阈值，将溢出的资金保留到下一个周期（防止高频环境下的资金统计丢失）
            self.current_notional_accumulator -= self.target_notional_usdt

    def add_liquidity_level(self, price: float, level_type: str):
        target_dict = self.sell_side_liquidity if level_type == 'SSL' else self.buy_side_liquidity
        for existing_price in list(target_dict.keys()):
            if abs(existing_price - price) <= self.merge_threshold:
                target_dict[existing_price]['weight'] += 1
                target_dict[existing_price]['timestamp'] = time.time()
                return
        target_dict[price] = {
            "timestamp": time.time(),
            "weight": 1
        }

    def _mitigate_levels(self, current_price: float):
        for price in list(self.sell_side_liquidity.keys()):
            if current_price <= price:
                self.sell_side_liquidity.pop(price)
        for price in list(self.buy_side_liquidity.keys()):
            if current_price >= price:
                self.buy_side_liquidity.pop(price)

    def clean_expired_levels(self):
        now = time.time()
        for d in [self.sell_side_liquidity, self.buy_side_liquidity]:
            for p, info in list(d.items()):
                if now - info['timestamp'] > self.level_ttl_seconds:
                    d.pop(p)

    # ==========================================
    # [查询服务层]
    # ==========================================

    def get_active_sweep_zone(self, buffer: float = 1.0) -> Optional[dict]:
        for price, info in self.sell_side_liquidity.items():
            if (price - buffer) <= self.current_price <= price:
                return {
                    "type": "SSL_SWEEP", 
                    "level": price, 
                    "weight": info['weight'],
                    "zone": (price, price - buffer)
                }
        for price, info in self.buy_side_liquidity.items():
            if price <= self.current_price <= (price + buffer):
                return {
                    "type": "BSL_SWEEP", 
                    "level": price, 
                    "weight": info['weight'],
                    "zone": (price, price + buffer)
                }
        return None

    def get_zone_thickness(self, upper: float, lower: float, side: str = 'bids') -> float:
        target_book = self.bids if side == 'bids' else self.asks
        return sum(vol for p, vol in target_book.items() if lower <= p <= upper)
