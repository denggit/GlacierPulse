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
    市场状态管家 (The Single Source of Truth) - 强化版
    
    更新日志：
    1. 引入 Liquidity Pools：区分 Buy-Side (高点) 和 Sell-Side (低点)。
    2. 自动消耗机制：价格一旦触碰，防线立刻失效（防止刻舟求剑）。
    3. 权重/合并逻辑：相近的价格会自动合并并增加权重。
    4. 性能优化：维持写入 O(1)，查询微秒级。
    """
    def __init__(self, volume_bar_size: float = 500.0, level_ttl_days: int = 7):
        # ==========================================
        # 1. 结构化流动性池 (Liquidity Pools)
        # ==========================================
        # 存储格式：{price: {"timestamp": float, "weight": int, "type": str}}
        self.sell_side_liquidity: Dict[float, Dict] = {} # 支撑位/波段低点 (SSL)
        self.buy_side_liquidity: Dict[float, Dict] = {}  # 阻力位/波段高点 (BSL)
        
        self.level_ttl_seconds = level_ttl_days * 24 * 3600
        # 价格合并阈值：ETH建议0.5-1.0 USDT，如果两个低点距离小于此，视为同一个
        self.merge_threshold = 1.0 

        # ==========================================
        # 2. 微观动能与盘口 (保持极速)
        # ==========================================
        self.current_price: float = 0.0
        self.current_cvd: float = 0.0
        self.volume_bar_size = volume_bar_size
        self.current_vol_accumulator = 0.0
        self.volume_bars = collections.deque(maxlen=100)
        
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}

    # ==========================================
    # [数据吸入层]
    # ==========================================

    def apply_trade(self, trade_data: Dict[str, Any]) -> None:
        """处理逐笔成交，并检查是否消耗了防线"""
        price = float(trade_data['price'])
        sz = float(trade_data['size'])
        side = trade_data['side']
        
        self.current_price = price

        # 1. 更新动能数据 (O(1))
        delta = sz if side == 'buy' else -sz
        self.current_cvd += delta
        self._update_volume_bars(price, sz)

        # 2. 检查防线消耗 (Mitigation)
        # 如果价格跌破支撑或冲破阻力，该防线被“点火”消耗，从此失效
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
    # [防线管理逻辑]
    # ==========================================

    def add_liquidity_level(self, price: float, level_type: str):
        """
        新增防线：具备自动合并（Confluence）逻辑
        level_type: 'SSL' (支撑) 或 'BSL' (阻力)
        """
        target_dict = self.sell_side_liquidity if level_type == 'SSL' else self.buy_side_liquidity
        
        # 检查是否存在相近的防线，如果有，则合并并增加权重
        for existing_price in list(target_dict.keys()):
            if abs(existing_price - price) <= self.merge_threshold:
                target_dict[existing_price]['weight'] += 1
                target_dict[existing_price]['timestamp'] = time.time()
                return

        # 如果是全新的价格，则新增
        target_dict[price] = {
            "timestamp": time.time(),
            "weight": 1
        }

    def _mitigate_levels(self, current_price: float):
        """
        消耗机制：剔除已被触碰的防线
        """
        # 支撑位被跌破 (或触碰) -> 移除
        for price in list(self.sell_side_liquidity.keys()):
            if current_price <= price:
                self.sell_side_liquidity.pop(price)

        # 阻力位被突破 -> 移除
        for price in list(self.buy_side_liquidity.keys()):
            if current_price >= price:
                self.buy_side_liquidity.pop(price)

    def clean_expired_levels(self):
        """清理超过 TTL 的老旧防线 (建议每小时运行一次)"""
        now = time.time()
        for d in [self.sell_side_liquidity, self.buy_side_liquidity]:
            for p, info in list(d.items()):
                if now - info['timestamp'] > self.level_ttl_seconds:
                    d.pop(p)

    # ==========================================
    # [查询服务层]
    # ==========================================

    def get_active_sweep_zone(self, buffer: float = 1.0) -> Optional[dict]:
        """
        雷达查询：当前价格是否正在“清扫”某个防线？
        """
        # 检查支撑位 (看多机会)
        for price, info in self.sell_side_liquidity.items():
            if (price - buffer) <= self.current_price <= price:
                return {
                    "type": "SSL_SWEEP", 
                    "level": price, 
                    "weight": info['weight'],
                    "zone": (price, price - buffer)
                }
        
        # 检查阻力位 (看空机会)
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
        """区间挂单查询"""
        target_book = self.bids if side == 'bids' else self.asks
        return sum(vol for p, vol in target_book.items() if lower <= p <= upper)

    def _update_volume_bars(self, price: float, sz: float):
        """内部方法：维护等量K线"""
        self.current_vol_accumulator += sz
        if self.current_vol_accumulator >= self.volume_bar_size:
            self.volume_bars.append({
                "close": price,
                "cvd": self.current_cvd,
                "time": time.time()
            })
            self.current_vol_accumulator -= self.volume_bar_size
