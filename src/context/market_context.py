#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 4/17/2026 11:08 PM
@File       : market_context.py
@Description: 
"""
import collections
from typing import List, Dict, Any, Optional

class MarketContext:
    """
    市场状态管家 (The Single Source of Truth)
    定位：以极低的延迟在内存中重构盘口与动能，提供 O(1) 的写入和动态的区间查询。
    绝对不包含任何交易逻辑。
    """
    def __init__(self, volume_bar_size: float = 500.0):
        # ==========================================
        # 1. 宏观地图层 (Macro Map)
        # ==========================================
        # 存放15分钟级别的波段低点（多头防线/扫损区）
        self.support_levels: List[float] = [] 
        
        # ==========================================
        # 2. 微观动能层 (Micro Dynamics)
        # ==========================================
        self.current_cvd: float = 0.0
        
        # 等量K线参数
        self.volume_bar_size: float = volume_bar_size 
        self.current_vol_accumulator: float = 0.0
        
        # 使用双端队列存储最近 100 根等量K线，底层为链表，确保 append 为绝对的 O(1)，无内存重分配抖动
        self.volume_bars = collections.deque(maxlen=100) 
        
        # 记录最新价，方便雷达随时调取
        self.current_price: float = 0.0

        # ==========================================
        # 3. 盘口状态层 (Orderbook State)
        # ==========================================
        # 扁平化绝对价格字典，精度 0.01。
        # 只有用到时才做区间聚合，解决边缘截断效应。
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}

    # ==========================================
    # [数据吸入层] - 供 okx_stream 极速调用 (严格 O(1))
    # ==========================================

    def apply_trade(self, trade_data: Dict[str, Any]) -> None:
        """
        处理逐笔成交 (Trades)
        预期 OKX 数据格式: {'price': '3000.50', 'sz': '10.5', 'side': 'buy'}
        """
        price = float(trade_data['price'])
        sz = float(trade_data['sz'])
        side = trade_data['side']
        
        self.current_price = price

        # 1. 更新 CVD (累计量微差)
        delta = sz if side == 'buy' else -sz
        self.current_cvd += delta

        # 2. 累加并生成等量 K 线 (Volume Bars)
        self.current_vol_accumulator += sz
        
        if self.current_vol_accumulator >= self.volume_bar_size:
            # 达到设定的体积，封口并存入双端队列
            self.volume_bars.append({
                "close_price": price,
                "cvd_at_close": self.current_cvd
            })
            # 重置累加器 (扣除刚好超过的部分，保证精度不丢失)
            self.current_vol_accumulator -= self.volume_bar_size

    def apply_book_delta(self, book_data: Dict[str, Any]) -> None:
        """
        处理订单簿增量 (Books / Books-l2-tbt)
        预期 OKX 数据格式: {'bids': [['2999.50', '10', '0', '1']], 'asks': [...]}
        """
        # 极速更新买盘 (Bids)
        for item in book_data.get('bids', []):
            price = float(item[0])
            qty = float(item[1])
            
            if qty == 0.0:
                # 撤单，从字典中安全移除
                self.bids.pop(price, None)
            else:
                # 新增或修改挂单量
                self.bids[price] = qty

        # 极速更新卖盘 (Asks)
        for item in book_data.get('asks', []):
            price = float(item[0])
            qty = float(item[1])
            
            if qty == 0.0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

    def set_support_levels(self, levels: List[float]) -> None:
        """
        冷启动或后台刷新时，由外部 (如 okx_loader 或 15m K线聚合器) 注入宏观防线
        """
        self.support_levels = levels

    # ==========================================
    # [查询服务层] - 供 Detectors 雷达调用 (按需计算)
    # ==========================================

    def get_zone_thickness(self, upper_price: float, lower_price: float) -> float:
        """
        [冰山雷达专用] 动态查询特定价格区间的买盘总厚度。
        利用生成器表达式实现零内存分配 (Zero-Allocation)。
        """
        # 仅遍历买盘字典，算出处于下限和上限之间的挂单量总和
        return sum(vol for p, vol in self.bids.items() if lower_price <= p <= upper_price)

    def get_recent_cvd_trend(self, lookback_bars: int = 3) -> float:
        """
        [扫损雷达专用] 获取最近 N 根等量 K 线的 CVD 净变动。
        负数说明在疯狂砸盘，正数说明在疯狂买入。
        """
        num_bars = len(self.volume_bars)
        if num_bars == 0:
            return 0.0
            
        # 如果历史 K 线数量不足，就取能拿到的最远的一根
        actual_lookback = min(lookback_bars, num_bars)
        
        # 对比当前的实时 CVD 与 N 根之前的历史 CVD
        oldest_cvd = self.volume_bars[-actual_lookback]["cvd_at_close"]
        return self.current_cvd - oldest_cvd

    def get_sweep_zone(self, current_price: float, buffer: float = 1.0) -> Optional[tuple]:
        """
        [扫损雷达专用] 检查当前价格是否刺穿了历史防线。
        如果刺穿，返回该防线的上下轨 (zone_upper, zone_lower)，供冰山雷达后续监视。
        如果没有刺穿，返回 None。
        """
        for support in self.support_levels:
            # 定义：当前价低于或等于支撑位，且没有跌透设定的 buffer (比如跌破不多于1块钱)
            if (support - buffer) <= current_price <= support:
                # 返回一个 Tuple：(监控区上限, 监控区下限)
                return (support, support - buffer)
                
        return None
