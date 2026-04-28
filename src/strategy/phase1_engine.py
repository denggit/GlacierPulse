#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 4/28/2026
@File       : phase1_engine.py
@Description: 机构级扫损与冰山点火引擎 (Tick流速驱动 + 战火线隔离版)
"""

import collections
import copy
import time
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class Phase1Engine:
    def __init__(self, market_context, iceberg_detector):
        self.ctx = market_context
        self.iceberg_radar = iceberg_detector
        
        # ==========================================
        # 1. 极速点火参数 (Trigger Configs)
        # ==========================================
        # 在过去 X 秒内，如果净砸盘量达到 Y，立刻点火！
        self.trigger_window_sec = 3.0
        self.trigger_cvd_usdt = -8_000_000.0  # 默认 800万 USDT 的极速抛压
        self.tick_buffer = collections.deque() # 存放元组: (timestamp, cvd_delta_usdt)
        
        # ==========================================
        # 2. 动态熔断关窗参数 (Close Configs)
        # ==========================================
        self.exhaustion_sec = 1.5           # 熔断器1：超过 1.5 秒没有新的强力砸盘，动能衰竭
        self.reversal_cvd_usdt = 500_000.0  # 熔断器2：出现 50万 USDT 的主买，多头反击，多空逆转
        self.max_drop_pct = 0.01            # 熔断器3：价格瞬间击穿超过 1% (黑天鹅防守)
        
        # ==========================================
        # 3. 状态机与快照内存 (State & Snapshots)
        # ==========================================
        self.is_detecting = False
        
        self.detect_start_ts = 0.0
        self.start_price = 0.0
        self.start_bids_snapshot: Dict[float, float] = {}
        
        # 核心：战火线隔离器 (精确记录发生过成交的价格极值)
        self.traded_min_price = float('inf')
        self.traded_max_price = 0.0
        
        self.window_cvd_usdt = 0.0          # 窗口期内累计的净抛压
        self.window_recent_cvd = 0.0        # 用于监测是否多空逆转的短期累计
        self.window_last_trade_ts = 0.0     # 用于监测动能是否衰竭

    def process_tick(self, trade_data: Dict[str, Any]) -> Optional[Dict]:
        """
        主入口：接收 WebSocket 传来的每一笔实时 Trade
        必须在 market_context 处理完基础记账后调用！
        """
        # OKX 传来的 timestamp 是毫秒，转成秒
        ts = int(trade_data.get('ts', time.time() * 1000)) / 1000.0
        price = float(trade_data['price'])
        size = float(trade_data['size'])
        side = trade_data['side']
        
        # 计算这笔交易的 USDT 名义价值
        trade_usdt = price * size
        cvd_delta = trade_usdt if side == 'buy' else -trade_usdt

        # --- 维护点火视窗 (永远在线) ---
        self.tick_buffer.append((ts, cvd_delta))
        self._clean_tick_buffer(ts)
        
        if not self.is_detecting:
            # 状态：闲置巡逻中 -> 检查是否需要点火
            if self._check_trigger_condition():
                self._open_window(ts, price)
        else:
            # 状态：窗口已开启 -> 维持观测，更新战火线，并检查是否熔断
            self._update_observation(ts, price, cvd_delta)
            
            if self._check_close_condition(ts, price):
                return self._close_window_and_detect()
                
        return None

    def _clean_tick_buffer(self, current_ts: float):
        """踢出 3 秒前的旧数据，保持窗口滑动"""
        while self.tick_buffer and (current_ts - self.tick_buffer[0][0]) > self.trigger_window_sec:
            self.tick_buffer.popleft()

    def _check_trigger_condition(self) -> bool:
        """检查点火条件：过去3秒内总净流出是否达到了 800 万"""
        if not self.tick_buffer: return False
        
        # 快速求和过去3秒的 CVD 差值
        recent_net_cvd = sum(delta for _, delta in self.tick_buffer)
        
        # 如果是极端的负数 (抛压大于阈值)
        return recent_net_cvd <= self.trigger_cvd_usdt

    def _open_window(self, ts: float, current_price: float):
        """🔥 引擎点火！记录快照"""
        self.is_detecting = True
        self.detect_start_ts = ts
        self.start_price = current_price
        
        # 【神级优化】：深拷贝当前的 Bids 订单簿，作为初始快照
        self.start_bids_snapshot = copy.deepcopy(self.ctx.bids)
        
        # 初始化战火线和动能累计
        self.traded_min_price = current_price
        self.traded_max_price = current_price
        self.window_cvd_usdt = 0.0
        self.window_recent_cvd = 0.0
        self.window_last_trade_ts = ts
        
        logger.info(f"🔥 [流速点火] 检测到极限抛压，开启冰山观测窗！起始价格: {current_price}")

    def _update_observation(self, ts: float, price: float, cvd_delta: float):
        """窗口维持：动态拉伸战火线"""
        self.window_last_trade_ts = ts
        self.window_cvd_usdt += cvd_delta
        
        # 记录最近一小段的动能，用来查反弹
        self.window_recent_cvd += cvd_delta 
        if cvd_delta < 0: # 如果还在砸盘，重置反弹累计器
            self.window_recent_cvd = 0.0
            
        # 动态扩大战火线 (极其关键：只记录发生过真实成交的区间)
        if price < self.traded_min_price: self.traded_min_price = price
        if price > self.traded_max_price: self.traded_max_price = price

    def _check_close_condition(self, ts: float, current_price: float) -> bool:
        """检查三大熔断条件"""
        # 1. 动能衰竭 (例如超过1.5秒没新单子)
        # 注意：在实盘中，需要有一个外部的定时器/心跳来驱动没有交易时的衰竭
        # 这里用两笔 trade 的时间差做简化模拟
        if (ts - self.window_last_trade_ts) >= self.exhaustion_sec:
            logger.debug("🛑 [熔断关窗] 抛压动能衰竭。")
            return True
            
        # 2. 多空逆转 (出现连续的反向买单吃货)
        if self.window_recent_cvd >= self.reversal_cvd_usdt:
            logger.debug("🛑 [熔断关窗] 多头开始反击，抛压被消化。")
            return True
            
        # 3. 价格击穿底线 (防御无底洞崩盘)
        drop_pct = (self.start_price - current_price) / self.start_price
        if drop_pct >= self.max_drop_pct:
            logger.debug(f"🛑 [熔断关窗] 价格击穿安全垫 ({drop_pct:.2%})，放弃检测。")
            return True
            
        return False

    def _close_window_and_detect(self) -> Optional[Dict]:
        """结算：战火线切片隔离，并呼叫雷达"""
        # 获取当前(结束时)的订单簿
        end_bids_snapshot = self.ctx.bids
        
        start_thickness_usdt = 0.0
        end_thickness_usdt = 0.0
        
        # 【核心逻辑】：只遍历战火线区间！过滤掉上下方主力的撤单欺诈
        # 因为盘口是以价格为 Key，我们需要确保只统计区间内的流动性
        for p, vol in self.start_bids_snapshot.items():
            if self.traded_min_price <= p <= self.traded_max_price:
                start_thickness_usdt += (p * vol)  # 转换为 USDT 价值
                
        for p, vol in end_bids_snapshot.items():
            if self.traded_min_price <= p <= self.traded_max_price:
                end_thickness_usdt += (p * vol)
                
        # 盘口真实的减少量
        book_reduction = start_thickness_usdt - end_thickness_usdt
        
        # 这段窗口期内的绝对砸盘量 (必定是正数)
        active_sold = abs(self.window_cvd_usdt)
        
        logger.info(f"📊 [观测结算] 耗时: {self.window_last_trade_ts - self.detect_start_ts:.2f}s | "
                    f"战火区: [{self.traded_min_price}, {self.traded_max_price}] | "
                    f"总砸盘: {active_sold:,.0f} U | 盘口消耗: {book_reduction:,.0f} U")

        # 状态机重置，准备迎接下一次暴风雨
        self.is_detecting = False
        self.tick_buffer.clear() 

        # 呼叫黑盒！
        signal = self.iceberg_radar.detect_buy_iceberg(active_sold, book_reduction)
        
        if signal['is_iceberg'] or signal['behavior'] == 'SPOOFING_WITHDRAWAL':
            return signal
            
        return None
