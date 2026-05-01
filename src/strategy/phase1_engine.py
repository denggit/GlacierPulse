#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 4/28/2026
@File       : phase1_engine.py
@Description: 机构级扫损与冰山点火引擎 (全天候双向双轨版)
"""

import collections
import copy
import logging
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)


class Phase1Engine:
    def __init__(self, market_context, iceberg_detector):
        self.ctx = market_context
        self.iceberg_radar = iceberg_detector

        # ==========================================
        # 1. 极速点火参数 (双向触发)
        # ==========================================
        self.trigger_window_sec = 3.0
        self.trigger_buy_cvd = -1_500_000.0  # 暴跌 150万 U，触发【多头冰山】探测 (寻找底部支撑)
        self.trigger_sell_cvd = 1_500_000.0  # 暴涨 150万 U，触发【空头冰山】探测 (寻找顶部阻力)
        self.tick_buffer = collections.deque()

        # ==========================================
        # 2. 动态熔断关窗参数
        # ==========================================
        self.exhaustion_sec = 1.5
        self.reversal_cvd_usdt = 150_000.0  # 反向逆转门槛 15万 U
        self.max_drop_pct = 0.01  # 击穿安全垫防御 (1%)
        self.cooldown_until_ts = 0.0

        # ==========================================
        # 3. 状态机与快照内存
        # ==========================================
        self.is_detecting = False
        self.current_direction = None  # 当前探测方向: 'BUY' (找底) 或 'SELL' (找顶)

        self.detect_start_ts = 0.0
        self.start_price = 0.0
        self.start_bids_snapshot: Dict[float, float] = {}
        self.start_asks_snapshot: Dict[float, float] = {}

        self.traded_min_price = float('inf')
        self.traded_max_price = 0.0

        self.window_cvd_usdt = 0.0
        self.window_recent_cvd = 0.0
        self.window_last_trade_ts = 0.0

    def process_tick(self, trade_data: Dict[str, Any]) -> Optional[Dict]:
        ts = float(trade_data['ts'])
        price = float(trade_data['price'])
        size = float(trade_data['size'])
        side = trade_data['side']

        trade_usdt = price * size
        cvd_delta = trade_usdt if side == 'buy' else -trade_usdt

        self.tick_buffer.append((ts, cvd_delta))
        self._clean_tick_buffer(ts)

        if not self.is_detecting:
            if ts < self.cooldown_until_ts:
                return None

            triggered, direction = self._check_trigger_condition()
            if triggered:
                self._open_window(ts, price, direction)
        else:
            # 增加对“动能衰竭”原因的捕获
            if (ts - self.window_last_trade_ts) >= self.exhaustion_sec:
                self.last_close_reason = "⏳ 动能衰竭(超时)"  # <--- 新增
                return self._close_window_and_detect()

            self._update_observation(ts, price, cvd_delta)

            # 修改 _check_close_condition 使其返回原因
            stop_signal, reason = self._check_close_condition(ts, price)
            if stop_signal:
                self.last_close_reason = reason  # <--- 新增
                return self._close_window_and_detect()

        return None

    def _clean_tick_buffer(self, current_ts: float):
        while self.tick_buffer and (current_ts - self.tick_buffer[0][0]) > self.trigger_window_sec:
            self.tick_buffer.popleft()

    def _check_trigger_condition(self) -> Tuple[bool, Optional[str]]:
        """双向点火检查"""
        if not self.tick_buffer: return False, None
        recent_net_cvd = sum(delta for _, delta in self.tick_buffer)

        if recent_net_cvd <= self.trigger_buy_cvd:
            return True, 'BUY'  # 寻找做多的冰山
        elif recent_net_cvd >= self.trigger_sell_cvd:
            return True, 'SELL'  # 寻找做空的冰山

        return False, None

    def _open_window(self, ts: float, current_price: float, direction: str):
        self.is_detecting = True
        self.current_direction = direction
        self.detect_start_ts = ts
        self.start_price = current_price

        # 双向快照必须同时拍下
        self.start_bids_snapshot = copy.deepcopy(self.ctx.bids)
        self.start_asks_snapshot = copy.deepcopy(self.ctx.asks)

        self.traded_min_price = current_price
        self.traded_max_price = current_price
        self.window_cvd_usdt = 0.0
        self.window_recent_cvd = 0.0
        self.window_last_trade_ts = ts

        trigger_cvd = sum(delta for _, delta in self.tick_buffer)
        icon = "🔥" if direction == 'BUY' else "🚀"
        target = "多头底" if direction == 'BUY' else "空头顶"
        logger.info(
            f"{icon} [流速点火] 寻找{target}! 3秒CVD: {trigger_cvd:,.0f} U | 触碰: {current_price} | 时间戳: {ts}")

    def _update_observation(self, ts: float, price: float, cvd_delta: float):
        self.window_last_trade_ts = ts
        self.window_cvd_usdt += cvd_delta

        if self.current_direction == 'BUY':
            if price < self.traded_min_price:
                self.traded_min_price = price
                self.window_recent_cvd = 0.0  # 砸出新低，反弹重置
            else:
                self.window_recent_cvd += cvd_delta
        else:  # SELL (找顶部阻力)
            if price > self.traded_max_price:
                self.traded_max_price = price
                self.window_recent_cvd = 0.0  # 冲出新高，砸盘动能重置
            else:
                self.window_recent_cvd += cvd_delta  # 否则正常累加动能

        # 始终维护极值
        if price < self.traded_min_price: self.traded_min_price = price
        if price > self.traded_max_price: self.traded_max_price = price

    def _check_close_condition(self, ts: float, current_price: float) -> Tuple[bool, str]:
        """修改返回值为 (是否关窗, 原因)"""
        if self.current_direction == 'BUY':
            if self.window_recent_cvd >= self.reversal_cvd_usdt:
                return True, "📈 多头反击(CVD翻转)"
            if (self.start_price - current_price) / self.start_price >= self.max_drop_pct:
                return True, "📉 跌幅过大(防穿仓)"
        else:
            if self.window_recent_cvd <= -self.reversal_cvd_usdt:
                return True, "📉 空头压制(CVD翻转)"
            if (current_price - self.start_price) / self.start_price >= self.max_drop_pct:
                return True, "📈 涨幅过大(避险)"
        return False, ""

    def _close_window_and_detect(self) -> Optional[Dict]:
        start_thickness_usdt = 0.0
        end_thickness_usdt = 0.0
        book_reduction = 0.0
        signal = None

        if self.current_direction == 'BUY':
            # 抓底部冰山，遍历 Bids 消耗
            for p, vol in self.start_bids_snapshot.items():
                if self.traded_min_price <= p <= self.traded_max_price: start_thickness_usdt += (p * vol)
            for p, vol in self.ctx.bids.items():
                if self.traded_min_price <= p <= self.traded_max_price: end_thickness_usdt += (p * vol)

            book_reduction = start_thickness_usdt - end_thickness_usdt
            active_sold = abs(self.window_cvd_usdt)
            signal = self.iceberg_radar.detect_buy_iceberg(active_sold, book_reduction)
            signal['direction'] = 'BUY'
            signal['min_price'] = self.traded_min_price

        else:
            # 抓顶部冰山，遍历 Asks 消耗
            for p, vol in self.start_asks_snapshot.items():
                if self.traded_min_price <= p <= self.traded_max_price: start_thickness_usdt += (p * vol)
            for p, vol in self.ctx.asks.items():
                if self.traded_min_price <= p <= self.traded_max_price: end_thickness_usdt += (p * vol)

            book_reduction = start_thickness_usdt - end_thickness_usdt
            active_bought = abs(self.window_cvd_usdt)  # 这里是总买入量

            # 注意：如果你的 radar 里没写 detect_sell_iceberg，
            # 可以直接复用 detect_buy_iceberg，因为数学公式（吸收率）是一模一样的！
            signal = self.iceberg_radar.detect_buy_iceberg(active_bought, book_reduction)
            signal['direction'] = 'SELL'
            signal['max_price'] = self.traded_max_price  # 顶部冰山关注的是最高价

        # 👇【修改】：在调用 logger 之前，先从 signal 里把核心数据掏出来
        hidden_vol = signal.get('hidden_volume', 0)
        abs_rate = signal.get('absorption_rate', 0) * 100
        is_iceberg = signal.get('is_iceberg', False)
        direction = "多头" if self.current_direction == 'BUY' else "空头"

        # 核心：使用统一的标签前缀 [MATCH] 方便你后续用 grep 过滤
        tag = f"🎯 [MATCH-{direction}]" if is_iceberg else "❌ [IGNORE]"

        logger.info(f"{tag} 原因: {self.last_close_reason} | 耗时: {self.window_last_trade_ts - self.detect_start_ts:.2f}s | "
                    f"战火区: [{self.traded_min_price}, {self.traded_max_price}] | "
                    f"总攻击: {abs(self.window_cvd_usdt):,.0f} U | 明面盘口消耗: {book_reduction:,.0f} U | "
                    f"暗盘吸收量: {hidden_vol:,.0f} U (吸收率 {abs_rate:.1f}%)")

        self.is_detecting = False
        self.tick_buffer.clear()

        # （phase1_engine.py 的结尾附近）

        # 👇【修改】：在 _close_window_and_detect 结尾追加耗时计算
        duration = self.window_last_trade_ts - self.detect_start_ts
        signal['duration'] = duration  # 🌟 将耗时注入 signal 提供给 main.py

        if signal['is_iceberg'] or signal['behavior'] == 'SPOOFING_WITHDRAWAL':
            self.cooldown_until_ts = self.window_last_trade_ts + 2.5
            return signal
        else:
            self.cooldown_until_ts = self.window_last_trade_ts + 0.1
            return None