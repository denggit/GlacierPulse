#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 4/28/2026
@File       : iceberg_detector.py
@Description: 机构级冰山订单检测器 (基于绝对隐藏成交量与动能吸收率)
"""

from typing import Dict, Union

class IcebergDetector:
    """
    冰山订单数学检测核心 (V2 绝对成交量版)。
    
    物理意义：
    隐藏成交量 = 实际发生的主动成交额 - 盘口对应方向挂单的真实减少量。
    - 如果 > 0：说明有未挂在明面上的暗单（冰山）参与了吃货。
    - 如果 = 0：说明盘口是全透明的，砸多少就消耗多少明牌挂单。
    - 如果 < 0：说明盘口减少的量比实际砸的量还大，主力在“撤单（Spoofing）”跑路。
    """
    
    def __init__(self, 
                 min_hidden_notional_usdt: float = 1_000_000.0, 
                 min_absorption_rate: float = 0.5):
        """
        :param min_hidden_notional_usdt: 最小绝对隐藏吸收金额 (默认 100万 USDT，过滤散户噪音)
        :param min_absorption_rate: 动能吸收率。隐藏吸收量占总砸盘量的比例 (默认 0.5，即至少 50% 的抛压是被暗盘接走的)
        """
        self.min_hidden_notional = min_hidden_notional_usdt
        self.min_absorption_rate = min_absorption_rate

    def detect_buy_iceberg(self, active_sold_notional: float, bid_book_reduction: float) -> Dict[str, Union[bool, float, str]]:
        """
        检测【底部买盘冰山】(抵御暴跌砸盘)
        :param active_sold_notional: 观测窗内，实际发生的市价卖出总金额 (必须 > 0)
        :param bid_book_reduction: 观测窗内，买一侧 (Bids) 对应区间挂单厚度的减少量
        """
        signal = self._calculate_microstructure(active_sold_notional, bid_book_reduction)
        signal["type"] = "BUY_ICEBERG" if signal["is_iceberg"] else "NONE"
        return signal

    def detect_sell_iceberg(self, active_bought_notional: float, ask_book_reduction: float) -> Dict[str, Union[bool, float, str]]:
        """
        检测【顶部卖盘冰山】(抵御 FOMO 追高)
        :param active_bought_notional: 观测窗内，实际发生的市价买入总金额 (必须 > 0)
        :param ask_book_reduction: 观测窗内，卖一侧 (Asks) 对应区间挂单厚度的减少量
        """
        signal = self._calculate_microstructure(active_bought_notional, ask_book_reduction)
        signal["type"] = "SELL_ICEBERG" if signal["is_iceberg"] else "NONE"
        return signal

    def _calculate_microstructure(self, active_notional: float, book_reduction: float) -> Dict[str, Union[bool, float, str]]:
        """微观结构核心计算引擎"""
        if active_notional <= 0:
            return self._empty_signal()

        # 核心公式：绝对隐藏成交量
        hidden_volume = active_notional - book_reduction
        
        # 核心比例：有多少比例的市价单是被“暗中”吃掉的？
        # 彻底消灭除零错误，因为 active_notional 必然大于 0 (触发条件决定)
        absorption_rate = hidden_volume / active_notional

        # 盘口行为定性判定
        behavior = "NORMAL"
        if hidden_volume < - (active_notional * 0.2): 
            # 盘口消失的量，比砸盘的量大了 20% 以上，典型的挂假墙后撤单跑路
            behavior = "SPOOFING_WITHDRAWAL"
        elif hidden_volume > 0:
            behavior = "ICEBERG_ABSORPTION"

        # 冰山判定门槛：绝对体量够大，且占据了抛压的主导权
        is_iceberg = (hidden_volume >= self.min_hidden_notional) and (absorption_rate >= self.min_absorption_rate)

        # 信心指数计算 (0.0 ~ 1.0)
        confidence = 0.0
        if is_iceberg:
            # 体量得分：超过阈值越多分数越高 (最高满分 1.0)
            vol_score = min(1.0, hidden_volume / (self.min_hidden_notional * 2.5))
            # 比例得分：如果吸收率高达 90% 以上直接满分，踩线 50% 给及格分
            rate_score = min(1.0, absorption_rate / 0.9) 
            # 综合加权 (体量更重要，占 60%)
            confidence = (vol_score * 0.6) + (rate_score * 0.4)

        return {
            "is_iceberg": is_iceberg,
            "type": "NONE", 
            "behavior": behavior,             # 新增：向外暴露出盘口定性行为
            "confidence": round(confidence, 4),
            "hidden_volume": round(hidden_volume, 2),  # 真正的冰山体量
            "absorption_rate": round(absorption_rate, 4), # 被暗盘吃掉的百分比
            "active_volume": round(active_notional, 2),
            "book_reduction": round(book_reduction, 2)
        }

    def _empty_signal(self) -> Dict[str, Union[bool, float, str]]:
        return {
            "is_iceberg": False,
            "type": "NONE",
            "behavior": "IDLE",
            "confidence": 0.0,
            "hidden_volume": 0.0,
            "absorption_rate": 0.0,
            "active_volume": 0.0,
            "book_reduction": 0.0
        }

if __name__ == "__main__":
    # === 实盘极限微观场景推演 ===
    detector = IcebergDetector(min_hidden_notional_usdt=1_000_000.0, min_absorption_rate=0.5)
    
    print("场景 1：【明牌天量墙（假冰山）】")
    # 盘面挂了 1000 万。散户砸了 500 万，盘口真真实实少了 500 万。没有任何隐藏吃单。
    print(detector.detect_buy_iceberg(active_sold_notional=5_000_000, bid_book_reduction=5_000_000))
    print("-" * 60)
    
    print("场景 2：【真正的巨型冰山（暗中托底）】")
    # 盘面只挂了 100 万。散户砸了 500 万，但盘口不仅没被击穿，反而只少了 50 万！
    print(detector.detect_buy_iceberg(active_sold_notional=5_000_000, bid_book_reduction=500_000))
    print("-" * 60)
    
    print("场景 3：【撤单欺诈（Spoofing 骗炮跑路）】")
    # 盘面挂了 800 万的假墙。散户刚刚试探性地砸了 100 万，主力立刻撤掉所有买单，导致盘口瞬间减少了 800 万。
    print(detector.detect_buy_iceberg(active_sold_notional=1_000_000, bid_book_reduction=8_000_000))
    print("-" * 60)

    print("场景 4：【超级反击（逆向挂单）】")
    # 散户砸了 300 万，主力不仅全盘接下，还往盘口上新增了 100 万的明牌挂单 (reduction 为负数)。
    print(detector.detect_buy_iceberg(active_sold_notional=3_000_000, bid_book_reduction=-1_000_000))
