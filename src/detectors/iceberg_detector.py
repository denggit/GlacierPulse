#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 4/17/2026 11:08 PM
@File       : iceberg_detector.py
@Description: 冰山订单检测器 (吸收率计算核心)
"""

from typing import Dict, Union

class IcebergDetector:
    """
    冰山订单数学检测核心。
    基于假设：真实的抛压如果遇到正常的流动性，盘口厚度会同比例减少。
    如果抛压极大，但盘口厚度几乎没变（甚至增加），说明有隐藏的限价买单（冰山）在疯狂吃货。
    """
    
    def __init__(self, 
                 min_absorption_ratio: float = 3.0, 
                 min_absorbed_notional: float = 1_000_000.0):
        """
        :param min_absorption_ratio: 最小吸收倍数阈值 (默认 3.0，即砸了3块钱，盘口才少了1块钱)
        :param min_absorbed_notional: 最小绝对吸收金额 (USDT)，过滤散户级别的假冰山
        """
        self.min_absorption_ratio = min_absorption_ratio
        self.min_absorbed_notional = min_absorbed_notional

    def detect_buy_iceberg(self, 
                           cvd_sold_notional: float, 
                           bid_book_reduction: float) -> Dict[str, Union[bool, float]]:
        """
        检测是否有【买盘冰山】在托底（应对暴跌砸盘）
        
        :param cvd_sold_notional: 观察窗口内，累计的主动卖出（市价砸盘）金额 (必须是正数)
        :param bid_book_reduction: 同一窗口内，买盘(Bids)挂单实际减少的金额
        :return: 结构化信号字典
        """
        # 如果根本没有砸盘，或者传入的数据异常，直接返回否定信号
        if cvd_sold_notional <= 0:
            return self._empty_signal()

        # 核心逻辑 1：处理极端冰山情况 (盘口根本没减少，或者因为有人疯狂挂单反而增加了)
        if bid_book_reduction <= 0:
            absorption_ratio = float('inf')
            absorbed_volume = cvd_sold_notional  # 砸出来的货被 100% 隐藏吃掉了
        else:
            # 核心逻辑 2：计算吸收倍数和绝对吸收量
            absorption_ratio = cvd_sold_notional / bid_book_reduction
            absorbed_volume = cvd_sold_notional - bid_book_reduction

        # 判断是否同时满足【相对比例】和【绝对体量】的双重标准
        is_iceberg = (absorption_ratio >= self.min_absorption_ratio) and \
                     (absorbed_volume >= self.min_absorbed_notional)

        # 核心逻辑 3：计算信心指数 (Confidence Score) 0.0 ~ 1.0
        confidence = 0.0
        if is_iceberg:
            # 比例得分：如果达到了阈值的 2 倍及以上，拿满 60% 的权重分数
            ratio_score = min(1.0, absorption_ratio / (self.min_absorption_ratio * 2.0)) if absorption_ratio != float('inf') else 1.0
            # 体量得分：如果吸收量达到了阈值的 2 倍及以上，拿满 40% 的权重分数
            vol_score = min(1.0, absorbed_volume / (self.min_absorbed_notional * 2.0))
            
            confidence = (ratio_score * 0.6) + (vol_score * 0.4)

        return {
            "is_iceberg": is_iceberg,
            "confidence": round(confidence, 4),
            "absorption_ratio": round(absorption_ratio if absorption_ratio != float('inf') else 999.9, 2),
            "absorbed_volume": round(absorbed_volume, 2),
            "cvd_sold": round(cvd_sold_notional, 2),
            "book_reduction": round(bid_book_reduction, 2)
        }

    def _empty_signal(self) -> Dict[str, Union[bool, float]]:
        return {
            "is_iceberg": False,
            "confidence": 0.0,
            "absorption_ratio": 0.0,
            "absorbed_volume": 0.0,
            "cvd_sold": 0.0,
            "book_reduction": 0.0
        }

if __name__ == "__main__":
    # === 本地数学逻辑极速测试 ===
    detector = IcebergDetector(min_absorption_ratio=3.0, min_absorbed_notional=1_000_000.0)
    
    print("测试案例 1：标准的猛烈砸盘（无冰山，正常穿透）")
    # 砸了 200万，盘口真真切切少了 180万
    print(detector.detect_buy_iceberg(cvd_sold_notional=2_000_000, bid_book_reduction=1_800_000))
    print("-" * 50)
    
    print("测试案例 2：教科书级的冰山托底（完美吸收）")
    # 砸了 500万，但盘口极其坚挺，只消耗了 50万的厚度 (10倍吸收)
    print(detector.detect_buy_iceberg(cvd_sold_notional=5_000_000, bid_book_reduction=500_000))
    print("-" * 50)
    
    print("测试案例 3：极端冰山反击（买盘不仅没少，还在增加）")
    # 砸了 300万，但由于暗冰山不仅吃货还往上顶，导致买盘厚度反而增加了 20万 (reduction 为负数)
    print(detector.detect_buy_iceberg(cvd_sold_notional=3_000_000, bid_book_reduction=-200_000))
