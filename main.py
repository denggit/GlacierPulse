#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@File       : main.py
@Description: GlacierPulse 实盘主启动程序 (20U 探路先锋版)
"""

import asyncio
import sys
import os

# 确保能正确导入 src 模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.utils.log import get_logger
from src.context.market_context import MarketContext
from src.detectors.iceberg_detector import IcebergDetector
from src.strategy.phase1_engine import Phase1Engine
from src.execution.trader import IcebergTrader
from src.data_feed.okx_books_stream import OKXBooksStreamer
from src.data_feed.okx_stream import OKXTickStreamer

logger = get_logger("GlacierPulse")


async def main():
    logger.info("==================================================")
    logger.info("🚀 GlacierPulse 探路先锋系统正在启动...")
    logger.info("==================================================")

    symbol = "ETH-USDT-SWAP"

    # ---------------------------------------------------------
    # 1. 实例化核心组件
    # ---------------------------------------------------------
    # 数据上下文与雷达
    ctx = MarketContext()
    iceberg_radar = IcebergDetector()

    # 策略引擎 (双向全天候雷达)
    engine = Phase1Engine(market_context=ctx, iceberg_detector=iceberg_radar)

    # 实盘交易执行器 (10X杠杆，动态资金管理)
    trader = IcebergTrader(symbol=symbol, leverage=10, td_mode="cross")

    # ---------------------------------------------------------
    # 2. 定义高频数据回调处理链路 (Callback Pipeline)
    # ---------------------------------------------------------
    # 【修复 1】：必须是 async def，因为 okx_stream 里面用了 await 回调
    async def on_trade_tick(trade_data):
        """每当有真实成交发生时触发"""
        # 1. 把数据喂给引擎进行检测
        signal = engine.process_tick(trade_data)

        # 2. 如果引擎吐出了信号，立即异步交由 Trader 处理
        if signal:
            current_price = float(trade_data['price'])
            # 使用 asyncio.create_task 确保交易逻辑不阻塞底层 WebSocket 接收
            asyncio.create_task(trader.process_signal(signal, current_price))

    # ---------------------------------------------------------
    # 3. 启动数据流与后台探针
    # ---------------------------------------------------------
    # 启动全量深度盘口流 (负责维护 Bids 和 Asks)
    books_stream = OKXBooksStreamer(symbol=symbol, context=ctx)
    # 【修复 3】：不能用 await，必须丢进后台任务并发执行，否则死循环阻塞
    asyncio.create_task(books_stream.connect())
    logger.info("📡 [数据流] OKX 订单簿 (Books) WebSocket 任务已拉起。")

    # 给订单簿一点点初始化时间，避免先收到 Trades 报错
    await asyncio.sleep(1)

    # 启动逐笔成交流 (负责驱动引擎和触发回调)
    # 【修复 2】：参数名严格对齐 okx_stream.py 里的 on_tick_callback
    trade_stream = OKXTickStreamer(symbol=symbol, on_tick_callback=on_trade_tick)
    asyncio.create_task(trade_stream.connect())
    logger.info("📡 [数据流] OKX 逐笔成交 (Trades) WebSocket 任务已拉起。")

    # 启动 Trader 的后台财务官探针 (实时查账、同步仓位状态)
    asyncio.create_task(trader.update_balance_loop())

    logger.info("==================================================")
    logger.info("🟢 系统初始化完成，已进入最高警戒深海潜航状态！")
    logger.info("==================================================")

    # ---------------------------------------------------------
    # 4. 保持主程序永久运行
    # ---------------------------------------------------------
    try:
        # 创建一个永远不会被 set 的 Event，让程序挂起并保持监听
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("🛑 收到停止信号，系统正在安全下线...")
        # 此处可添加优雅断开 WS 连接的代码
        sys.exit(0)


if __name__ == "__main__":
    # 配置 Windows 下的 Asyncio 事件循环策略 (防止部分环境报错)
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[系统退出] 用户手动终止进程。")