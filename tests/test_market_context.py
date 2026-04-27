#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 4/25/26 10:26 AM
@File       : test_market_context.py
@Description: 
"""
import asyncio
import os
import sys

# 确保能导入 src 目录
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.context.market_context import MarketContext
from src.data_feed.okx_stream import OKXTickStreamer
from src.data_feed.okx_books_stream import OKXBooksStreamer
from src.utils.log import get_logger

logger = get_logger(__name__)


async def soak_test_monitor(context: MarketContext):
    """
    机构级浸泡测试监控器：不仅打印状态，还负责抓捕内存泄漏
    """
    logger.info("🧪 [浸泡测试] 监控器已启动，每 10 秒输出一次健康报告...")

    while True:
        await asyncio.sleep(10)  # 测试期间每10秒看一次，避免刷屏

        bid_len = len(context.bids)
        ask_len = len(context.asks)

        logger.info("=" * 50)
        logger.info(f"⏱️ [心跳检测] 最新价格: {context.current_price:.2f} | CVD: {context.current_cvd:.2f}")
        logger.info(f"📊 [盘口健康度] Bids: {bid_len}档 | Asks: {ask_len}档")
        logger.info(
            f"📦 [微观动能] 等量K线缓存数: {len(context.volume_bars)}/100 | 当前累积: {context.current_vol_accumulator:.2f}")
        logger.info(
            f"🎯 [现存防线] 支撑(SSL): {list(context.sell_side_liquidity.keys())} | 阻力(BSL): {list(context.buy_side_liquidity.keys())}")

        # ---------------- 核心检查逻辑 ----------------
        # 1. 检查订单簿是否发散 (OKX 默认推 400 档，如果超过 450 绝对有问题)
        if bid_len > 450 or ask_len > 450:
            logger.error("🚨 [内存告警] 盘口字典体积异常膨胀！可能发生丢包导致的撤单失效！")

        # 2. 检查订单簿是否死机 (如果小于 10 档，说明没连上或者被清空了)
        if bid_len < 10 and context.current_price > 0:
            logger.error("🚨 [数据告警] 盘口干涸！WebSocket 可能已断开但未重连！")

        logger.info("=" * 50)
        

async def run_test():
    # 从你的系统环境变量或 .env 中读取参数 (如果没有则回退到 150 万)
    # 测试期间，为了能更快看到效果，你可以手动把这里的 fallback 值改小一点，比如 300_000.0 (30万U)
    notional_threshold = float(os.getenv("TARGET_NOTIONAL_USDT", 300_000.0))
    
    # 1. 实例化 (使用新的参数名)
    context = MarketContext(target_notional_usdt=notional_threshold)
    logger.info(f"⚙️ 系统已初始化，当前 K 线名义价值门槛: {notional_threshold:,.0f} USDT")

    # 塞入测试防线
    context.add_liquidity_level(3000.0, 'SSL') 
    context.add_liquidity_level(3200.0, 'BSL')

    # 2. 回调对接
    # 修复：因为 okx_stream.py 里使用了 await，这里必须显式定义为 async 函数
    async def async_trade_callback(data):
        context.apply_trade(data)

    trade_streamer = OKXTickStreamer(on_tick_callback=async_trade_callback)

    # 因为 okx_books_stream.py 里没有用 await，所以它用同步 lambda 没问题
    book_streamer = OKXBooksStreamer(on_book_callback=lambda data: context.apply_book_delta(data))

    # 3. 启动！
    logger.info("🚀 [浸泡测试] 正在启动双通道数据流引擎...")
    try:
        await asyncio.gather(
            trade_streamer.connect(),
            book_streamer.connect(),
            soak_test_monitor(context)
        )
    except KeyboardInterrupt:
        logger.info("🛑 [浸泡测试] 手动终止。")


if __name__ == "__main__":
    # Windows 环境下如果报错 EventLoop 错误，可以加上这一句
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_test())
