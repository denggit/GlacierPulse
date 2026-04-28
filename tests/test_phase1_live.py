#!/usr/bin/env python
# -*- coding: utf-8 -*-
# tests/test_phase1_live.py

import asyncio
import os
import sys

# 确保能导入 src 目录
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging

from src.data_feed.okx_books_stream import OKXBooksStreamer
from src.context.market_context import MarketContext
from src.detectors.iceberg_detector import IcebergDetector
from src.strategy.phase1_engine import Phase1Engine

# ==========================================
# 极简日志配置：只看 Info (业务日志) 和 Error (报错)
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# 屏蔽底层 websocket 烦人的连接日志
logging.getLogger('websockets').setLevel(logging.WARNING)

async def main():
    load_dotenv()
    
    # 1. 初始化底座 (它在后台默默切分150万U的K线，但不会再打印了)
    target_notional = float(os.getenv("TARGET_NOTIONAL_USDT", 1_500_000.0))
    ctx = MarketContext(target_notional_usdt=target_notional)
    
    # 2. 初始化雷达 (隐藏吸收大于 100万U，且吸收率 > 50%)
    detector = IcebergDetector(
        min_hidden_notional_usdt=1_000_000.0, 
        min_absorption_rate=0.5
    )
    
    # 3. 初始化点火引擎
    engine = Phase1Engine(market_context=ctx, iceberg_detector=detector)
    
    # 💡 [实盘测试小贴士]：由于测试期间很难立刻碰到 800万 级别的连环爆仓，
    # 建议你可以先把它改成 -300万 测一下系统的敏锐度，确认能抓到后，再改回 -800万。
    engine.trigger_cvd_usdt = -3_000_000.0  
    
    logger.info("🚀 Phase 1 冰山猎手已启动 | 保持静默运行，只播报异常与猎物...")

    # 4. 定义数据流回调
    async def handle_trade(trade_data):
        try:
            # 1. 底座记账
            ctx.apply_trade(trade_data)
            
            # 2. 引擎查流速
            signal = engine.process_tick(trade_data)
            
            # 3. 如果关窗结算并抓到了猎物
            if signal:
                if signal['is_iceberg']:
                    logger.info(f"🎯 [捕获冰山!] 确信度: {signal['confidence']:.2f} | "
                                f"隐藏体量: {signal['hidden_volume']:,.0f} U | "
                                f"吸收率: {signal['absorption_rate']:.1%}")
                elif signal['behavior'] == 'SPOOFING_WITHDRAWAL':
                    logger.warning(f"⚠️ [撤单欺诈!] 主力撤销了假墙！"
                                   f"虚假支撑消失量: {abs(signal['hidden_volume']):,.0f} U")
        except Exception as e:
            logger.error(f"❌ 处理 Trade 时发生错误: {e}", exc_info=True)

    async def handle_book(book_data):
        try:
            ctx.apply_book_delta(book_data)
        except Exception as e:
            logger.error(f"❌ 处理 Book 时发生错误: {e}", exc_info=True)

    # 5. 启动双流通道
    trade_streamer = OKXTickStreamer(callback=handle_trade)
    book_streamer = OKXBooksStreamer(callback=handle_book)
    
    # 异步并发：先启动订单簿构建底座，再接入逐笔成交
    asyncio.create_task(book_streamer.start())
    await asyncio.sleep(1) # 给订单簿 1 秒的初始化时间
    asyncio.create_task(trade_streamer.start())

    # 挂起主线程，让它天荒地老地跑下去
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 测试手动终止。")
