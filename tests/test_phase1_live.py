#!/usr/bin/env python
# -*- coding: utf-8 -*-
# tests/test_phase1_live.py

import asyncio
import os
import sys

# 确保能导入 src 目录
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from dotenv import load_dotenv

from src.data_feed.okx_stream import OKXTickStreamer
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
    
    # 1. 初始化底座
    target_notional = float(os.getenv("TARGET_NOTIONAL_USDT", 1_500_000.0))
    ctx = MarketContext(target_notional_usdt=target_notional)
    
    # 2. 初始化雷达
    detector = IcebergDetector(
        min_hidden_notional_usdt=1_000_000.0, 
        min_absorption_rate=0.5
    )
    
    # 3. 初始化点火引擎
    engine = Phase1Engine(market_context=ctx, iceberg_detector=detector)
    
    engine.trigger_cvd_usdt = -3_000_000.0  
    
    logger.info("🚀 Phase 1 冰山猎手已启动 | 保持静默运行，只播报异常与猎物...")

    # 4. 定义数据流回调
    # [修复] handle_trade 保持 async，因为 okx_stream 中使用了 await
    async def handle_trade(trade_data):
        try:
            ctx.apply_trade(trade_data)
            signal = engine.process_tick(trade_data)
            
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

    # [修复] 去掉了 async，因为 okx_books_stream 中是普通同步调用
    def handle_book(book_data):
        try:
            ctx.apply_book_delta(book_data)
        except Exception as e:
            logger.error(f"❌ 处理 Book 时发生错误: {e}", exc_info=True)

    # 5. 启动双流通道
    # [修复] 修改了参数名，与类定义保持一致
    trade_streamer = OKXTickStreamer(on_tick_callback=handle_trade)
    book_streamer = OKXBooksStreamer(on_book_callback=handle_book)
    
    # [修复] 启动方法名由 start() 改为 connect()
    asyncio.create_task(book_streamer.connect())
    await asyncio.sleep(1) # 给订单簿 1 秒的初始化时间
    asyncio.create_task(trade_streamer.connect())

    # 挂起主线程，让它天荒地老地跑下去
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 测试手动终止。")
