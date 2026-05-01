#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@File       : main.py
@Description: GlacierPulse 实盘主启动程序 (修正传参及异步逻辑)
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

logger = get_logger("Main")


async def main():
    logger.info("==================================================")
    logger.info("🚀 GlacierPulse 探路先锋系统正在启动...")
    logger.info("==================================================")

    symbol = "ETH-USDT-SWAP"

    # 1. 实例化核心组件
    ctx = MarketContext()
    iceberg_radar = IcebergDetector()
    engine = Phase1Engine(market_context=ctx, iceberg_detector=iceberg_radar)
    trader = IcebergTrader(symbol=symbol, leverage=10, td_mode="cross")

    # 2. 定义高频数据回调处理链路 (Callback Pipeline)

    # 【处理逐笔成交】：必须是 async def，因为 okx_stream 中使用了 await 回调
    # 在函数外部定义一个全局变量，防止并发双开
    trader_task = None

    # 在函数外部定义一个全局变量，防止并发双开
    trader_task = None

    async def on_trade_tick(trade_data):
        nonlocal trader_task
        signal = engine.process_tick(trade_data)

        if signal:
            is_iceberg = signal.get('is_iceberg', False)
            direction = signal.get('direction', 'BUY')

            if is_iceberg:
                direction_label = "多" if direction == 'BUY' else "空"
                abs_rate = signal.get('absorption_rate', 0) * 100
                conf = signal.get('confidence', 0)

                # 👇 【修正】：把名字从 total_attack 改为 active_volume
                actual_attack = abs(signal.get('active_volume', 0))

                # ✨ 【优化过滤】：
                if conf < 0.8 or actual_attack < 1_000_000:
                    logger.info(f"⏭️ [信号过滤] 确信度({conf:.2f})或攻击量({actual_attack:,.0f}U)不足，放弃捕捉。")
                    return

                # ✨ 【高亮逻辑】：吸收率 >= 100% 且确信度高，显示为金黄色加粗特效
                if abs_rate >= 100 and conf >= 0.9:
                    highlight_start = "\033[1;33;44m"  # 蓝底金黄字
                    highlight_end = "\033[0m"
                else:
                    highlight_start = ""
                    highlight_end = ""

                logger.info(f"{highlight_start}🎯 [捕获冰山 ({direction_label})] 确信度: {conf:.2f} | "
                            f"隐藏体量: {signal['hidden_volume']:,.0f} U | 吸收率: {abs_rate:.1f}%{highlight_end}")

            elif signal.get('behavior') == 'SPOOFING_WITHDRAWAL':
                logger.warning(
                    f"⚠️ [撤单欺诈!] 主力撤销了假墙！虚假支撑消失量: {abs(signal.get('hidden_volume', 0)):,.0f} U")
                return
            else:
                return

                # 只有在 is_iceberg == True 或者持仓状态下的反向信号才允许下发给 Trader
            if trader_task and not trader_task.done():
                return

            current_price = float(trade_data['price'])
            trader_task = asyncio.create_task(trader.process_signal(signal, current_price))

    # 【处理盘口更新】：必须是同步 def，因为 okx_books_stream 中是普通调用
    def on_book_update(book_data):
        """将盘口增量数据喂给 MarketContext"""
        try:
            ctx.apply_book_delta(book_data)
        except Exception as e:
            logger.error(f"❌ 处理 Book 时发生错误: {e}")

    # 3. 启动数据流与后台探针

    # 启动订单簿流：使用 on_book_callback 参数
    books_stream = OKXBooksStreamer(symbol=symbol, on_book_callback=on_book_update)
    asyncio.create_task(books_stream.connect())
    logger.info("📡 [数据流] OKX 订单簿 (Books) WebSocket 任务已拉起。")

    # 给订单簿一点点初始化时间
    await asyncio.sleep(1)

    # 启动逐笔成交流：使用 on_tick_callback 参数
    trade_stream = OKXTickStreamer(symbol=symbol, on_tick_callback=on_trade_tick)
    asyncio.create_task(trade_stream.connect())
    logger.info("📡 [数据流] OKX 逐笔成交 (Trades) WebSocket 任务已拉起。")

    # 启动 Trader 的后台财务官探针 (实时查账、同步仓位)
    asyncio.create_task(trader.update_balance_loop())

    logger.info("==================================================")
    logger.info("🟢 系统初始化完成，已进入最高警戒深海潜航状态！")
    logger.info("==================================================")

    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("🛑 收到停止信号，系统正在安全下线...")
        sys.exit(0)

    # ---------------------------------------------------------
    # 4. 保持主程序永久运行
    # ---------------------------------------------------------
    try:
        # 创建一个永远不会被 set 的 Event，让程序挂起并保持监听
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        # 【修复】：正确捕获 asyncio 取消信号
        logger.info("🛑 收到系统关闭指令，正在取消所有后台任务并安全下线...")
        # 实际实盘中，这里还可以调用 await trader._request("POST", "/api/v5/trade/cancel-all-after", ...) 做一键撤单

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[系统退出] 用户手动终止进程。")