import asyncio
import json
import websockets

from src.utils.log import get_logger

logger = get_logger(__name__)

class OKXBooksStreamer:
    def __init__(self, symbol="ETH-USDT-SWAP", on_book_callback=None):
        self.symbol = symbol
        # 核心：通过回调把订单簿增量数据抛给 MarketContext
        self.on_book_callback = on_book_callback 
        # 保持和你 okx_stream 一致的域名
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"

    async def connect(self):
        """建立 WebSocket 连接，保持心跳与断线重连 (专属订单簿通道)"""
        subscribe_msg = {
            "op": "subscribe",
            "args": [{"channel": "books", "instId": self.symbol}]
        }

        while True:
            try:
                logger.info(f"📚 [数据层] 正在连接 OKX 订单簿专属通道 ({self.symbol})...")
                async with websockets.connect(self.ws_url) as ws:
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info("✅ [数据层] 订单簿接入成功！持续监听 400 档盘口深度...")

                    while True:
                        response = await ws.recv()
                        data = json.loads(response)

                        # OKX 订单簿推送只要包含 'data'，就传给下游字典
                        if 'data' in data and self.on_book_callback:
                            # 订单簿的 data 列表里通常只有一个字典，包含了 bids 和 asks
                            book_data = data['data'][0]
                            
                            # 因为 context.apply_book_delta 是普通同步函数，所以直接调用，不需要 await
                            self.on_book_callback(book_data)

            except Exception as e:
                logger.error(f"❌ [数据层] 订单簿链路断开，准备 3 秒后重连: {e}")
                await asyncio.sleep(3)
