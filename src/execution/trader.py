#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@File       : trader.py
@Description: 机构级冰山订单流执行器 (全动态余额感知 + OKX 稳健基建 + 限价止盈优化)
"""
import asyncio
import base64
import datetime
import hmac
import json
import math
import os
import sys

import requests

current_file = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(current_file))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.utils.log import get_logger
from config.env_loader import OKX_CONFIG
from typing import Optional, Dict, Any

logger = get_logger(__name__)


class IcebergTrader:
    def __init__(self, symbol="ETH-USDT-SWAP", leverage=5, td_mode="cross"):
        # API 基建
        self.symbol = symbol
        self.api_key = OKX_CONFIG.get('api_key')
        self.secret_key = OKX_CONFIG.get('secret_key')
        self.passphrase = OKX_CONFIG.get('passphrase')
        self.base_url = "https://www.okx.com"

        # 合约基础属性
        self.contract_multiplier = 0.1  # ETH-USDT-SWAP 一张 = 0.1 ETH
        self.td_mode = td_mode
        self.leverage = leverage

        # ==================================
        # 探路策略配置 (去除了写死的资金量)
        # ==================================
        self.max_risk_pct = 0.05  # 单笔极限亏损 5% (按当前真实余额算)
        self.fee_rate = 0.001  # 往返总手续费 0.1%
        self.sl_tick_offset = 1.51  # 止损避险偏移 (整数向下再减 1.51)
        self.tp_pct = 0.005  # 固定止盈目标 (+0.5%)
        self.trailing_trigger_pct = 0.002  # 追踪触发阈值 (+0.2%)

        # 动态账户与持仓状态机
        self.available_usdt = 0.0  # 🌟 动态读取的账户可用 USDT 余额
        self.in_position = False
        self.entry_price = 0.0
        self.current_sl_price = 0.0
        self.current_tp_price = 0.0
        self.current_oco_algo_id = None

        self._balance_update_failures = 0
        self._is_updating_sl = False  # 🌟 新增：防止高频重复发单的锁

        if not self.api_key:
            logger.error("⚠️ OKX API 密钥未配置，请检查 .env 文件！实盘将无法执行下单。")

    # =======================================================
    # 底层 API 鉴权与请求模块
    # =======================================================
    def _get_signature(self, timestamp, method, request_path, body):
        message = str(timestamp) + str(method) + str(request_path) + str(body)
        mac = hmac.new(
            bytes(self.secret_key, encoding='utf8'),
            bytes(message, encoding='utf-8'),
            digestmod='sha256'
        )
        return base64.b64encode(mac.digest()).decode('utf-8')

    def _get_headers(self, method, request_path, body=""):
        timestamp = datetime.datetime.utcnow().isoformat()[:-3] + 'Z'
        sign = self._get_signature(timestamp, method, request_path, body)
        return {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": sign,
            "OK-ACCESS-TIMESTAMP": str(timestamp),
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json"
        }

    async def _request(self, method, endpoint, payload=None):
        def do_request():
            url = self.base_url + endpoint
            body_str = json.dumps(payload) if payload else ""
            headers = self._get_headers(method, endpoint, body_str)
            try:
                if method == 'POST':
                    res = requests.post(url, data=body_str, headers=headers, timeout=5)
                else:
                    res = requests.get(url, headers=headers, timeout=5)
                return res.json()
            except Exception as e:
                logger.error(f"API 请求异常: {e}")
                return None

        return await asyncio.to_thread(do_request)

    # =======================================================
    # 策略执行核心逻辑
    # =======================================================
    async def process_signal(self, signal: Dict[str, Any], current_price: float):
        """接收雷达信号主入口"""
        direction = signal.get('direction', 'BUY')

        # 👇 处理反向平仓
        if direction == 'SELL':
            if self.in_position:
                # 🛠️ 执行“严格限制”检查
                conf = signal.get('confidence', 0)
                hidden_vol = signal.get('hidden_volume', 0)
                abs_rate = signal.get('absorption_rate', 0)

                if conf >= 0.9 and hidden_vol >= 2_000_000 and abs_rate >= 0.8:
                    logger.info(f"🚨 [策略B-反向逃顶] 遇到超强阻力 (体量:{hidden_vol:,.0f}U)，执行紧急平仓！")
                    await self._execute_close_position()
                else:
                    logger.info(f"👀 [数据记录] 上方存在空头阻力但强度不足以平仓，确信度:{conf:.2f}")
            else:
                logger.debug("👀 [数据打点] 记录上方卖盘冰山阻力位，暂不开空。")
            return

        min_price = signal.get('min_price', current_price)

        if not self.in_position:
            await self._execute_entry(current_price, min_price)
        else:
            await self._check_trailing_stop(current_price, min_price)

    async def _execute_entry(self, current_price: float, iceberg_min_price: float):
        """精准计算风控并执行市价开多"""

        # 🌟 安全检查：确保有余额可以交易
        if self.available_usdt < 2.0:
            logger.warning(f"⚠️ 当前账户可用余额过低 ({self.available_usdt:.2f} U)，拒绝开仓！")
            return

        # 1. 整数避险止损
        base_int = math.floor(iceberg_min_price)
        proposed_sl = round(base_int - self.sl_tick_offset, 2)

        # 2. 风险与头寸计算
        price_drop_risk_per_eth = current_price - proposed_sl
        fee_cost_per_eth = current_price * self.fee_rate
        total_risk_per_eth = price_drop_risk_per_eth + fee_cost_per_eth

        if total_risk_per_eth <= 0:
            logger.warning("⚠️ 止损价格计算异常，放弃开仓。")
            return

        # 🌟 核心修改：使用动态 API 查回来的实际余额！
        max_loss_usdt = self.available_usdt * self.max_risk_pct
        max_eth_by_risk = max_loss_usdt / total_risk_per_eth

        # 预留 5% 的手续费和滑点空间
        max_eth_by_leverage = (self.available_usdt * 0.95 * self.leverage) / current_price

        actual_eth = min(max_eth_by_risk, max_eth_by_leverage)

        # 转换为真实的合约张数，支持 0.01 张的极小步长精度
        raw_contracts = actual_eth / self.contract_multiplier
        contracts = math.floor(raw_contracts * 100) / 100.0  # 向下取整到两位小数，如 0.875 -> 0.87

        if contracts < 0.01:
            logger.warning(
                f"⚠️ 风险过大，算出的张数 {contracts} 小于最小下单量 0.01 张，放弃开仓！(当前余额: {self.available_usdt:.2f} U)")
            return

        is_full = (actual_eth == max_eth_by_leverage)
        pos_msg = f"满仓({self.leverage}X)突击" if is_full else "严控防线降杠杆"
        proposed_tp = round(current_price * (1 + self.tp_pct), 2)

        logger.info(
            f"⚔️ [{pos_msg}] 现价 {current_price} | 动用本金: ~{contracts * self.contract_multiplier * current_price / self.leverage:.2f} U | 买入 {contracts} 张")

        # 3. 执行市价买单 (Taker)
        buy_payload = {
            "instId": self.symbol, "tdMode": self.td_mode, "side": "buy",
            "ordType": "market", "sz": str(contracts)
        }
        buy_res = await self._request("POST", "/api/v5/trade/order", buy_payload)

        if buy_res and buy_res.get('code') == '0':
            logger.info("✅ 进场成交！立刻挂载 OCO 防御阵地...")
            self.in_position = True
            self.entry_price = current_price

            # 4. 挂载 OCO (止盈限价 + 止损市价)
            await self._place_oco_order(contracts, proposed_tp, proposed_sl)
        else:
            logger.error(f"❌ 进场失败: {buy_res}")

    async def _execute_close_position(self):
        """立即平掉所有当前持仓并清理挂单"""
        # A. 撤销所有当前算法单 (OCO)
        if self.current_oco_algo_id:
            cancel_payload = [{"instId": self.symbol, "algoId": self.current_oco_algo_id}]
            await self._request("POST", "/api/v5/trade/cancel-algos", cancel_payload)
            self.current_oco_algo_id = None

        # B. 查询实时持仓大小 (防止由于追踪止损导致的部分成交误差)
        pos_res = await self._request("GET", f"/api/v5/account/positions?instId={self.symbol}")
        contracts = 0.0
        if pos_res and pos_res.get('code') == '0':
            for p in pos_res['data']:
                if p['instId'] == self.symbol:
                    contracts = abs(float(p['pos']))
                    break

        if contracts <= 0:
            self.in_position = False
            return

        # C. 发送市价平仓单 (反向逃顶必须是市价夺路而逃)
        close_payload = {
            "instId": self.symbol,
            "tdMode": self.td_mode,
            "side": "sell",  # 平多单是卖出
            "ordType": "market",
            "sz": str(contracts),
            "reduceOnly": True
        }
        res = await self._request("POST", "/api/v5/trade/order", close_payload)

        if res and res.get('code') == '0':
            logger.info(f"💰 [逃顶成功] 已按市价平仓 {contracts} 张合约，成功躲避潜在抛压。")
            self.in_position = False
            # 平仓后立刻刷新一次余额，准备下一波战斗
            await self.fetch_balance_and_position()
        else:
            logger.error(f"❌ 平仓失败！可能导致裸奔: {res}")

    async def _place_oco_order(self, contracts: float, tp_price: float, sl_price: float):
        """挂载止盈止损二选一条件单"""
        payload = {
            "instId": self.symbol,
            "tdMode": self.td_mode,
            "side": "sell",
            "ordType": "oco",
            "sz": str(contracts),
            "tpTriggerPx": str(tp_price),
            "tpOrdPx": str(tp_price),     # 🌟 核心修改：指定具体价格，触发后挂出限价单 (Maker) 赚取极低费率
            "slTriggerPx": str(sl_price),
            "slOrdPx": "-1",              # 🌟 保持不变：止损必须是市价，保命不讲价
            "reduceOnly": True
        }
        res = await self._request("POST", "/api/v5/trade/order-algo", payload)

        if res and res.get('code') == '0':
            self.current_oco_algo_id = res['data'][0]['algoId']
            self.current_sl_price = sl_price
            self.current_tp_price = tp_price
            logger.info(f"🛡️ [阵地稳固] 止损(市价): {sl_price} | 止盈(限价): {tp_price}")
        else:
            logger.error(f"❌ OCO 挂单失败，账户处于裸奔状态请注意！{res}")

    async def _check_trailing_stop(self, current_price: float, new_iceberg_min_price: float):
        """基于新冰山的阶梯追踪止损"""
        rise_pct = (new_iceberg_min_price - self.entry_price) / self.entry_price

        if rise_pct >= self.trailing_trigger_pct:
            new_sl = round(math.floor(new_iceberg_min_price) - self.sl_tick_offset, 2)

            if new_sl > self.current_sl_price:
                # 锚定新冰山底价，重新计算止盈
                new_tp = round(new_iceberg_min_price * (1 + self.tp_pct), 2)

                logger.info(f"📈 [阶梯追踪!] 发现更高冰山阵地 (+{rise_pct:.2%})，部队前压！")

                if self.current_oco_algo_id:
                    cancel_payload = [{"instId": self.symbol, "algoId": self.current_oco_algo_id}]
                    await self._request("POST", "/api/v5/trade/cancel-algos", cancel_payload)

                pos_res = await self._request("GET", f"/api/v5/account/positions?instId={self.symbol}")
                contracts = 0.0
                if pos_res and pos_res.get('code') == '0':
                    for p in pos_res['data']:
                        if p['instId'] == self.symbol:
                            contracts = abs(float(p['pos']))
                            break

                if contracts > 0:
                    logger.info(
                        f"🔄 止损上移: {self.current_sl_price} -> {new_sl} | 止盈: {self.current_tp_price} -> {new_tp}")
                    await self._place_oco_order(contracts, new_tp, new_sl)

    # =======================================================
    # 财务探针后台守护
    # =======================================================
    async def fetch_balance_and_position(self) -> bool:
        """🌟 核心：同时请求获取 USDT 可用余额与真实持仓状态"""
        pos_res = await self._request("GET", f"/api/v5/account/positions?instId={self.symbol}")
        balance_res = await self._request("GET", "/api/v5/account/balance")

        if not pos_res or not balance_res or pos_res.get('code') != '0' or balance_res.get('code') != '0':
            return False

        # 1. 更新持仓状态与 OCO 清理
        positions = pos_res.get('data', [])
        has_pos = any(abs(float(p.get('pos', 0))) > 0 for p in positions)

        if not has_pos and self.in_position:
            logger.info("💰 [战报] 侦测到仓位已清空（止盈或止损触发），系统重置状态，等待下一次猎杀。")
            self.in_position = False
            self.current_oco_algo_id = None
            self.current_sl_price = 0.0
        elif has_pos and not self.in_position:
            self.in_position = True

        # 2. 动态更新可用余额
        details = balance_res['data'][0]['details']
        for asset in details:
            if asset['ccy'] == 'USDT':
                self.available_usdt = float(asset['availEq'])
                break

        return True

    async def update_balance_loop(self):
        """后台查账：断网重连、资金同步、状态同步"""
        logger.info("💰 [财务官] 已上线！同步初始化 API 状态并拉取真实余额...")
        await self._set_leverage_on_startup()

        while True:
            try:
                success = await self.fetch_balance_and_position()
                if success:
                    self._balance_update_failures = 0
                else:
                    raise ConnectionError("API 返回异常数据")

                # 战时 5 秒死盯，平时 60 秒查账
                await asyncio.sleep(5 if self.in_position else 60)

            except Exception as e:
                self._balance_update_failures += 1
                logger.error(f"💰 财务接口异常: {e}")
                await asyncio.sleep(min(10 * self._balance_update_failures, 300))

    async def _set_leverage_on_startup(self):
        """系统冷启动：设置全/逐仓与杠杆"""
        mode_payload = {"instId": self.symbol, "mgnMode": self.td_mode}
        await self._request("POST", "/api/v5/account/set-isolated-mode", mode_payload)

        lev_payload = {"instId": self.symbol, "lever": str(self.leverage), "mgnMode": self.td_mode}
        res = await self._request("POST", "/api/v5/account/set-leverage", lev_payload)

        if res and res.get('code') == '0':
            logger.info(f"✅ 实盘同步成功！{self.symbol} 已锁定 【{self.td_mode.upper()} {self.leverage}X】")
        else:
            logger.warning(f"⚠️ 实盘参数同步提示: {res.get('msg', '可能已经设置过')}")

    async def check_breakeven_lock(self, current_price: float):
        """🌟 实时价格监控：利润达到 0.3% 时，强制将止损提至进场价 + 0.12%"""
        # 如果没持仓、刚进场还没拿到底价、或者正在更新订单中，直接返回
        if not self.in_position or self.entry_price == 0 or self._is_updating_sl:
            return

        breakeven_sl = round(self.entry_price * 1.0012, 2)

        # 如果当前的止损已经高于或等于保本价，说明已经安全，不需要重复操作
        if self.current_sl_price >= breakeven_sl:
            return

        # 计算当前利润率
        profit_pct = (current_price - self.entry_price) / self.entry_price

        # 触发条件：利润冲破 0.3%！
        if profit_pct >= 0.003:
            self._is_updating_sl = True  # 上锁，防止后续 tick 重复调用
            try:
                logger.info(
                    f"🔒 [利润保护] 现价 {current_price} (利润 {profit_pct:.2%})！触发保本锁，止损上调至 +0.12% ({breakeven_sl})")

                # 1. 撤销旧的 OCO 订单
                if self.current_oco_algo_id:
                    cancel_payload = [{"instId": self.symbol, "algoId": self.current_oco_algo_id}]
                    await self._request("POST", "/api/v5/trade/cancel-algos", cancel_payload)

                # 2. 查询当前真实仓位大小
                pos_res = await self._request("GET", f"/api/v5/account/positions?instId={self.symbol}")
                contracts = 0.0
                if pos_res and pos_res.get('code') == '0':
                    for p in pos_res['data']:
                        if p['instId'] == self.symbol:
                            contracts = abs(float(p['pos']))
                            break

                # 3. 按保本止损价重新挂载 OCO
                if contracts > 0:
                    await self._place_oco_order(contracts, self.current_tp_price, breakeven_sl)
            finally:
                self._is_updating_sl = False  # 解锁
