#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from tools.backtest_iceberg_boll_mid_strategy import (
    MartingaleParams,
    MartingalePosition,
    martingale_liquidation_price,
    martingale_position_size,
    martingale_stop_price,
    martingale_tp_price,
    run_martingale_from_files,
    simulate_martingale_backtest,
)


START = 1_767_225_600.0


def _minute_bars(closes: list[float], *, after: dict[int, dict[str, float]] | None = None) -> list[dict[str, float]]:
    after = after or {}
    rows = []
    for minute, close in enumerate(closes):
        row = {
            "timestamp": START + minute * 60,
            "open": close,
            "high": close + 0.5,
            "low": close - 0.5,
            "close": close,
            "volume": 1.0,
        }
        row.update(after.get(minute, {}))
        rows.append(row)
    return rows


def _long_klines(after: dict[int, dict[str, float]] | None = None) -> list[dict[str, float]]:
    return _minute_bars([100.0] * 15 + [100.0] * 15 + [94.0] * 120, after=after)


def _short_klines(after: dict[int, dict[str, float]] | None = None) -> list[dict[str, float]]:
    return _minute_bars([100.0] * 15 + [100.0] * 15 + [106.0] * 120, after=after)


def _candidate(direction: str = "BUY", *, ts_min: int = 45, price: float | None = None, event_key: str | None = None, **overrides) -> dict:
    price = price if price is not None else (92.0 if direction == "BUY" else 108.0)
    row = {
        "record_type": "candidate_finalized",
        "result": "ICEBERG",
        "event_key": event_key or f"{direction.lower()}-{ts_min}-{price}",
        "direction": direction,
        "settle_ts": START + ts_min * 60,
        "settle_price": price,
        "trigger_price": 0,
        "zone_lower": price if direction == "BUY" else price - 0.2,
        "zone_upper": price + 0.2 if direction == "BUY" else price,
        "min_trade_price": price,
        "max_trade_price": price,
        "first_iceberg_pie_min_trade_price": price,
        "first_pie_min_trade_price": price,
        "first_iceberg_pie_max_trade_price": price,
        "first_pie_max_trade_price": price,
        "truth_score": {"truth_score_total": 0, "truth_label": "NOT_ICEBERG"},
    }
    row.update(overrides)
    return row


def _params(**overrides) -> MartingaleParams:
    values = {
        "scenario_id": "unit",
        "boll_period": 3,
        "timezone": "UTC",
        "liquidation_check": True,
    }
    values.update(overrides)
    return MartingaleParams(**values)


def _position(side: str, prices: list[float], *, params: MartingaleParams | None = None) -> MartingalePosition:
    params = params or _params()
    direction = "SELL" if side == "SHORT" else "BUY"
    pos = MartingalePosition("unit", "P00001", side, direction, START, 100.0)
    for idx, price in enumerate(prices, start=1):
        size, reason = martingale_position_size(100.0, pos.used_margin_total, price, params)
        assert reason is None
        leg = {
            "leg_id": idx,
            "entry_ts": START + idx * 60,
            "entry_price": price,
            "eth_qty": size["eth_qty"],
            "notional": size["notional"],
            "margin_used": size["margin_used"],
            "entry_fee": size["entry_fee"],
        }
        pos.legs.append(leg)
    return pos


def _write_inputs(tmp_path: Path, candidates: list[dict], klines: list[dict]) -> tuple[Path, Path]:
    phase1 = tmp_path / "phase1_candidates.jsonl"
    phase1.write_text("\n".join(json.dumps(row) for row in candidates) + "\n", encoding="utf-8")
    kline = tmp_path / "kline.csv"
    with kline.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        writer.writerows(klines)
    return phase1, kline


def test_buy_iceberg_below_lower_opens_first_leg_and_above_lower_rejected():
    result = simulate_martingale_backtest(
        [_candidate("BUY", price=92.0), _candidate("BUY", ts_min=46, price=93.0)],
        _long_klines(),
        _params(liquidation_check=False),
    )
    assert result["legs"][0]["side"] == "LONG"
    assert result["legs"][0]["direction"] == "BUY"
    assert any(row["reason"] == "NOT_BELOW_BOLL_LOWER" for row in result["rejections"])


def test_sell_iceberg_above_upper_opens_short_and_inside_upper_rejected():
    result = simulate_martingale_backtest(
        [_candidate("SELL", price=108.0), _candidate("SELL", ts_min=46, price=107.0)],
        _short_klines(),
        _params(liquidation_check=False),
    )
    assert result["legs"][0]["side"] == "SHORT"
    assert result["legs"][0]["direction"] == "SELL"
    assert any(row["reason"] == "NOT_ABOVE_BOLL_UPPER" for row in result["rejections"])


def test_non_buy_sell_iceberg_rejected_as_unsupported_direction():
    result = simulate_martingale_backtest([_candidate("HOLD", price=100.0)], _long_klines(), _params())
    assert result["legs"] == []
    assert result["rejections"][0]["reason"] == "UNSUPPORTED_DIRECTION"


def test_long_freeze_requires_drop_and_resets_after_each_add():
    result = simulate_martingale_backtest(
        [
            _candidate("BUY", ts_min=45, price=92.0, event_key="open"),
            _candidate("BUY", ts_min=50, price=91.8, event_key="reject"),
            _candidate("BUY", ts_min=51, price=91.7, event_key="add"),
            _candidate("BUY", ts_min=52, price=91.5, event_key="reset-reject"),
        ],
        _long_klines(),
        _params(drop_pct=0.003, take_profit_pct=0.1, liquidation_check=False),
    )
    assert [leg["signal_price"] for leg in result["legs"]] == [92.0, 91.7]
    rejected = {row["candidate_event_key"]: row["reason"] for row in result["rejections"]}
    assert rejected["reject"] == "FROZEN_NOT_ENOUGH_DROP"
    assert rejected["reset-reject"] == "FROZEN_NOT_ENOUGH_DROP"


def test_long_after_freeze_expiry_does_not_require_drop():
    result = simulate_martingale_backtest(
        [_candidate("BUY", ts_min=45, price=92.0), _candidate("BUY", ts_min=76, price=92.0)],
        _long_klines(),
        _params(drop_pct=0.003, take_profit_pct=0.1, liquidation_check=False),
    )
    assert len(result["legs"]) == 2
    assert result["legs"][1]["freeze_rule_used"] == "FREEZE_EXPIRED"


def test_short_freeze_requires_rise_and_after_expiry_does_not():
    result = simulate_martingale_backtest(
        [
            _candidate("SELL", ts_min=45, price=108.0, event_key="open"),
            _candidate("SELL", ts_min=50, price=108.2, event_key="reject"),
            _candidate("SELL", ts_min=51, price=108.4, event_key="add"),
            _candidate("SELL", ts_min=82, price=108.0, event_key="expired"),
        ],
        _short_klines(),
        _params(drop_pct=0.003, take_profit_pct=0.1, liquidation_check=False),
    )
    assert [leg["signal_price"] for leg in result["legs"]] == [108.0, 108.4, 108.0]
    rejected = {row["candidate_event_key"]: row["reason"] for row in result["rejections"]}
    assert rejected["reject"] == "FROZEN_NOT_ENOUGH_RISE"
    assert result["legs"][2]["freeze_rule_used"] == "FREEZE_EXPIRED"


def test_margin_scenarios_size_contracts_correctly():
    size_3, _ = martingale_position_size(100.0, 0.0, 92.0, _params(margin_pct=0.03))
    size_5, _ = martingale_position_size(100.0, 0.0, 92.0, _params(margin_pct=0.05))
    assert size_3["contracts"] == pytest.approx(16.3)
    assert size_5["contracts"] == pytest.approx(27.17)


def test_drop_scenarios_are_independent():
    result_03 = simulate_martingale_backtest(
        [_candidate("BUY", ts_min=45, price=92.0), _candidate("BUY", ts_min=50, price=91.6)],
        _long_klines(),
        _params(drop_pct=0.003, liquidation_check=False),
    )
    result_05 = simulate_martingale_backtest(
        [_candidate("BUY", ts_min=45, price=92.0), _candidate("BUY", ts_min=50, price=91.6)],
        _long_klines(),
        _params(drop_pct=0.005, liquidation_check=False),
    )
    assert len(result_03["legs"]) == 2
    assert len(result_05["legs"]) == 1
    assert result_05["rejections"][0]["reason"] == "FROZEN_NOT_ENOUGH_DROP"


def test_avg_cost_weighted_correctly():
    pos = _position("LONG", [92.0, 91.7], params=_params(liquidation_check=False))
    expected = sum(leg["entry_price"] * leg["eth_qty"] for leg in pos.legs) / sum(leg["eth_qty"] for leg in pos.legs)
    assert pos.avg_cost == pytest.approx(expected)


def test_long_tp_uses_max_fixed_target_and_boll_mid():
    pos = _position("LONG", [92.0])
    assert martingale_tp_price(pos, {"middle": 98.0}, _params()) == pytest.approx(98.0)
    assert martingale_tp_price(pos, {"middle": 92.1}, _params()) == pytest.approx(92.0 * 1.005)


def test_short_tp_uses_min_fixed_target_and_boll_mid():
    pos = _position("SHORT", [108.0])
    assert martingale_tp_price(pos, {"middle": 102.0}, _params()) == pytest.approx(102.0)
    assert martingale_tp_price(pos, {"middle": 107.8}, _params()) == pytest.approx(108.0 * 0.995)


def test_long_stop_requires_more_than_12_legs():
    candidates = [_candidate("BUY", ts_min=45 + i, price=92.0 - i * 0.4) for i in range(12)]
    result = simulate_martingale_backtest(
        candidates,
        _long_klines(after={70: {"high": 94.0, "low": 87.0, "close": 93.0}}),
        _params(drop_pct=0.003, take_profit_pct=0.1, liquidation_check=False),
    )
    assert result["positions"][0]["leg_count"] == 12
    assert result["positions"][0]["exit_reason"] != "MARTINGALE_STOP"


def test_long_more_than_12_legs_triggers_martingale_stop_when_liquidation_disabled():
    candidates = [_candidate("BUY", ts_min=45 + i, price=92.0 - i * 0.4) for i in range(13)]
    result = simulate_martingale_backtest(
        candidates,
        _long_klines(after={70: {"high": 94.0, "low": 87.0, "close": 93.0}}),
        _params(drop_pct=0.003, take_profit_pct=0.1, liquidation_check=False),
    )
    assert result["positions"][0]["leg_count"] == 13
    assert result["positions"][0]["exit_reason"] == "MARTINGALE_STOP"


def test_short_more_than_12_legs_triggers_martingale_stop_when_liquidation_disabled():
    candidates = [_candidate("SELL", ts_min=45 + i, price=108.0 + i * 0.4) for i in range(13)]
    result = simulate_martingale_backtest(
        candidates,
        _short_klines(after={70: {"high": 113.0, "low": 105.0, "close": 106.0}}),
        _params(drop_pct=0.003, take_profit_pct=0.1, liquidation_check=False),
    )
    assert result["positions"][0]["leg_count"] == 13
    assert result["positions"][0]["exit_reason"] == "MARTINGALE_STOP"


def test_liquidation_proxy_priority_and_prices_for_long_and_short():
    long_pos = _position("LONG", [92.0])
    short_pos = _position("SHORT", [108.0])
    assert martingale_liquidation_price(long_pos, _params()) == pytest.approx(92.0 * 0.986)
    assert martingale_liquidation_price(short_pos, _params()) == pytest.approx(108.0 * 1.014)

    candidates = [_candidate("BUY", ts_min=45 + i, price=92.0 - i * 0.4) for i in range(13)]
    result = simulate_martingale_backtest(
        candidates,
        _long_klines(after={70: {"high": 94.0, "low": 87.0, "close": 93.0}}),
        _params(take_profit_pct=0.1),
    )
    assert result["positions"][0]["exit_reason"] == "LIQUIDATION_PROXY"


def test_short_liquidation_proxy_can_trigger_before_stop():
    candidates = [_candidate("SELL", ts_min=45 + i, price=108.0 + i * 0.4) for i in range(12)]
    result = simulate_martingale_backtest(
        candidates,
        _short_klines(after={70: {"high": 112.0, "low": 105.0, "close": 106.0}}),
        _params(take_profit_pct=0.1),
    )
    assert result["positions"][0]["leg_count"] <= 12
    assert result["positions"][0]["exit_reason"] == "LIQUIDATION_PROXY"


def test_entry_and_exit_fees_are_charged():
    result = simulate_martingale_backtest(
        [_candidate("BUY", ts_min=45, price=92.0)],
        _long_klines(after={46: {"high": 99.0, "low": 93.5, "close": 98.0}}),
        _params(liquidation_check=False),
    )
    leg = result["legs"][0]
    pos = result["positions"][0]
    assert leg["entry_fee"] == pytest.approx(leg["notional"] * 0.0005)
    assert pos["exit_fee"] == pytest.approx(pos["total_eth_qty"] * pos["exit_price"] * 0.0005)
    assert pos["net_pnl"] == pytest.approx(pos["gross_pnl"] - pos["total_entry_fee"] - pos["exit_fee"])


def test_opposite_side_position_open_rejections():
    long_result = simulate_martingale_backtest(
        [_candidate("BUY", ts_min=45, price=92.0), _candidate("SELL", ts_min=46, price=108.0)],
        _long_klines(),
        _params(liquidation_check=False),
    )
    short_result = simulate_martingale_backtest(
        [_candidate("SELL", ts_min=45, price=108.0), _candidate("BUY", ts_min=46, price=92.0)],
        _short_klines(),
        _params(liquidation_check=False),
    )
    assert long_result["rejections"][0]["reason"] == "OPPOSITE_SIDE_POSITION_OPEN"
    assert short_result["rejections"][0]["reason"] == "OPPOSITE_SIDE_POSITION_OPEN"


def test_no_active_position_allows_buy_and_sell_independent_baskets():
    buy_result = simulate_martingale_backtest([_candidate("BUY", ts_min=45, price=92.0)], _long_klines(), _params(liquidation_check=False))
    sell_result = simulate_martingale_backtest([_candidate("SELL", ts_min=45, price=108.0)], _short_klines(), _params(liquidation_check=False))
    assert buy_result["legs"][0]["side"] == "LONG"
    assert sell_result["legs"][0]["side"] == "SHORT"


def test_scenario_summary_outputs_four_scenarios_and_direction_stats(tmp_path):
    phase1, kline = _write_inputs(tmp_path, [_candidate("BUY", ts_min=45, price=92.0), _candidate("SELL", ts_min=90, price=108.0)], _long_klines())
    out = tmp_path / "out"
    run_martingale_from_files(
        phase1,
        kline,
        out,
        _params(liquidation_check=False),
        margin_pct_list=[0.03, 0.05],
        drop_pct_list=[0.003, 0.005],
    )
    with (out / "iceberg_boll_martingale_scenario_summary.csv").open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 4
    assert {"long_positions", "short_positions", "long_net_pnl", "short_net_pnl"}.issubset(rows[0].keys())


def test_truth_score_does_not_participate_in_martingale_entry():
    result = simulate_martingale_backtest(
        [_candidate("BUY", price=92.0, truth_score={"truth_score_total": 0, "truth_label": "NOT_ICEBERG"})],
        _long_klines(),
        _params(liquidation_check=False),
    )
    assert len(result["legs"]) == 1


def test_candidate_open_is_not_checked_for_exit_until_next_1m_bar():
    result = simulate_martingale_backtest(
        [_candidate("BUY", ts_min=46, price=92.0)],
        _long_klines(after={46: {"high": 200.0, "low": 93.5, "close": 94.0}, 47: {"high": 94.5, "low": 93.5, "close": 94.0}}),
        _params(take_profit_pct=0.1, liquidation_check=False),
    )
    assert result["positions"][0]["exit_reason"] == "END_OF_DATA_CLOSE"
    assert result["positions"][0]["close_ts"] != START + 46 * 60


def test_short_stop_price_uses_upside_drawdown():
    pos = _position("SHORT", [108.0])
    assert martingale_stop_price(pos, _params()) == pytest.approx(108.0 * 1.02)
