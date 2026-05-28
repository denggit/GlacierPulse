#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

import tools.backtest_iceberg_boll_mid_strategy as strategy
from tools.backtest_iceberg_boll_mid_strategy import (
    BacktestParams,
    boll_touch_info,
    build_bollinger_bars,
    calculate_position_size,
    candidate_price,
    compute_fee_aware_r_metrics,
    find_a3_entry,
    is_near_required_band,
    last_closed_boll,
    run_from_files,
    simulate_backtest,
    simulate_exit,
    structural_stop,
)


START = 1_767_225_600.0


def _minute_bars(closes: list[float], *, after: dict[int, dict[str, float]] | None = None) -> list[dict[str, float]]:
    bars: list[dict[str, float]] = []
    after = after or {}
    for minute, close in enumerate(closes):
        row = {
            "timestamp": START + minute * 60,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 1.0,
        }
        row.update(after.get(minute, {}))
        bars.append(row)
    return bars


def _base_buy_klines(*, entry_high: float = 93.2, exit_high: float = 98.5) -> list[dict[str, float]]:
    closes = [100.0] * 15 + [100.0] * 15 + [94.0] * 15 + [94.0] * 30
    return _minute_bars(
        closes,
        after={
            46: {"high": entry_high, "low": 93.0, "close": 93.1},
            47: {"high": exit_high, "low": 93.1, "close": exit_high},
        },
    )


def _base_sell_klines(*, entry_low: float = 106.8, exit_low: float = 101.5) -> list[dict[str, float]]:
    closes = [100.0] * 15 + [100.0] * 15 + [106.0] * 15 + [106.0] * 30
    return _minute_bars(
        closes,
        after={
            46: {"low": entry_low, "high": 107.2, "close": 106.9},
            47: {"low": exit_low, "high": 106.9, "close": exit_low},
        },
    )


def _candidate(direction: str = "BUY", **overrides) -> dict:
    row = {
        "record_type": "candidate_finalized",
        "result": "ICEBERG",
        "event_key": f"{direction.lower()}-1",
        "direction": direction,
        "settle_ts": START + 45 * 60,
        "settle_price": 93.0 if direction == "BUY" else 107.0,
        "trigger_price": 0,
        "zone_lower": 92.2 if direction == "BUY" else 107.6,
        "zone_upper": 92.4 if direction == "BUY" else 107.8,
        "min_trade_price": 93.5,
        "max_trade_price": 106.0,
        "first_iceberg_pie_min_trade_price": 93.5,
        "first_iceberg_pie_max_trade_price": 106.0,
        "truth_score": {"truth_score_total": 0, "truth_label": "NOT_ICEBERG"},
        "absorption_rate": 0.8,
        "hidden_volume": 1000,
        "active_notional": 2000,
    }
    row.update(overrides)
    return row


def _params(**overrides) -> BacktestParams:
    values = {
        "boll_period": 3,
        "boll_std": 2.0,
        "min_target_r": 3.0,
        "timezone": "UTC",
        "entry_timeout_min": 60,
    }
    values.update(overrides)
    return BacktestParams(**values)


def _write_inputs(tmp_path: Path, candidates: list[dict], klines: list[dict]) -> tuple[Path, Path]:
    phase1 = tmp_path / "phase1_candidates.jsonl"
    phase1.write_text("\n".join(json.dumps(row) for row in candidates) + "\n", encoding="utf-8")
    kline = tmp_path / "kline.csv"
    with kline.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        writer.writerows(klines)
    return phase1, kline


def test_buy_touch_lower_band_passes():
    result = simulate_backtest([_candidate("BUY")], _base_buy_klines(), _params())
    assert len(result["trades"]) == 1
    assert result["trades"][0]["direction"] == "BUY"
    assert result["trades"][0]["boll_touch_flag"] is True
    assert result["trades"][0]["boll_touch_type"] == "ZONE_CONTAINS_LOWER"


def test_buy_not_touch_lower_band_rejected():
    result = simulate_backtest(
        [_candidate("BUY", settle_price=95.0, zone_lower=94.0, zone_upper=95.0, first_iceberg_pie_min_trade_price=94.5, min_trade_price=94.5)],
        _base_buy_klines(),
        _params(),
    )
    assert result["rejections"][0]["reason"] == "NOT_TOUCH_BOLL_LOWER"
    assert result["rejections"][0]["boll_touch_type"] == "NO_TOUCH"


def test_sell_touch_upper_band_passes():
    result = simulate_backtest([_candidate("SELL")], _base_sell_klines(), _params())
    assert len(result["trades"]) == 1
    assert result["trades"][0]["direction"] == "SELL"
    assert result["trades"][0]["boll_touch_flag"] is True
    assert result["trades"][0]["boll_touch_type"] == "ZONE_CONTAINS_UPPER"


def test_sell_not_touch_upper_band_rejected():
    result = simulate_backtest(
        [_candidate("SELL", settle_price=105.0, zone_lower=105.0, zone_upper=106.0, first_iceberg_pie_max_trade_price=106.0, max_trade_price=106.0)],
        _base_sell_klines(),
        _params(),
    )
    assert result["rejections"][0]["reason"] == "NOT_TOUCH_BOLL_UPPER"
    assert result["rejections"][0]["boll_touch_type"] == "NO_TOUCH"


def test_buy_a3_breakout_above_zone_upper_enters():
    entry, trigger = find_a3_entry(_candidate("BUY"), _base_buy_klines(), _params())
    assert trigger == 92.4
    assert entry["entry_price"] == 92.4
    assert entry["entry_ts"] == START + 46 * 60


def test_sell_a3_breakout_below_zone_lower_enters():
    entry, trigger = find_a3_entry(_candidate("SELL"), _base_sell_klines(), _params())
    assert trigger == 107.6
    assert entry["entry_price"] == 107.6
    assert entry["entry_ts"] == START + 46 * 60


def test_structural_stop_uses_sweep_extreme_plus_or_minus_buffer():
    buy_stop, buy_risk, buy_basis, buy_basis_type = structural_stop(
        _candidate("BUY", first_iceberg_pie_min_trade_price=93.5, zone_lower=92.0),
        93.0,
        _params(),
    )
    sell_stop, sell_risk, sell_basis, sell_basis_type = structural_stop(
        _candidate("SELL", first_iceberg_pie_max_trade_price=106.0, zone_upper=108.0),
        107.0,
        _params(),
    )
    assert buy_stop == 90.5
    assert buy_risk == 2.5
    assert buy_basis == 92.0
    assert buy_basis_type == "ZONE_LOWER"
    assert sell_stop == 109.5
    assert sell_risk == 2.5
    assert sell_basis == 108.0
    assert sell_basis_type == "ZONE_UPPER"


def test_compute_fee_aware_r_metrics_buy():
    metrics = compute_fee_aware_r_metrics("BUY", 100.0, 97.0, 109.0, 0.0005)

    assert metrics["price_risk_u"] == pytest.approx(3.0)
    assert metrics["price_target_u"] == pytest.approx(9.0)
    assert metrics["entry_fee_u"] == pytest.approx(0.05)
    assert metrics["stop_exit_fee_u"] == pytest.approx(0.0485)
    assert metrics["target_exit_fee_u"] == pytest.approx(0.0545)
    assert metrics["planned_loss_u"] == pytest.approx(3.0985)
    assert metrics["target_net_profit_u"] == pytest.approx(8.8955)
    assert metrics["target_price_r"] == pytest.approx(3.0)
    assert metrics["target_net_r"] == pytest.approx(8.8955 / 3.0985)
    assert metrics["target_net_r"] < metrics["target_price_r"]


def test_compute_fee_aware_r_metrics_sell():
    metrics = compute_fee_aware_r_metrics("SELL", 100.0, 103.0, 91.0, 0.0005)

    assert metrics["price_risk_u"] == pytest.approx(3.0)
    assert metrics["price_target_u"] == pytest.approx(9.0)
    assert metrics["entry_fee_u"] == pytest.approx(0.05)
    assert metrics["stop_exit_fee_u"] == pytest.approx(0.0515)
    assert metrics["target_exit_fee_u"] == pytest.approx(0.0455)
    assert metrics["planned_loss_u"] == pytest.approx(3.1015)
    assert metrics["target_net_profit_u"] == pytest.approx(8.9045)
    assert metrics["target_price_r"] == pytest.approx(3.0)
    assert metrics["target_net_r"] == pytest.approx(8.9045 / 3.1015)
    assert metrics["target_net_r"] < metrics["target_price_r"]


def test_target_r_below_minimum_is_rejected():
    params = _params(min_target_r=20.0)
    result = simulate_backtest([_candidate("BUY")], _base_buy_klines(), params)
    assert result["rejections"][0]["reason"] == "TARGET_LT_MIN_R"


def test_target_filter_rejects_when_price_r_passes_but_fee_aware_r_fails():
    params = _params(min_target_r=3.0, fee_rate_per_side=0.0015)
    result = simulate_backtest([_candidate("BUY")], _base_buy_klines(), params)

    assert result["trades"] == []
    rejection = result["rejections"][0]
    assert rejection["reason"] == "TARGET_LT_MIN_R"
    assert rejection["target_price_r"] >= 3.0
    assert rejection["target_net_r"] < 3.0
    assert rejection["fee_share_r_at_target"] > 0
    assert rejection["planned_loss_u"] > 0


def test_target_filter_accepts_only_when_fee_aware_r_passes():
    result = simulate_backtest([_candidate("BUY")], _base_buy_klines(), _params(min_target_r=3.0))

    assert len(result["trades"]) == 1
    trade = result["trades"][0]
    assert trade["target_price_r_at_entry"] >= 3.0
    assert trade["target_net_r_at_entry"] >= 3.0


def test_position_size_uses_stop_price_fee_for_planned_risk():
    size, reason = calculate_position_size(100.0, 100.0, 97.0, 3.0, BacktestParams())
    assert reason is None
    expected_per_contract_risk = 0.1 * (3.0 + 100.0 * 0.0005 + 97.0 * 0.0005)
    assert size["contracts"] == pytest.approx(6.45)
    assert size["actual_total_risk"] == pytest.approx(size["contracts"] * expected_per_contract_risk)

    gross_pnl = size["contracts"] * 0.1 * (97.0 - 100.0)
    entry_fee = size["contracts"] * 0.1 * 100.0 * 0.0005
    exit_fee = size["contracts"] * 0.1 * 97.0 * 0.0005
    realized_r = (gross_pnl - entry_fee - exit_fee) / size["actual_total_risk"]
    assert realized_r == pytest.approx(-1.0)


def test_contracts_floor_to_cent_step():
    params = BacktestParams(contract_step=0.01)
    size, _ = calculate_position_size(100.0, 123.0, 122.0, 1.0, params)
    assert size["contracts"] == pytest.approx(17.81)


def test_margin_shortfall_shrinks_position():
    params = BacktestParams(leverage=1.0, contract_step=0.01, min_contracts=0.01)
    size, reason = calculate_position_size(100.0, 10_000.0, 9_999.0, 1.0, params)
    assert reason is None
    assert size["contracts"] == pytest.approx(0.1)
    assert size["required_margin"] == pytest.approx(100.0)


def test_dynamic_boll_mid_target_updates_after_15m_close_only():
    klines = _minute_bars(
        [100.0] * 5,
        after={
            2: {"high": 104.5, "low": 99.0, "close": 104.0},
            3: {"high": 105.2, "low": 104.0, "close": 105.0},
        },
    )
    boll_bars = [
        {"close_ts": START + 60, "middle": 110.0, "close": 100.0},
        {"close_ts": START + 180, "middle": 105.0, "close": 104.0},
    ]
    out = simulate_exit("BUY", klines, 1, START + 60, 100.0, 90.0, 110.0, boll_bars[0], boll_bars)
    assert out["exit_reason"] == "BOLL_MID_TARGET"
    assert out["exit_price"] == 105.0
    assert out["exit_ts"] == START + 180


def test_same_bar_stop_and_target_hit_uses_conservative_stop_first():
    klines = _minute_bars([100.0], after={0: {"high": 106.0, "low": 94.0, "close": 100.0}})
    boll = {"close_ts": START - 60, "middle": 105.0, "close": 100.0}
    out = simulate_exit("BUY", klines, 0, START, 100.0, 95.0, 105.0, boll, [boll])
    assert out["exit_reason"] == "AMBIGUOUS_STOP_FIRST"
    assert out["exit_price"] == 95.0


def test_output_trade_rejection_and_summary_files(tmp_path):
    phase1, kline = _write_inputs(
        tmp_path,
        [
            _candidate("BUY"),
            _candidate("BUY", event_key="reject", settle_price=95.0, zone_lower=94.0, zone_upper=95.0, first_iceberg_pie_min_trade_price=94.5, min_trade_price=94.5),
        ],
        _base_buy_klines(),
    )
    out = tmp_path / "out"
    run_from_files(phase1, kline, out, _params())
    assert (out / "iceberg_boll_trades.csv").exists()
    assert (out / "iceberg_boll_equity_curve.csv").exists()
    assert (out / "iceberg_boll_rejections.csv").exists()
    assert (out / "iceberg_boll_summary.json").exists()
    assert (out / "iceberg_boll_summary.md").exists()

    with (out / "iceberg_boll_trades.csv").open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    expected_fields = {
        "target_price_r_at_entry",
        "target_net_r_at_entry",
        "fee_share_r_at_stop",
        "fee_share_r_at_target",
        "planned_loss_u",
        "target_net_profit_u",
    }
    assert expected_fields.issubset(rows[0].keys())
    assert rows[0]["target_r_at_entry"] == rows[0]["target_net_r_at_entry"]


def test_one_position_rejects_by_entry_overlap_not_candidate_time():
    klines = _minute_bars(
        [100.0] * 30 + [94.0] * 15 + [93.0] * 30,
        after={
            48: {"high": 93.0, "low": 90.6, "close": 91.0},
            55: {"high": 94.0, "low": 93.0, "close": 94.0},
            56: {"high": 98.1, "low": 93.5, "close": 98.0},
        },
    )
    candidate_a = _candidate(
        "BUY",
        event_key="candidate-a",
        settle_ts=START + 45 * 60,
        settle_price=92.3,
        zone_lower=92.2,
        zone_upper=94.0,
    )
    candidate_b = _candidate(
        "BUY",
        event_key="candidate-b",
        settle_ts=START + 46 * 60,
        settle_price=92.3,
        zone_lower=92.2,
        zone_upper=92.4,
    )
    result = simulate_backtest([candidate_a, candidate_b], klines, _params(min_target_r=1.0))
    assert [row["candidate_event_key"] for row in result["trades"]] == ["candidate-b", "candidate-a"]
    assert not [row for row in result["rejections"] if row["reason"] == "POSITION_ALREADY_OPEN"]
    assert result["trades"][0]["entry_ts"] == START + 47 * 60
    assert result["trades"][0]["exit_ts"] == START + 48 * 60
    assert result["trades"][1]["entry_ts"] == START + 55 * 60


def test_one_position_rejects_full_interval_overlap(monkeypatch):
    klines = _minute_bars(
        [100.0] * 30 + [94.0] * 15 + [93.0] * 45,
        after={
            47: {"high": 93.0, "low": 91.0, "close": 92.5},
            55: {"high": 94.0, "low": 93.0, "close": 94.0},
        },
    )
    candidate_a = _candidate(
        "BUY",
        event_key="candidate-a",
        settle_ts=START + 45 * 60,
        settle_price=92.3,
        zone_lower=92.2,
        zone_upper=94.0,
    )
    candidate_b = _candidate(
        "BUY",
        event_key="candidate-b",
        settle_ts=START + 46 * 60,
        settle_price=92.3,
        zone_lower=92.2,
        zone_upper=92.4,
    )

    def fake_exit(direction, klines, entry_idx, entry_ts, entry_price, stop_price, initial_target, entry_boll, boll_bars):
        exit_ts = START + (70 if entry_ts == START + 55 * 60 else 60) * 60
        return {
            "exit_ts": exit_ts,
            "exit_price": initial_target,
            "exit_reason": "BOLL_MID_TARGET",
            "exit_boll_mid": initial_target,
            "target_price_at_exit": initial_target,
        }

    monkeypatch.setattr(strategy, "simulate_exit", fake_exit)
    result = simulate_backtest([candidate_a, candidate_b], klines, _params(min_target_r=1.0))

    assert [row["candidate_event_key"] for row in result["trades"]] == ["candidate-b"]
    rejection = next(row for row in result["rejections"] if row["reason"] == "POSITION_ALREADY_OPEN")
    assert rejection["candidate_event_key"] == "candidate-a"
    assert rejection["entry_trigger"] == 94.0
    assert rejection["stop_basis_price"] == 92.2


def test_position_sizing_uses_equity_after_entry_time_ordered_prior_trade():
    klines = _minute_bars(
        [100.0] * 30 + [94.0] * 15 + [93.0] * 30,
        after={
            48: {"high": 93.0, "low": 90.6, "close": 91.0},
            55: {"high": 94.0, "low": 93.0, "close": 94.0},
            56: {"high": 98.1, "low": 93.5, "close": 98.0},
        },
    )
    candidate_a = _candidate(
        "BUY",
        event_key="candidate-a",
        settle_ts=START + 45 * 60,
        settle_price=92.3,
        zone_lower=92.2,
        zone_upper=94.0,
    )
    candidate_b = _candidate(
        "BUY",
        event_key="candidate-b",
        settle_ts=START + 46 * 60,
        settle_price=92.3,
        zone_lower=92.2,
        zone_upper=92.4,
    )
    result = simulate_backtest([candidate_a, candidate_b], klines, _params(min_target_r=1.0))

    first, second = result["trades"]
    assert first["candidate_event_key"] == "candidate-b"
    assert first["net_pnl"] < 0
    assert second["candidate_event_key"] == "candidate-a"
    assert second["equity_before"] == pytest.approx(first["equity_after"])


def test_truth_score_does_not_participate_in_entry():
    row = _candidate("BUY", truth_score={"truth_score_total": 0, "truth_label": "NOT_ICEBERG"})
    result = simulate_backtest([row], _base_buy_klines(), _params())
    assert len(result["trades"]) == 1
    assert result["trades"][0]["truth_score"] == 0


def test_boll_signal_uses_last_closed_15m_bar_before_candidate_not_forming_bar():
    klines = _base_buy_klines()
    params = _params()
    boll_bars = build_bollinger_bars(klines, params)
    signal = last_closed_boll(boll_bars, START + 45 * 60 + 30)
    assert signal["close_ts"] == START + 45 * 60
    assert signal["middle"] == pytest.approx(98.0)


def test_entry_target_uses_last_closed_15m_bar_before_entry_not_future_bar():
    klines = _base_buy_klines()
    result = simulate_backtest([_candidate("BUY")], klines, _params())
    trade = result["trades"][0]
    assert trade["entry_ts"] == START + 46 * 60
    assert trade["initial_boll_mid"] == pytest.approx(98.0)


def test_entry_first_hit_does_not_use_bars_before_entry():
    klines = _base_buy_klines()
    klines[45]["low"] = 1.0
    result = simulate_backtest([_candidate("BUY")], klines, _params())
    assert len(result["trades"]) == 1
    assert result["trades"][0]["exit_reason"] != "STOP"


def test_candidate_price_priority_settle_trigger_zone_mid():
    assert candidate_price({"settle_price": 10, "trigger_price": 20, "zone_lower": 1, "zone_upper": 3}) == 10
    assert candidate_price({"trigger_price": 20, "zone_lower": 1, "zone_upper": 3}) == 20
    assert candidate_price({"zone_lower": 1, "zone_upper": 3}) == 2


def test_boll_touch_type_distinguishes_sweep_and_pie_sources():
    buy_boll = {"lower": 92.0, "middle": 98.0, "upper": 104.0}
    sell_boll = {"lower": 96.0, "middle": 102.0, "upper": 108.0}
    assert boll_touch_info(_candidate("BUY", zone_lower=93.0, zone_upper=94.0, first_iceberg_pie_min_trade_price=91.5), buy_boll)["boll_touch_type"] == "PIE_LOW_TOUCH_LOWER"
    assert boll_touch_info(_candidate("BUY", zone_lower=93.0, zone_upper=94.0, first_iceberg_pie_min_trade_price=93.5, min_trade_price=91.5), buy_boll)["boll_touch_type"] == "SWEEP_LOW_TOUCH_LOWER"
    assert boll_touch_info(_candidate("SELL", zone_lower=105.0, zone_upper=106.0, first_iceberg_pie_max_trade_price=108.5), sell_boll)["boll_touch_type"] == "PIE_HIGH_TOUCH_UPPER"
    assert boll_touch_info(_candidate("SELL", zone_lower=105.0, zone_upper=106.0, first_iceberg_pie_max_trade_price=106.0, max_trade_price=108.5), sell_boll)["boll_touch_type"] == "SWEEP_HIGH_TOUCH_UPPER"


def test_band_proximity_helper_for_buy_and_sell():
    boll = {"lower": 90, "middle": 100, "upper": 110}
    assert is_near_required_band("BUY", 91.9, boll, 0.2)[0] is True
    assert is_near_required_band("BUY", 92.1, boll, 0.2)[0] is False
    assert is_near_required_band("SELL", 108.1, boll, 0.2)[0] is True
    assert is_near_required_band("SELL", 107.9, boll, 0.2)[0] is False
