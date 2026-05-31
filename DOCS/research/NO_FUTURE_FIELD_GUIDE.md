# No-Future Field Guide

V7.3.0 uses explicit suffixes to separate runtime inputs from research and backtest outputs.

- `_rt`: runtime-safe at the decision timestamp and allowed in strategy entry conditions.
- `_future`: computed from data after the event or entry. Never allowed in entry conditions.
- `_offline`: audit or research labels. Never allowed in entry conditions.
- `_sim`: simulated/backtest results. Never allowed in entry conditions.

`A2_COMPRESSION` is now `A2_COMPRESSION_FUTURE_PROXY` because it is derived from post-zone MFE/MAE/range windows. It can describe outcomes but cannot define runtime A2.

`PRICE_BREAKOUT_PERSISTENT` is an `a3_quality_future_type_v2` label. It waits for breakout persistence and no quick return, so it cannot be a runtime entry condition.

Runtime strategy entry conditions must use `a2_rt_*`, `a3_entry_rt_*`, and runtime A1 context such as `vp24h_a1_vp_setup_rt`. `src/research/no_future_audit.py` rejects `_future`, `_offline`, `_sim`, and deprecated lookahead aliases such as `a3_preview_ignition_quality`, `a2_compression_state`, `truth_score_avg`, `PRICE_BREAKOUT_PERSISTENT`, and `A2_COMPRESSION`.

