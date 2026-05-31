#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


FieldAvailability = Literal["rt", "future", "offline", "sim"]


@dataclass(frozen=True)
class FieldSpec:
    name: str
    availability: FieldAvailability
    source_module: str
    description: str
    allowed_for_strategy_entry: bool
    deprecated_aliases: list[str]


NO_FUTURE_FIELD_HYGIENE_VERSION = "v7.3.0.no_future_field_registry"


def _spec(
    name: str,
    availability: FieldAvailability,
    source_module: str,
    description: str,
    *,
    aliases: list[str] | None = None,
) -> FieldSpec:
    return FieldSpec(
        name=name,
        availability=availability,
        source_module=source_module,
        description=description,
        allowed_for_strategy_entry=availability == "rt",
        deprecated_aliases=list(aliases or []),
    )


FIELD_SPECS: dict[str, FieldSpec] = {}
DEPRECATED_LOOKAHEAD_ALIASES: dict[str, str] = {}


def register(spec: FieldSpec) -> FieldSpec:
    FIELD_SPECS[spec.name] = spec
    for alias in spec.deprecated_aliases:
        DEPRECATED_LOOKAHEAD_ALIASES[alias] = spec.name
    return spec


def get_field_spec(name: str) -> FieldSpec | None:
    canonical = DEPRECATED_LOOKAHEAD_ALIASES.get(str(name), str(name))
    return FIELD_SPECS.get(canonical)


def is_deprecated_lookahead_alias(name: str) -> bool:
    return str(name) in DEPRECATED_LOOKAHEAD_ALIASES


def infer_availability(name: str) -> FieldAvailability:
    text = str(name)
    spec = get_field_spec(text)
    if spec:
        return spec.availability
    if is_deprecated_lookahead_alias(text):
        return "future"
    if text.endswith("_future") or "_future_" in text:
        return "future"
    if text.endswith("_offline") or "_offline_" in text:
        return "offline"
    if text.endswith("_sim") or "_sim_" in text or text in {"realized_r_sim", "mfe_r_sim", "mae_r_sim"}:
        return "sim"
    if text.endswith("_rt") or "_rt_" in text:
        return "rt"
    return "rt"


def field_hygiene_summary(fieldnames: list[str] | None = None) -> dict[str, int | str]:
    names = list(fieldnames or FIELD_SPECS.keys())
    counts = {"future": 0, "offline": 0, "sim": 0, "rt": 0}
    for name in names:
        counts[infer_availability(name)] += 1
    return {
        "no_future_field_hygiene_version": NO_FUTURE_FIELD_HYGIENE_VERSION,
        "future_field_count": counts["future"],
        "offline_field_count": counts["offline"],
        "sim_field_count": counts["sim"],
        "rt_field_count": counts["rt"],
        "deprecated_lookahead_alias_count": len(DEPRECATED_LOOKAHEAD_ALIASES),
    }


def _register_defaults() -> None:
    future_aliases = {
        "mfe_15m_u_future": ["mfe_15m_u"],
        "mae_15m_u_future": ["mae_15m_u"],
        "mfe_1h_u_future": ["mfe_1h_u"],
        "mae_1h_u_future": ["mae_1h_u"],
        "mfe_4h_u_future": ["mfe_4h_u"],
        "mae_4h_u_future": ["mae_4h_u"],
        "a2_compression_state_future": ["a2_compression_state", "A2_COMPRESSION"],
        "a2_compression_ratio_15m_future": ["a2_compression_ratio_15m"],
        "a2_compression_ratio_1h_future": ["a2_compression_ratio_1h"],
        "a2_net_mfe_15m_r_future": ["a2_net_mfe_15m_r"],
        "a2_net_mae_15m_r_future": ["a2_net_mae_15m_r"],
        "a2_net_mfe_1h_r_future": ["a2_net_mfe_1h_r"],
        "a2_net_mae_1h_r_future": ["a2_net_mae_1h_r"],
        "a2_net_mfe_4h_r_future": ["a2_net_mfe_4h_r"],
        "a2_net_mae_4h_r_future": ["a2_net_mae_4h_r"],
        "a2_net_hit_1r_15m_future": ["a2_net_hit_1r_15m"],
        "a2_net_hit_1r_1h_future": ["a2_net_hit_1r_1h"],
        "a2_net_hit_2r_1h_future": ["a2_net_hit_2r_1h"],
        "a3_future_ignition_quality": ["a3_preview_ignition_quality"],
        "a3_future_breakout_seen_flag": ["a3_preview_breakout_raw_flag"],
        "a3_future_persistence_3m_flag": ["a3_preview_persistence_3m_flag"],
        "a3_future_no_quick_return_3m_flag": ["a3_preview_no_quick_return_3m_flag"],
        "a3_quality_future_type_v2": ["a3_aggression_type_v2", "PRICE_BREAKOUT_PERSISTENT"],
        "a3_future_realized_r_proxy_1h": ["a3_preview_realized_r_proxy_1h", "realized_r_proxy"],
    }
    for name, aliases in future_aliases.items():
        register(_spec(name, "future", "zone_truth", "future outcome or quality label", aliases=aliases))

    offline_aliases = {
        "truth_score_max_offline": ["truth_score_max"],
        "truth_score_avg_offline": ["truth_score_avg"],
        "truth_score_median_offline": ["truth_score_median"],
        "truth_score_min_offline": ["truth_score_min"],
        "truth_label_offline": ["truth_label"],
    }
    for name, aliases in offline_aliases.items():
        register(_spec(name, "offline", "zone_truth", "offline audit label", aliases=aliases))

    for name in (
        "a1_vp_setup_rt",
        "vp24h_a1_vp_setup_rt",
        "a2_rt_ready_for_a3_flag",
        "a3_entry_rt_flag",
        "a3_entry_rt_price",
        "a3_entry_rt_ts",
        "a3_entry_rt_direction",
    ):
        register(_spec(name, "rt", "runtime_three_a", "runtime-safe entry input"))

    for name in (
        "realized_r_sim",
        "target_fixed_2r_price_sim",
        "avg_realized_r_sim",
        "median_realized_r_sim",
        "avg_mfe_r_sim",
        "avg_mae_r_sim",
    ):
        register(_spec(name, "sim", "runtime_three_a", "simulated backtest result"))


_register_defaults()
