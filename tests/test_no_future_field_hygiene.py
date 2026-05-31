#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from src.research.field_registry import field_hygiene_summary, is_deprecated_lookahead_alias
from src.research.no_future_audit import validate_entry_conditions


@pytest.mark.parametrize(
    "field",
    [
        "a3_future_ignition_quality",
        "a3_preview_ignition_quality",
        "a2_compression_state_future",
        "a2_compression_state",
        "truth_score_avg_offline",
        "truth_score_avg",
        "realized_r_sim",
        "mfe_15m_u_future",
        "mae_15m_u_future",
        "PRICE_BREAKOUT_PERSISTENT",
        "A2_COMPRESSION",
    ],
)
def test_no_future_audit_rejects_future_offline_sim_and_aliases(field):
    with pytest.raises(ValueError):
        validate_entry_conditions([field])


def test_no_future_audit_allows_runtime_fields():
    validate_entry_conditions(
        [
            "a1_vp_setup_rt",
            "vp24h_a1_vp_setup_rt",
            "a2_rt_ready_for_a3_flag",
            "a3_entry_rt_flag",
            "a3_entry_rt_price",
            "a3_entry_rt_ts",
        ]
    )


def test_field_hygiene_counts_suffixes_and_aliases():
    summary = field_hygiene_summary(["a2_rt_state", "mfe_15m_u_future", "truth_score_avg_offline", "realized_r_sim"])
    assert summary["rt_field_count"] == 1
    assert summary["future_field_count"] == 1
    assert summary["offline_field_count"] == 1
    assert summary["sim_field_count"] == 1
    assert is_deprecated_lookahead_alias("a3_preview_ignition_quality")

