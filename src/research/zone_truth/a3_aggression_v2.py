#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Mapping

from .a3_quality_future_v2 import attach_a3_quality_future_v2, classify_a3_quality_future_v2


def classify_a3_aggression_v2(row: Mapping[str, Any]) -> dict[str, Any]:
    """Deprecated compatibility wrapper for the V7.3 future quality label."""
    out = classify_a3_quality_future_v2(row)
    return {
        **out,
        "a3_aggression_type_v2": out["a3_quality_future_type_v2"],
        "a3_aggression_score_v2": out["a3_quality_future_score_v2"],
        "a3_aggression_reason_v2": "DEPRECATED_LOOKAHEAD_ALIAS|" + str(out["a3_quality_future_reason_v2"]),
    }


def attach_a3_aggression_v2(row: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out.update(classify_a3_aggression_v2(out))
    return out

