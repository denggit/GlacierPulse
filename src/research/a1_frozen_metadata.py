#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A1 frozen-zone metadata field schema for research outputs."""

A1_METADATA_FIELDS = (
    "frozen_reason",
    "frozen_state",
    "frozen_event_id",
    "event_count",
    "iceberg_count",
    "ignore_count",
    "spoof_count",
    "cancel_count",
    "high_count",
    "medium_count",
    "low_count",
    "positive_score",
    "negative_score",
    "net_score",
)

A1_STRING_METADATA_FIELDS = (
    "frozen_reason",
    "frozen_state",
    "frozen_event_id",
)

A1_COUNT_METADATA_FIELDS = (
    "event_count",
    "iceberg_count",
    "ignore_count",
    "spoof_count",
    "cancel_count",
    "high_count",
    "medium_count",
    "low_count",
)

A1_SCORE_METADATA_FIELDS = (
    "positive_score",
    "negative_score",
    "net_score",
)
