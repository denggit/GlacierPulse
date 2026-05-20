#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A1 reaction aliases for the current Phase2 orderflow evaluator."""

from src.strategy.phase2_orderflow_evaluator import (
    Phase2BookSample,
    Phase2FlowBucket,
    Phase2OrderflowEvaluator,
    Phase2TrackedZone,
)

A1ReactionEvaluator = Phase2OrderflowEvaluator
A1ReactionTrackedZone = Phase2TrackedZone
A1ReactionFlowBucket = Phase2FlowBucket
A1ReactionBookSample = Phase2BookSample

__all__ = [
    "A1ReactionEvaluator",
    "A1ReactionTrackedZone",
    "A1ReactionFlowBucket",
    "A1ReactionBookSample",
    "Phase2OrderflowEvaluator",
    "Phase2TrackedZone",
    "Phase2FlowBucket",
    "Phase2BookSample",
]

