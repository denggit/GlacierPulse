#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Deprecated compatibility wrapper for the V6.3 A1 reaction evaluator.

The implementation now lives in:
src.strategy.a1_reaction.reaction_evaluator

Legacy imports are intentionally preserved:
from src.strategy.phase2_orderflow_evaluator import Phase2OrderflowEvaluator
"""

from src.strategy.a1_reaction.reaction_evaluator import (
    A1ReactionBookSample,
    A1ReactionEvaluator,
    A1ReactionFlowBucket,
    A1ReactionTrackedZone,
    Phase2BookSample,
    Phase2FlowBucket,
    Phase2OrderflowEvaluator,
    Phase2TrackedZone,
)

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
