#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Deprecated compatibility wrapper for the V6.3 execution research candidate evaluator.

The implementation now lives in:
src.strategy.execution_research.candidate_evaluator

Legacy imports are intentionally preserved:
from src.strategy.phase3_candidate_evaluator import Phase3CandidateEvaluator
"""

from src.strategy.execution_research.candidate_evaluator import (
    ExecutionResearchCandidateEvaluator,
    Phase3CandidateEvaluator,
)

__all__ = [
    "ExecutionResearchCandidateEvaluator",
    "Phase3CandidateEvaluator",
]
