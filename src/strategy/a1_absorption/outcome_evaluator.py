#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A1 zone outcome evaluator alias for the current iceberg outcome evaluator."""

from src.strategy.iceberg.outcome_evaluator import IcebergOutcomeEvaluator

A1ZoneOutcomeEvaluator = IcebergOutcomeEvaluator

__all__ = ["A1ZoneOutcomeEvaluator", "IcebergOutcomeEvaluator"]

