#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""V6.2 Phase3 outcome research evaluator skeleton."""

from typing import Any, Dict, List


class Phase3OutcomeEvaluator:
    def __init__(self):
        self.outcomes: List[Dict[str, Any]] = []

    def record(self, outcome: Dict[str, Any]) -> None:
        self.outcomes.append(dict(outcome))
