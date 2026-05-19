#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""V6.2 Phase3 candidate research evaluator skeleton."""

import logging
from typing import Any, Dict, Optional

from config.research_evaluator import PHASE3_REAL_TRADING_ENABLED

logger = logging.getLogger(__name__)


class Phase3CandidateEvaluator:
    def __init__(self, real_trading_enabled: bool = PHASE3_REAL_TRADING_ENABLED):
        self.real_trading_enabled = bool(real_trading_enabled)

    def evaluate(self, candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Reserved for research-only candidate scoring in later V6.2 steps."""
        if self.real_trading_enabled:
            logger.warning("[PHASE3-REAL-TRADING-BLOCKED] reason=v62_research_step1")
        return None
