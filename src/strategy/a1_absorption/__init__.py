"""Semantic aliases for current A1 iceberg absorption research modules."""

from .engine import A1AbsorptionEngine
from .outcome_evaluator import A1ZoneOutcomeEvaluator
from .schema import A1AbsorptionContext, A1OutcomeRecord, A1ReactionSnapshot
from .zone_tracker import A1ZoneTracker

__all__ = [
    "A1AbsorptionEngine",
    "A1ZoneTracker",
    "A1ZoneOutcomeEvaluator",
    "A1AbsorptionContext",
    "A1ReactionSnapshot",
    "A1OutcomeRecord",
]
