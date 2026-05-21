"""A1 absorption runtime and research modules for V6.3.8."""

from .engine import A1AbsorptionEngine
from .event_schema import A1AbsorptionContext, A1OutcomeRecord, A1ReactionSnapshot
from .outcome_evaluator import A1OutcomeEvaluator
from .reaction_evaluator import (
    A1ReactionBookSample,
    A1ReactionEvaluator,
    A1ReactionFlowBucket,
    A1ReactionTrackedZone,
)
from .research_report import (
    A1ResearchGroupStats,
    A1ResearchReport,
    A1ResearchReportBuilder,
    A1ResearchSample,
)
from .zone_tracker import A1ZoneTracker

__all__ = [
    "A1AbsorptionEngine",
    "A1ZoneTracker",
    "A1OutcomeEvaluator",
    "A1ReactionEvaluator",
    "A1ReactionTrackedZone",
    "A1ReactionFlowBucket",
    "A1ReactionBookSample",
    "A1AbsorptionContext",
    "A1ReactionSnapshot",
    "A1OutcomeRecord",
    "A1ResearchSample",
    "A1ResearchGroupStats",
    "A1ResearchReport",
    "A1ResearchReportBuilder",
]
