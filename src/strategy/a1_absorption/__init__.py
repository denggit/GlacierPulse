"""Semantic package exports for A1 iceberg absorption research modules."""

from .engine import A1AbsorptionEngine
from .outcome_evaluator import A1ZoneOutcomeEvaluator
from .schema import A1AbsorptionContext, A1OutcomeRecord, A1ReactionSnapshot
from .research_report import (
    A1ResearchGroupStats,
    A1ResearchReport,
    A1ResearchReportBuilder,
    A1ResearchSample,
)
from .score_model import (
    A1ScoreBreakdown,
    A1ScoreRecord,
    A1UnifiedScoreModel,
    bucket_for_score,
)
from .zone_tracker import A1ZoneTracker

__all__ = [
    "A1AbsorptionEngine",
    "A1ZoneTracker",
    "A1ZoneOutcomeEvaluator",
    "A1AbsorptionContext",
    "A1ReactionSnapshot",
    "A1OutcomeRecord",
    "A1ResearchSample",
    "A1ResearchGroupStats",
    "A1ResearchReport",
    "A1ResearchReportBuilder",
    "A1ScoreBreakdown",
    "A1ScoreRecord",
    "A1UnifiedScoreModel",
    "bucket_for_score",
]
