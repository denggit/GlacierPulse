"""A1 absorption runtime and research modules for V6.3.x."""

from .engine import A1AbsorptionEngine
from .event_schema import A1AbsorptionContext, A1OutcomeRecord, A1ReactionSnapshot
from .outcome_evaluator import A1OutcomeEvaluator
from .reaction_evaluator import (
    A1ReactionBookSample,
    A1ReactionEvaluator,
    A1ReactionFlowBucket,
    A1ReactionTrackedZone,
)
from .reaction_event_recorder import A1ReactionEventRecorder
from .reaction_taxonomy import (
    A1_REACTION_BREAKOUT_AWAY,
    A1_REACTION_CLEAN_HOLD,
    A1_REACTION_DELAYED_EXPANSION,
    A1_REACTION_FAILED_RECLAIM,
    A1_REACTION_FAST_CLEAN_HOLD,
    A1_REACTION_NO_RESPONSE,
    A1_REACTION_OPPOSITE_A1_CONFLICT,
    A1_REACTION_SWEEP_NO_RECLAIM,
    A1_REACTION_SWEEP_RECLAIM_DIRECT,
    A1_REACTION_SWEEP_RECLAIM_NO_RETEST,
    A1_REACTION_SWEEP_RECLAIM_RETEST,
    A1_REACTION_TIMEOUT,
    A1_REACTION_UNKNOWN,
    legacy_phase2_type_for_reaction,
    normalize_a1_reaction_type,
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
    "A1ReactionEventRecorder",
    "A1_REACTION_CLEAN_HOLD",
    "A1_REACTION_FAST_CLEAN_HOLD",
    "A1_REACTION_SWEEP_RECLAIM_RETEST",
    "A1_REACTION_SWEEP_RECLAIM_DIRECT",
    "A1_REACTION_SWEEP_RECLAIM_NO_RETEST",
    "A1_REACTION_SWEEP_NO_RECLAIM",
    "A1_REACTION_BREAKOUT_AWAY",
    "A1_REACTION_FAILED_RECLAIM",
    "A1_REACTION_OPPOSITE_A1_CONFLICT",
    "A1_REACTION_DELAYED_EXPANSION",
    "A1_REACTION_NO_RESPONSE",
    "A1_REACTION_TIMEOUT",
    "A1_REACTION_UNKNOWN",
    "normalize_a1_reaction_type",
    "legacy_phase2_type_for_reaction",
    "A1AbsorptionContext",
    "A1ReactionSnapshot",
    "A1OutcomeRecord",
    "A1ResearchSample",
    "A1ResearchGroupStats",
    "A1ResearchReport",
    "A1ResearchReportBuilder",
]
