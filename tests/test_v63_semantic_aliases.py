#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.strategy.phase1_zone_engine import Phase1Engine
from src.strategy import phase1_zone_engine
from src.strategy.a1_absorption.engine import A1AbsorptionEngine

from src.strategy.iceberg.zone_tracker import IcebergZoneTracker
from src.strategy.a1_absorption.zone_tracker import A1ZoneTracker

from src.strategy.iceberg.outcome_evaluator import IcebergOutcomeEvaluator
from src.strategy.a1_absorption.outcome_evaluator import A1ZoneOutcomeEvaluator

from src.strategy.phase2_orderflow_evaluator import (
    Phase2BookSample as LegacyPhase2BookSample,
    Phase2FlowBucket as LegacyPhase2FlowBucket,
    Phase2OrderflowEvaluator as LegacyPhase2OrderflowEvaluator,
    Phase2TrackedZone as LegacyPhase2TrackedZone,
)
from src.strategy.a1_reaction import (
    A1ReactionEvaluator as PackageA1ReactionEvaluator,
    Phase2OrderflowEvaluator as PackagePhase2OrderflowEvaluator,
)
from src.strategy.a1_reaction.reaction_evaluator import (
    A1ReactionBookSample,
    A1ReactionEvaluator,
    A1ReactionFlowBucket,
    A1ReactionTrackedZone,
    Phase2BookSample,
    Phase2FlowBucket,
    Phase2OrderflowEvaluator as NewPathPhase2OrderflowEvaluator,
    Phase2TrackedZone,
)

from src.strategy.execution_research import (
    ExecutionResearchCandidateEvaluator as PackageExecutionResearchCandidateEvaluator,
    ExecutionResearchOutcomeEvaluator as PackageExecutionResearchOutcomeEvaluator,
    Phase3CandidateEvaluator as PackagePhase3CandidateEvaluator,
    Phase3OutcomeEvaluator as PackagePhase3OutcomeEvaluator,
    ResearchVirtualPosition as PackageResearchVirtualPosition,
    ResearchVirtualPositionManager as PackageResearchVirtualPositionManager,
    VirtualPosition as PackageVirtualPosition,
    VirtualPositionManager as PackageVirtualPositionManager,
)
from src.strategy.phase3_candidate_evaluator import (
    Phase3CandidateEvaluator as LegacyPhase3CandidateEvaluator,
)
from src.strategy.execution_research.candidate_evaluator import (
    ExecutionResearchCandidateEvaluator,
    Phase3CandidateEvaluator as NewPathPhase3CandidateEvaluator,
)

from src.strategy.virtual_position_manager import (
    VirtualPosition,
    VirtualPositionManager,
)
from src.strategy.execution_research.virtual_position_manager import (
    ResearchVirtualPosition,
    ResearchVirtualPositionManager,
)

from src.strategy.phase3_trade_outcome_evaluator import Phase3OutcomeEvaluator
from src.strategy.execution_research.trade_outcome_evaluator import (
    ExecutionResearchOutcomeEvaluator,
)

from src.strategy.a1_absorption.metadata import A1_METADATA_FIELDS


def test_a1_absorption_engine_alias():
    assert A1AbsorptionEngine is Phase1Engine


def test_a1_zone_tracker_alias():
    assert A1ZoneTracker is IcebergZoneTracker


def test_a1_zone_outcome_evaluator_alias():
    assert A1ZoneOutcomeEvaluator is IcebergOutcomeEvaluator


def test_a1_reaction_evaluator_alias():
    assert A1ReactionEvaluator is LegacyPhase2OrderflowEvaluator


def test_a1_reaction_new_path_is_runtime_implementation():
    assert A1ReactionEvaluator is NewPathPhase2OrderflowEvaluator
    assert LegacyPhase2OrderflowEvaluator is NewPathPhase2OrderflowEvaluator


def test_a1_reaction_dataclass_aliases_match_legacy_exports():
    assert A1ReactionTrackedZone is Phase2TrackedZone
    assert A1ReactionFlowBucket is Phase2FlowBucket
    assert A1ReactionBookSample is Phase2BookSample
    assert LegacyPhase2TrackedZone is Phase2TrackedZone
    assert LegacyPhase2FlowBucket is Phase2FlowBucket
    assert LegacyPhase2BookSample is Phase2BookSample


def test_execution_research_candidate_evaluator_alias():
    assert ExecutionResearchCandidateEvaluator is NewPathPhase3CandidateEvaluator
    assert LegacyPhase3CandidateEvaluator is NewPathPhase3CandidateEvaluator


def test_research_virtual_position_manager_alias():
    assert ResearchVirtualPositionManager is VirtualPositionManager


def test_execution_research_outcome_evaluator_alias():
    assert ExecutionResearchOutcomeEvaluator is Phase3OutcomeEvaluator


def test_a1_metadata_reexport_contains_existing_fields():
    assert "frozen_reason" in A1_METADATA_FIELDS
    assert "iceberg_count" in A1_METADATA_FIELDS
    assert "net_score" in A1_METADATA_FIELDS


def test_a1_reaction_package_exports_match_new_path():
    assert PackageA1ReactionEvaluator is A1ReactionEvaluator
    assert PackagePhase2OrderflowEvaluator is NewPathPhase2OrderflowEvaluator


def test_execution_research_package_exports_match_new_path():
    assert PackageExecutionResearchCandidateEvaluator is ExecutionResearchCandidateEvaluator
    assert PackagePhase3CandidateEvaluator is NewPathPhase3CandidateEvaluator


def test_execution_research_package_exports_virtual_position_names():
    assert PackageResearchVirtualPositionManager is ResearchVirtualPositionManager
    assert PackageResearchVirtualPosition is ResearchVirtualPosition
    assert PackageVirtualPositionManager is VirtualPositionManager
    assert PackageVirtualPosition is VirtualPosition


def test_execution_research_package_exports_outcome_names():
    assert PackageExecutionResearchOutcomeEvaluator is ExecutionResearchOutcomeEvaluator
    assert PackagePhase3OutcomeEvaluator is Phase3OutcomeEvaluator


def test_phase1_engine_runtime_imports_use_semantic_paths():
    assert phase1_zone_engine.A1ReactionEvaluator is A1ReactionEvaluator
    assert (
        phase1_zone_engine.ExecutionResearchCandidateEvaluator
        is ExecutionResearchCandidateEvaluator
    )
