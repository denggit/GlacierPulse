#!/usr/bin/env python
# -*- coding: utf-8 -*-

from src.strategy.phase1_zone_engine import Phase1Engine
from src.strategy.a1_absorption.engine import A1AbsorptionEngine

from src.strategy.iceberg.zone_tracker import IcebergZoneTracker
from src.strategy.a1_absorption.zone_tracker import A1ZoneTracker

from src.strategy.iceberg.outcome_evaluator import IcebergOutcomeEvaluator
from src.strategy.a1_absorption.outcome_evaluator import A1ZoneOutcomeEvaluator

from src.strategy.phase2_orderflow_evaluator import Phase2OrderflowEvaluator
from src.strategy.a1_reaction.reaction_evaluator import A1ReactionEvaluator

from src.strategy.phase3_candidate_evaluator import Phase3CandidateEvaluator
from src.strategy.execution_research.candidate_evaluator import (
    ExecutionResearchCandidateEvaluator,
)

from src.strategy.virtual_position_manager import VirtualPositionManager
from src.strategy.execution_research.virtual_position_manager import (
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
    assert A1ReactionEvaluator is Phase2OrderflowEvaluator


def test_execution_research_candidate_evaluator_alias():
    assert ExecutionResearchCandidateEvaluator is Phase3CandidateEvaluator


def test_research_virtual_position_manager_alias():
    assert ResearchVirtualPositionManager is VirtualPositionManager


def test_execution_research_outcome_evaluator_alias():
    assert ExecutionResearchOutcomeEvaluator is Phase3OutcomeEvaluator


def test_a1_metadata_reexport_contains_existing_fields():
    assert "frozen_reason" in A1_METADATA_FIELDS
    assert "iceberg_count" in A1_METADATA_FIELDS
    assert "net_score" in A1_METADATA_FIELDS
