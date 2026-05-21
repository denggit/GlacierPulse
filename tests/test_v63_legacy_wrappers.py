from src.strategy.phase2_orderflow_evaluator import (
    Phase2BookSample,
    Phase2FlowBucket,
    Phase2OrderflowEvaluator,
    Phase2TrackedZone,
)
from src.strategy.a1_reaction.reaction_evaluator import (
    A1ReactionBookSample,
    A1ReactionEvaluator,
    A1ReactionFlowBucket,
    A1ReactionTrackedZone,
)


def test_legacy_phase2_orderflow_evaluator_wrapper_points_to_a1_reaction():
    assert Phase2OrderflowEvaluator is A1ReactionEvaluator


def test_legacy_phase2_orderflow_evaluator_can_instantiate():
    evaluator = Phase2OrderflowEvaluator()
    assert evaluator is not None
    assert hasattr(evaluator, "register_frozen_zone")
    assert hasattr(evaluator, "on_trade")
    assert hasattr(evaluator, "on_book_update")
    assert hasattr(evaluator, "pop_confirmed_events")


def test_legacy_phase2_wrapper_exports_objects_from_a1_reaction_module():
    assert Phase2OrderflowEvaluator.__module__ == "src.strategy.a1_reaction.reaction_evaluator"
    assert Phase2TrackedZone.__module__ == "src.strategy.a1_reaction.reaction_evaluator"
    assert Phase2FlowBucket.__module__ == "src.strategy.a1_reaction.reaction_evaluator"
    assert Phase2BookSample.__module__ == "src.strategy.a1_reaction.reaction_evaluator"


def test_legacy_phase2_wrapper_dataclass_exports_point_to_a1_reaction():
    assert Phase2TrackedZone is A1ReactionTrackedZone
    assert Phase2FlowBucket is A1ReactionFlowBucket
    assert Phase2BookSample is A1ReactionBookSample
