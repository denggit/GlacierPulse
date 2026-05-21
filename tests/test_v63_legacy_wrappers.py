from src.strategy.phase2_orderflow_evaluator import Phase2OrderflowEvaluator
from src.strategy.a1_reaction.reaction_evaluator import A1ReactionEvaluator


def test_legacy_phase2_orderflow_evaluator_wrapper_points_to_a1_reaction():
    assert Phase2OrderflowEvaluator is A1ReactionEvaluator


def test_legacy_phase2_orderflow_evaluator_can_instantiate():
    evaluator = Phase2OrderflowEvaluator()
    assert evaluator is not None
    assert hasattr(evaluator, "register_frozen_zone")
    assert hasattr(evaluator, "on_trade")
    assert hasattr(evaluator, "on_book_update")
    assert hasattr(evaluator, "pop_confirmed_events")
