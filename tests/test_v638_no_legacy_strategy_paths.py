import importlib.util
import os
import sys
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


LEGACY_MODULES = (
    "src.strategy.phase1_zone_engine",
    "src.strategy.phase2_orderflow_evaluator",
    "src.strategy.phase3_candidate_evaluator",
    "src.strategy.phase3_trade_outcome_evaluator",
    "src.strategy.virtual_position_manager",
    "src.strategy.execution_research.candidate_evaluator",
    "src.strategy.a1_reaction",
    "src.strategy.iceberg.zone_tracker",
    "src.strategy.iceberg.outcome_evaluator",
    "src.strategy.a1_absorption.score_model",
    "src.strategy.a1_absorption.schema",
)

LEGACY_FILES = (
    "src/strategy/phase1_zone_engine.py",
    "src/strategy/phase2_orderflow_evaluator.py",
    "src/strategy/phase3_candidate_evaluator.py",
    "src/strategy/phase3_trade_outcome_evaluator.py",
    "src/strategy/virtual_position_manager.py",
    "src/strategy/execution_research/candidate_evaluator.py",
    "src/strategy/a1_reaction",
    "src/strategy/iceberg/zone_tracker.py",
    "src/strategy/iceberg/outcome_evaluator.py",
    "src/strategy/a1_absorption/score_model.py",
    "src/strategy/a1_absorption/schema.py",
)


def _find_spec_or_none(module_name: str):
    try:
        return importlib.util.find_spec(module_name)
    except ModuleNotFoundError:
        return None


def test_v638_legacy_strategy_modules_are_not_importable():
    for module_name in LEGACY_MODULES:
        assert _find_spec_or_none(module_name) is None


def test_v638_legacy_strategy_paths_are_removed():
    root = Path(__file__).resolve().parents[1]
    for relative_path in LEGACY_FILES:
        assert not (root / relative_path).exists()
