import inspect
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategy.a1_absorption import research_report
from src.strategy.a1_absorption.research_report import A1ResearchReportBuilder


def test_v638_research_report_has_no_score_model_dependency():
    source = inspect.getsource(research_report)
    assert "A1ScoreRecord" not in source
    assert "A1UnifiedScoreModel" not in source
    assert "score_model" not in source


def test_v638_research_report_builds_from_raw_a1_mapping():
    report = A1ResearchReportBuilder.build([
        {
            "zone_id": "z-report",
            "direction": "BUY",
            "frozen_reason": "HIGH_ICEBERG",
            "frozen_state": "DISCOVERED",
            "a1_reaction_type": "SWEEP_RECLAIM",
            "phase2_type": "SWEEP_RECLAIM",
            "candidate_type": "SWEEP_RECLAIM_RETEST_ENTRY",
            "iceberg_count": 2,
            "high_count": 1,
            "net_score": 3.2,
            "relevant_book_depth_available": True,
            "reload_score": 0.3,
            "absorption_score": 0.8,
            "reclaim_score": 0.7,
            "retest_score": 0.6,
            "realized_r_multiple": 1.1,
        }
    ])
    assert report.sample_count == 1
    assert "HIGH_ICEBERG" in report.groups["frozen_reason"]
    assert "SWEEP_RECLAIM" in report.groups["a1_reaction_type"]
    assert "SWEEP_RECLAIM" in report.groups["legacy_phase2_type"]
