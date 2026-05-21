import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.strategy.a1_absorption.research_report import A1ResearchSample


def _parsed_bool(value):
    return A1ResearchSample.from_mapping(
        {
            "zone_id": "z-bool",
            "direction": "BUY",
            "relevant_book_depth_available": value,
        }
    ).relevant_book_depth_available


def test_research_report_relevant_book_depth_bool_parsing():
    assert _parsed_bool(True) == "True"
    assert _parsed_bool(False) == "False"
    assert _parsed_bool("true") == "True"
    assert _parsed_bool("false") == "False"
    assert _parsed_bool("0") == "False"
    assert _parsed_bool("1") == "True"
    assert _parsed_bool(None) == "False"
