import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import research_evaluator as cfg
import main


def test_v638_real_trading_flags_disabled_by_default():
    assert cfg.REAL_EXECUTION_ENABLED is False
    assert cfg.PHASE3_REAL_TRADING_ENABLED is False
    assert main.A1_SINGLE_EVENT_TRADING_ENABLED is False
