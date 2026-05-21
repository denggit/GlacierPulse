from pathlib import Path


def test_main_loads_runtime_profile_before_config_imports():
    text = Path("main.py").read_text(encoding="utf-8")

    assert "load_runtime_profile" in text
    assert "tools/run_with_profile.py" not in text
    assert text.index("load_runtime_profile()") < text.index("from config import research_evaluator")


def test_main_runtime_profile_loader_stays_near_top():
    lines = Path("main.py").read_text(encoding="utf-8").splitlines()
    call_line = next(
        index
        for index, line in enumerate(lines, start=1)
        if line.strip() == "load_runtime_profile()"
    )

    assert call_line <= 25
