import importlib
import json
import os
from pathlib import Path

import pytest

from src.config.runtime_profile_loader import load_runtime_profile


def _write_profile(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_missing_profile_file_does_not_error(tmp_path, capsys):
    result = load_runtime_profile(tmp_path / "missing.json")

    assert result.loaded is False
    assert result.applied_count == 0
    assert "no profile file found" in capsys.readouterr().out


def test_direct_key_value_profile_injects_env(tmp_path, monkeypatch):
    monkeypatch.delenv("V63102_DIRECT_BOOL", raising=False)
    monkeypatch.delenv("V63102_DIRECT_TEXT", raising=False)
    path = _write_profile(
        tmp_path / "profile.json",
        {
            "V63102_DIRECT_BOOL": True,
            "V63102_DIRECT_TEXT": "abc",
        },
    )

    result = load_runtime_profile(path)

    assert result.loaded is True
    assert result.applied_count == 2
    assert os.environ["V63102_DIRECT_BOOL"] == "true"
    assert os.environ["V63102_DIRECT_TEXT"] == "abc"


def test_nested_env_profile_injects_env(tmp_path, monkeypatch):
    monkeypatch.delenv("V63102_NESTED_BOOL", raising=False)
    path = _write_profile(
        tmp_path / "profile.json",
        {
            "name": "nested",
            "description": "test",
            "allow_override_env": False,
            "env": {
                "V63102_NESTED_BOOL": False,
            },
        },
    )

    result = load_runtime_profile(path)

    assert result.profile_name == "nested"
    assert result.applied_count == 1
    assert os.environ["V63102_NESTED_BOOL"] == "false"


def test_supported_value_types_are_stringified(tmp_path, monkeypatch):
    keys = [
        "V63102_BOOL_TRUE",
        "V63102_BOOL_FALSE",
        "V63102_INT",
        "V63102_FLOAT",
        "V63102_STRING",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    path = _write_profile(
        tmp_path / "profile.json",
        {
            "env": {
                "V63102_BOOL_TRUE": True,
                "V63102_BOOL_FALSE": False,
                "V63102_INT": 123,
                "V63102_FLOAT": 1.5,
                "V63102_STRING": "abc",
            },
        },
    )

    result = load_runtime_profile(path)

    assert result.applied_count == 5
    assert os.environ["V63102_BOOL_TRUE"] == "true"
    assert os.environ["V63102_BOOL_FALSE"] == "false"
    assert os.environ["V63102_INT"] == "123"
    assert os.environ["V63102_FLOAT"] == "1.5"
    assert os.environ["V63102_STRING"] == "abc"


def test_null_dict_and_list_values_are_skipped(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("V63102_NULL", raising=False)
    monkeypatch.delenv("V63102_DICT", raising=False)
    monkeypatch.delenv("V63102_LIST", raising=False)
    path = _write_profile(
        tmp_path / "profile.json",
        {
            "env": {
                "V63102_NULL": None,
                "V63102_DICT": {"x": 1},
                "V63102_LIST": [1, 2],
            },
        },
    )

    result = load_runtime_profile(path)

    assert "V63102_NULL" not in os.environ
    assert "V63102_DICT" not in os.environ
    assert "V63102_LIST" not in os.environ
    assert result.applied_count == 0
    assert result.skipped_null_count == 1
    assert result.skipped_invalid_count == 2
    output = capsys.readouterr().out
    assert "skip unsupported value key=V63102_DICT type=dict" in output
    assert "skip unsupported value key=V63102_LIST type=list" in output


def test_existing_env_wins_by_default(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("V63102_EXISTING", "shell")
    path = _write_profile(
        tmp_path / "profile.json",
        {
            "env": {
                "V63102_EXISTING": "json",
            },
        },
    )

    result = load_runtime_profile(path)

    assert os.environ["V63102_EXISTING"] == "shell"
    assert result.skipped_existing_count == 1
    assert "skip existing env key=V63102_EXISTING" in capsys.readouterr().out


def test_allow_override_env_true_overwrites_existing_env(tmp_path, monkeypatch):
    monkeypatch.setenv("V63102_OVERRIDE", "shell")
    path = _write_profile(
        tmp_path / "profile.json",
        {
            "allow_override_env": True,
            "env": {
                "V63102_OVERRIDE": "json",
            },
        },
    )

    result = load_runtime_profile(path)

    assert os.environ["V63102_OVERRIDE"] == "json"
    assert result.applied_count == 1
    assert result.skipped_existing_count == 0
    assert result.allow_override_env is True


def test_glacier_runtime_profile_selects_path(tmp_path, monkeypatch):
    monkeypatch.delenv("V63102_SELECTED", raising=False)
    path = _write_profile(
        tmp_path / "selected.json",
        {
            "env": {
                "V63102_SELECTED": "yes",
            },
        },
    )
    monkeypatch.setenv("GLACIER_RUNTIME_PROFILE", str(path))

    result = load_runtime_profile()

    assert result.loaded is True
    assert os.environ["V63102_SELECTED"] == "yes"
    assert result.path == str(path)


def test_invalid_json_raises_clear_error(tmp_path, capsys):
    path = tmp_path / "broken.json"
    path.write_text("{broken", encoding="utf-8")

    with pytest.raises(RuntimeError, match="invalid json"):
        load_runtime_profile(path)

    assert "[RUNTIME-PROFILE-ERROR] invalid json" in capsys.readouterr().out


def test_result_contains_expected_fields(tmp_path, monkeypatch):
    monkeypatch.delenv("V63102_RESULT", raising=False)
    path = _write_profile(tmp_path / "profile.json", {"V63102_RESULT": "ok"})

    result = load_runtime_profile(path)

    assert result.path == str(path)
    assert result.loaded is True
    assert result.applied_count == 1
    assert result.skipped_existing_count == 0
    assert result.skipped_invalid_count == 0


def test_loader_import_has_no_main_trader_okx_or_websocket_dependency():
    loader = importlib.import_module("src.config.runtime_profile_loader")
    source = Path(loader.__file__).read_text(encoding="utf-8").lower()

    assert "import main" not in source
    assert "trader" not in source
    assert "okx" not in source
    assert "websocket" not in source
