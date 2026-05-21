#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Load runtime JSON profiles into environment variables before config import."""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


PROFILE_PATH_ENV = "GLACIER_RUNTIME_PROFILE"
DEFAULT_PROFILE_PATH = "config/runtime_profile.json"


@dataclass(frozen=True)
class RuntimeProfileLoadResult:
    path: str
    loaded: bool
    applied_count: int = 0
    skipped_existing_count: int = 0
    skipped_invalid_count: int = 0
    skipped_null_count: int = 0
    profile_name: str = ""
    allow_override_env: bool = False


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_profile_path(profile_path: str | os.PathLike[str] | None) -> Path:
    raw_path = str(profile_path or os.environ.get(PROFILE_PATH_ENV) or DEFAULT_PROFILE_PATH)
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path
    return _project_root() / path


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(_project_root()).as_posix()
    except ValueError:
        return str(path)


def _emit(message: str) -> None:
    print(message)


def _bool_from_profile(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("1", "true", "yes", "on"):
            return True
        if text in ("0", "false", "no", "off"):
            return False
    return default


def _profile_env(profile: Mapping[str, Any]) -> tuple[Mapping[str, Any], str, bool]:
    if "env" in profile:
        env_value = profile.get("env")
        if not isinstance(env_value, Mapping):
            raise RuntimeError("[RUNTIME-PROFILE-ERROR] env must be a JSON object")
        name = str(profile.get("name") or "")
        allow_override_env = _bool_from_profile(profile.get("allow_override_env"), False)
        return env_value, name, allow_override_env
    return profile, str(profile.get("name") or ""), False


def _stringify_env_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, str):
        return value
    return None


def load_runtime_profile(
    profile_path: str | os.PathLike[str] | None = None,
) -> RuntimeProfileLoadResult:
    """Load a JSON runtime profile and inject supported values into os.environ."""

    path = _resolve_profile_path(profile_path)
    display_path = _display_path(path)

    if not path.exists():
        _emit(
            "[RUNTIME-PROFILE] no profile file found "
            f"path={display_path} using_env_and_code_defaults=true"
        )
        return RuntimeProfileLoadResult(path=display_path, loaded=False)

    try:
        with path.open("r", encoding="utf-8") as f:
            profile = json.load(f)
    except json.JSONDecodeError as exc:
        message = f"[RUNTIME-PROFILE-ERROR] invalid json path={display_path} error={exc}"
        _emit(message)
        raise RuntimeError(message) from exc

    if not isinstance(profile, Mapping):
        message = f"[RUNTIME-PROFILE-ERROR] profile root must be a JSON object path={display_path}"
        _emit(message)
        raise RuntimeError(message)

    env_values, profile_name, allow_override_env = _profile_env(profile)
    applied_count = 0
    skipped_existing_count = 0
    skipped_invalid_count = 0
    skipped_null_count = 0

    for key, value in env_values.items():
        if not isinstance(key, str):
            skipped_invalid_count += 1
            _emit(f"[RUNTIME-PROFILE-WARN] skip non-string key={key!r}")
            continue

        if value is None:
            skipped_null_count += 1
            continue

        env_value = _stringify_env_value(value)
        if env_value is None:
            skipped_invalid_count += 1
            _emit(
                "[RUNTIME-PROFILE-WARN] skip unsupported value "
                f"key={key} type={type(value).__name__}"
            )
            continue

        if key in os.environ and not allow_override_env:
            skipped_existing_count += 1
            _emit(f"[RUNTIME-PROFILE] skip existing env key={key}")
            continue

        os.environ[key] = env_value
        applied_count += 1

    _emit(
        "[RUNTIME-PROFILE] loaded "
        f"path={display_path} "
        f"profile={profile_name or 'unnamed'} "
        f"applied={applied_count} "
        f"skipped_existing={skipped_existing_count} "
        f"skipped_invalid={skipped_invalid_count} "
        f"override_env={'true' if allow_override_env else 'false'}"
    )

    return RuntimeProfileLoadResult(
        path=display_path,
        loaded=True,
        applied_count=applied_count,
        skipped_existing_count=skipped_existing_count,
        skipped_invalid_count=skipped_invalid_count,
        skipped_null_count=skipped_null_count,
        profile_name=profile_name,
        allow_override_env=allow_override_env,
    )
