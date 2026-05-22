#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from config import research_evaluator as cfg

from .session import TIMEZONE, session_info

logger = logging.getLogger(__name__)


STATIC_BASE = {
    "min_event_start_notional_usdt": 300000.0,
    "min_hidden_notional_usdt": 1000000.0,
    "min_local_depth_usdt": 300000.0,
    "local_zone_width": 1.5,
    "max_wait_ms": 700.0,
}

SESSION_MULTIPLIERS = {
    "ASIA_OFF": {"liquidity_factor": 0.75, "volatility_factor": 0.85, "max_wait_ms": 900.0},
    "ASIA_DAY": {"liquidity_factor": 0.90, "volatility_factor": 0.90, "max_wait_ms": 800.0},
    "EUROPE_PRE_US": {"liquidity_factor": 1.00, "volatility_factor": 1.00, "max_wait_ms": 700.0},
    "US_LATE": {"liquidity_factor": 1.00, "volatility_factor": 1.10, "max_wait_ms": 700.0},
    "US_OPEN": {"liquidity_factor": 2.00, "volatility_factor": 1.60, "max_wait_ms": 500.0},
}

LIMITS = {
    "min_event_start_notional_usdt": (150000.0, 1500000.0),
    "min_hidden_notional_usdt": (500000.0, 4000000.0),
    "min_local_depth_usdt": (150000.0, 1500000.0),
    "local_zone_width": (1.0, 4.0),
    "max_wait_ms": (300.0, 1200.0),
}


class A1DynamicParamPreviewer:
    def __init__(
        self,
        enabled: bool = True,
        json_path: str = "runtime_state/a1_dynamic_params.json",
        interval_sec: float = 300.0,
        mode: str = "preview_only",
        symbol: str = "ETH-USDT-SWAP",
        timezone_name: str = TIMEZONE,
    ) -> None:
        self.enabled = bool(enabled)
        self.json_path = Path(json_path)
        self.interval_sec = float(interval_sec)
        self.mode = str(mode or "preview_only")
        self.symbol = symbol
        self.timezone_name = timezone_name
        self._last_write_ts = 0.0

    @classmethod
    def from_config(cls, symbol: str = "ETH-USDT-SWAP") -> "A1DynamicParamPreviewer":
        try:
            return cls(
                enabled=bool(getattr(cfg, "A1_DYNAMIC_PARAM_PREVIEW_ENABLED", True)),
                json_path=str(getattr(cfg, "A1_DYNAMIC_PARAM_PREVIEW_JSON_PATH", "runtime_state/a1_dynamic_params.json")),
                interval_sec=float(getattr(cfg, "A1_DYNAMIC_PARAM_PREVIEW_INTERVAL_SEC", 300.0)),
                mode=str(getattr(cfg, "A1_DYNAMIC_PARAM_MODE", "preview_only")),
                symbol=symbol,
            )
        except Exception as exc:
            logger.warning("[PHASE1-TRUTH-FALLBACK] reason=dynamic_preview_config_failed error=%s", exc)
            return cls(enabled=False)

    def maybe_write(self, now_ts: float | None = None, force: bool = False, source_stats: Mapping[str, Any] | None = None) -> bool:
        if not self.enabled:
            return False
        now = float(now_ts or time.time())
        if not force and self._last_write_ts and now - self._last_write_ts < self.interval_sec:
            return False
        try:
            payload = self.build_payload(now, source_stats=source_stats)
            self._atomic_write(payload)
            self._last_write_ts = now
            preview = payload["dynamic_preview_params"]
            logger.info(
                "[A1-DYNAMIC-PREVIEW] mode=%s active_source=%s session=%s preview_start=%.0f preview_hidden=%.0f",
                payload["mode"],
                payload["active_params"]["source"],
                payload["session_tag"],
                float(preview["min_event_start_notional_usdt"]),
                float(preview["min_hidden_notional_usdt"]),
            )
            return True
        except Exception as exc:
            logger.warning("[PHASE1-TRUTH-FALLBACK] reason=dynamic_preview_write_failed error=%s", exc)
            return False

    def build_payload(self, now_ts: float | None = None, source_stats: Mapping[str, Any] | None = None) -> dict[str, Any]:
        now = float(now_ts or time.time())
        info = session_info(now, self.timezone_name)
        tag = str(info["session_tag"])
        multiplier = SESSION_MULTIPLIERS.get(tag, SESSION_MULTIPLIERS["EUROPE_PRE_US"])
        dynamic = {
            "min_event_start_notional_usdt": _clamp(
                STATIC_BASE["min_event_start_notional_usdt"] * multiplier["liquidity_factor"],
                *LIMITS["min_event_start_notional_usdt"],
            ),
            "min_hidden_notional_usdt": _clamp(
                STATIC_BASE["min_hidden_notional_usdt"] * multiplier["liquidity_factor"],
                *LIMITS["min_hidden_notional_usdt"],
            ),
            "min_local_depth_usdt": _clamp(
                STATIC_BASE["min_local_depth_usdt"] * multiplier["liquidity_factor"],
                *LIMITS["min_local_depth_usdt"],
            ),
            "local_zone_width": _clamp(
                STATIC_BASE["local_zone_width"] * multiplier["volatility_factor"],
                *LIMITS["local_zone_width"],
            ),
            "max_wait_ms": _clamp(multiplier["max_wait_ms"], *LIMITS["max_wait_ms"]),
        }
        local_dt = datetime.fromtimestamp(now, tz=ZoneInfo(self.timezone_name))
        utc_dt = datetime.fromtimestamp(now, tz=timezone.utc)
        return {
            "schema_version": "v6.3.11.a1_dynamic_preview.1",
            "mode": "preview_only",
            "symbol": self.symbol,
            "computed_at_utc": utc_dt.isoformat(),
            "computed_at_local": local_dt.isoformat(),
            "timezone": self.timezone_name,
            "session_tag": tag,
            "is_weekend": bool(info["is_weekend"]),
            "static_params": dict(STATIC_BASE),
            "dynamic_preview_params": dynamic,
            "active_params": {
                "source": "static",
                "params": dict(STATIC_BASE),
            },
            "safety": {
                "dynamic_params_active": False,
                "mode_requested": self.mode,
                "preview_only_enforced": True,
            },
            "source_stats": dict(source_stats or {}),
        }

    def _atomic_write(self, payload: Mapping[str, Any]) -> None:
        self.json_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.json_path.with_name(self.json_path.name + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(dict(payload), f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")
        os.replace(tmp_path, self.json_path)


def _clamp(value: float, low: float, high: float) -> float:
    return float(max(low, min(high, value)))
