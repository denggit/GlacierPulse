#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Mapping

from .models import SCHEMA_VERSION

logger = logging.getLogger(__name__)


class Phase1CandidateRecorder:
    def __init__(
        self,
        enabled: bool = True,
        write_jsonl: bool = True,
        jsonl_path: str = "logs/research/phase1_candidates.jsonl",
    ) -> None:
        self.enabled = bool(enabled)
        self.write_jsonl = bool(write_jsonl)
        self.jsonl_path = Path(jsonl_path)
        self.total_settled = 0
        self.total_finalized = 0

    def record_settled(self, candidate: Mapping[str, Any]) -> bool:
        record = dict(candidate)
        record["schema_version"] = record.get("schema_version") or SCHEMA_VERSION
        record["record_type"] = "candidate_settled"
        ok = self._write(record)
        if ok:
            self.total_settled += 1
        return ok

    def record_finalized(self, candidate: Mapping[str, Any]) -> bool:
        record = dict(candidate)
        record["schema_version"] = record.get("schema_version") or SCHEMA_VERSION
        record["record_type"] = "candidate_finalized"
        ok = self._write(record)
        if ok:
            self.total_finalized += 1
        return ok

    def _write(self, record: Mapping[str, Any]) -> bool:
        if not self.enabled:
            return False
        if not self.write_jsonl:
            return True
        try:
            self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            with self.jsonl_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(dict(record), ensure_ascii=False, sort_keys=True) + "\n")
            return True
        except Exception as exc:
            logger.warning("[PHASE1-TRUTH-FALLBACK] reason=recorder_write_failed error=%s", exc)
            return False

    def summary(self) -> dict[str, Any]:
        return {
            "active": self.enabled,
            "write_jsonl": self.write_jsonl,
            "jsonl_path": str(self.jsonl_path),
            "total_settled": self.total_settled,
            "total_finalized": self.total_finalized,
        }
