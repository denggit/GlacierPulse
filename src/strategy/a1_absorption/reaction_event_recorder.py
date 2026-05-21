#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from collections import Counter, deque
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional


class A1ReactionEventRecorder:
    def __init__(self, enabled: bool = True, write_jsonl: bool = False, jsonl_path: str = "logs/research/a1_reaction_events.jsonl", max_recent_events: int = 5000):
        self.enabled = bool(enabled)
        self.write_jsonl = bool(write_jsonl)
        self.jsonl_path = str(jsonl_path)
        self.max_recent_events = max(1, int(max_recent_events))
        self.recent_events: Deque[Dict[str, Any]] = deque(maxlen=self.max_recent_events)
        self.by_a1_reaction_type = Counter()
        self.by_event_kind = Counter()
        self.total_events = 0

    def record(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.enabled or not isinstance(event, dict):
            return None
        e = dict(event)
        self.recent_events.append(e)
        self.total_events += 1
        self.by_a1_reaction_type[str(e.get("a1_reaction_type") or "A1_REACTION_UNKNOWN")] += 1
        self.by_event_kind[str(e.get("reaction_event_kind") or "UNKNOWN")] += 1
        if self.write_jsonl:
            path = Path(self.jsonl_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        return e

    def record_many(self, events: Iterable[Dict[str, Any]]) -> int:
        count = 0
        for event in events:
            if self.record(event) is not None:
                count += 1
        return count

    def summary(self) -> Dict[str, Any]:
        return {
            "total_events": self.total_events,
            "total_confirmed": self.by_event_kind.get("CONFIRMED", 0),
            "total_failed": self.by_event_kind.get("FAILED", 0),
            "total_timeout": self.by_event_kind.get("TIMEOUT", 0),
            "total_no_response": self.by_event_kind.get("NO_RESPONSE", 0),
            "total_missed_fast_move": self.by_event_kind.get("MISSED_FAST_MOVE", 0),
            "total_sweep_no_reclaim": self.by_event_kind.get("SWEEP_NO_RECLAIM", 0),
            "total_reclaim_no_retest": self.by_event_kind.get("RECLAIM_NO_RETEST", 0),
            "by_a1_reaction_type": dict(self.by_a1_reaction_type),
            "by_event_kind": dict(self.by_event_kind),
        }

    def pop_recent_events(self) -> List[Dict[str, Any]]:
        events = list(self.recent_events)
        self.recent_events.clear()
        return events
