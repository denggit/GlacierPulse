#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

from .io_utils import read_csv, read_jsonl, write_csv, write_json, write_jsonl
from .schema import A1EdgeEvent, A1_EDGE_EVENT_FIELDS


class A1EdgeDatasetExporter:
    def load_events(self, path: Path | str, symbol: str = "") -> List[A1EdgeEvent]:
        p = Path(path)
        suffix = p.suffix.lower()
        if suffix == ".jsonl":
            records = read_jsonl(p)
        elif suffix == ".csv":
            records = read_csv(p)
        else:
            raise ValueError(f"Unsupported events file extension: {p.suffix}. Use .jsonl or .csv")
        return self.normalize_records(records, symbol=symbol)

    def normalize_records(self, records: Iterable[Mapping[str, Any]], symbol: str = "") -> List[A1EdgeEvent]:
        events: List[A1EdgeEvent] = []
        for record in records or []:
            data = dict(record or {})
            if symbol and not data.get("symbol"):
                data["symbol"] = symbol
            events.append(A1EdgeEvent.from_mapping(data))
        return self.deduplicate(events)

    def deduplicate(self, events: Iterable[A1EdgeEvent]) -> List[A1EdgeEvent]:
        seen = set()
        unique: List[A1EdgeEvent] = []
        for event in events or []:
            if event.zone_id:
                key = (event.zone_id, event.a1_reaction_type, event.reaction_event_kind, event.event_ts)
            else:
                key = (
                    event.direction,
                    event.event_ts,
                    event.frozen_low,
                    event.frozen_high,
                    event.a1_reaction_type,
                )
            if key in seen:
                continue
            seen.add(key)
            unique.append(event)
        return unique

    def build_summary(self, events: Iterable[A1EdgeEvent]) -> Dict[str, Any]:
        rows = list(events or [])
        event_ts_values = [e.event_ts for e in rows if e.event_ts > 0]
        return {
            "total_events": len(rows),
            "by_direction": dict(Counter(e.direction for e in rows)),
            "by_a1_reaction_type": dict(Counter(e.a1_reaction_type for e in rows)),
            "by_reaction_event_kind": dict(Counter(e.reaction_event_kind for e in rows)),
            "by_legacy_phase2_type": dict(Counter(e.legacy_phase2_type for e in rows)),
            "by_frozen_reason": dict(Counter(e.frozen_reason or "UNKNOWN" for e in rows)),
            "by_frozen_state": dict(Counter(e.frozen_state or "UNKNOWN" for e in rows)),
            "confirmed_count": sum(1 for e in rows if e.has_confirmed),
            "failed_count": sum(1 for e in rows if e.has_failed),
            "book_depth_available_count": sum(1 for e in rows if e.relevant_book_depth_available),
            "unknown_reaction_type_count": sum(1 for e in rows if not e.a1_reaction_type or "UNKNOWN" in e.a1_reaction_type),
            "unknown_frozen_reason_count": sum(1 for e in rows if not e.frozen_reason or e.frozen_reason == "UNKNOWN"),
            "min_event_ts": min(event_ts_values) if event_ts_values else 0.0,
            "max_event_ts": max(event_ts_values) if event_ts_values else 0.0,
        }

    def export(self, events: Iterable[A1EdgeEvent], out_dir: Path | str) -> Dict[str, Any]:
        out = Path(out_dir)
        rows = [event.to_dict() for event in self.deduplicate(events)]
        summary = self.build_summary([A1EdgeEvent.from_mapping(row) for row in rows])
        write_csv(out / "a1_edge_events.csv", rows, A1_EDGE_EVENT_FIELDS)
        write_jsonl(out / "a1_edge_events.jsonl", rows)
        write_json(out / "a1_edge_summary.json", summary)
        return summary
