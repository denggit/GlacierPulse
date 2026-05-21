import csv
import json

from src.research.a1_edge.dataset_exporter import A1EdgeDatasetExporter
from src.research.a1_edge.schema import A1_EDGE_EVENT_FIELDS


def test_jsonl_csv_empty_dedup_summary_and_column_order(tmp_path):
    exporter = A1EdgeDatasetExporter()
    jsonl = tmp_path / "events.jsonl"
    row = {
        "zone_id": "z1",
        "direction": "BUY",
        "reaction_event_ts": 1000,
        "a1_reaction_type": "A1_REACTION_CLEAN_HOLD",
        "reaction_event_kind": "CONFIRMED",
        "has_confirmed": True,
    }
    jsonl.write_text(json.dumps(row) + "\n" + json.dumps(row) + "\n", encoding="utf-8")
    events = exporter.load_events(jsonl, symbol="ETH-USDT-SWAP")
    assert len(events) == 1
    out = tmp_path / "out"
    summary = exporter.export(events, out)
    assert summary["total_events"] == 1
    assert summary["confirmed_count"] == 1
    assert (out / "a1_edge_dataset_summary.json").exists()
    assert not (out / "a1_edge_summary.json").exists()
    with (out / "a1_edge_events.csv").open(newline="", encoding="utf-8") as f:
        assert next(csv.reader(f)) == A1_EDGE_EVENT_FIELDS
    csv_path = tmp_path / "events.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    assert len(exporter.load_events(csv_path)) == 1
    empty_summary = exporter.export([], tmp_path / "empty")
    assert empty_summary["total_events"] == 0
    assert (tmp_path / "empty" / "a1_edge_events.jsonl").exists()
    assert (tmp_path / "empty" / "a1_edge_dataset_summary.json").exists()


def test_deduplicate_uses_event_key_not_zone_id_only():
    exporter = A1EdgeDatasetExporter()
    rows = [
        {"zone_id": "z1", "reaction_event_ts": 1000, "a1_reaction_type": "A", "reaction_event_kind": "CONFIRMED"},
        {"zone_id": "z1", "reaction_event_ts": 1000, "a1_reaction_type": "A", "reaction_event_kind": "FAILED"},
        {"zone_id": "z1", "reaction_event_ts": 1000, "a1_reaction_type": "A", "reaction_event_kind": "CONFIRMED"},
        {"direction": "BUY", "reaction_event_ts": 1000, "frozen_low": 99, "frozen_high": 101, "a1_reaction_type": "A", "reaction_event_kind": "CONFIRMED"},
        {"direction": "BUY", "reaction_event_ts": 1001, "frozen_low": 99, "frozen_high": 101, "a1_reaction_type": "A", "reaction_event_kind": "CONFIRMED"},
    ]
    events = exporter.normalize_records(rows)
    assert len(events) == 4
    keys = {event.event_key for event in events}
    assert len(keys) == 4
