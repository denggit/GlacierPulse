from src.research.a1_edge.schema import A1EdgeEvent


def test_event_from_mapping_defaults_and_normalization():
    event = A1EdgeEvent.from_mapping({"direction": "long", "has_confirmed": "yes", "timestamp": "2026-05-21T00:00:00Z"})
    assert event.direction == "BUY"
    assert event.has_confirmed is True
    assert event.a1_reaction_type == "A1_REACTION_UNKNOWN"
    assert event.event_ts > 0


def test_timestamp_seconds_millis_and_iso_priority():
    sec = A1EdgeEvent.from_mapping({"reaction_event_ts": 1000})
    ms = A1EdgeEvent.from_mapping({"reaction_event_ts": 1_000_000_000_000})
    iso = A1EdgeEvent.from_mapping({"timestamp": "1970-01-01T00:16:40+00:00"})
    assert sec.event_ts == 1000
    assert ms.event_ts == 1_000_000_000
    assert iso.event_ts == 1000


def test_bool_strings_and_to_dict_fields():
    event = A1EdgeEvent.from_mapping({"has_failed": "true", "relevant_book_depth_available": "0", "direction": "bad"})
    data = event.to_dict()
    assert data["has_failed"] is True
    assert data["relevant_book_depth_available"] is False
    assert data["direction"] == "UNKNOWN"
    assert "book_absorption_score" in data
