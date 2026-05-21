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
    assert "event_key" in data


def test_event_key_generation_with_zone_id_and_fallback():
    with_zone = A1EdgeEvent.from_mapping({
        "zone_id": "z1",
        "direction": "BUY",
        "reaction_event_ts": 1000,
        "frozen_low": 99,
        "frozen_high": 101,
        "a1_reaction_type": "A1_REACTION_CLEAN_HOLD",
        "reaction_event_kind": "CONFIRMED",
    })
    assert with_zone.event_key == "z1|A1_REACTION_CLEAN_HOLD|CONFIRMED|1000.0"

    fallback = A1EdgeEvent.from_mapping({
        "direction": "SELL",
        "reaction_event_ts": 1001,
        "frozen_low": 98,
        "frozen_high": 102,
        "a1_reaction_type": "A1_REACTION_FAILED_RECLAIM",
        "reaction_event_kind": "FAILED",
    })
    assert fallback.event_key == "SELL|1001.0|98.0|102.0|A1_REACTION_FAILED_RECLAIM|FAILED"


def test_event_key_from_record_takes_priority():
    event = A1EdgeEvent.from_mapping({
        "event_key": "custom-key",
        "zone_id": "z1",
        "reaction_event_ts": 1000,
        "a1_reaction_type": "A",
        "reaction_event_kind": "B",
    })
    assert event.event_key == "custom-key"
