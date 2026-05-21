from src.research.a1_edge.hypothesis_simulator import A1HypothesisSimulator
from src.research.a1_edge.schema import A1EdgeEvent


def _klines():
    return [
        {"timestamp": i * 60, "open": 100, "high": 104 if i > 0 else 101, "low": 99, "close": 103, "volume": 1}
        for i in range(10)
    ]


def test_hypotheses_entry_skips_fee_and_realized_proxy():
    event = A1EdgeEvent.from_mapping({
        "zone_id": "z1",
        "direction": "BUY",
        "reaction_event_ts": 30,
        "last_price": 100,
        "frozen_low": 99,
        "frozen_high": 101,
        "a1_reaction_type": "A1_REACTION_CLEAN_HOLD",
    })
    simulator = A1HypothesisSimulator(roundtrip_fee_pct=0.001, min_group_sample_size=1)
    rows = simulator.simulate([event], _klines())
    types = {row.hypothesis_type for row in rows}
    assert {"IMMEDIATE_ENTRY", "NEXT_BAR_ENTRY", "DELAYED_ENTRY_1M", "DELAYED_ENTRY_3M"}.issubset(types)
    assert len(rows) >= 8
    assert any(row.is_skipped for row in rows)
    assert all(row.event_key == event.event_key for row in rows)
    assert any(row.is_skipped and row.event_key == event.event_key for row in rows)
    immediate = next(row for row in rows if row.hypothesis_type == "IMMEDIATE_ENTRY")
    assert immediate.fee_share_r > 0
    assert immediate.hit_1r is True
    assert immediate.realized_r_proxy > 0
    summary = simulator.summarize(rows)
    assert any(row["dimension"] == "ALL" for row in summary)


def test_same_zone_multiple_events_keep_distinct_event_keys_in_hypotheses():
    first = A1EdgeEvent.from_mapping({"zone_id": "same", "direction": "BUY", "reaction_event_ts": 30, "last_price": 100, "frozen_low": 99, "frozen_high": 101, "a1_reaction_type": "A", "reaction_event_kind": "CONFIRMED"})
    second = A1EdgeEvent.from_mapping({"zone_id": "same", "direction": "BUY", "reaction_event_ts": 31, "last_price": 100, "frozen_low": 99, "frozen_high": 101, "a1_reaction_type": "A", "reaction_event_kind": "FAILED"})
    rows = A1HypothesisSimulator().simulate([first, second], _klines())
    keys = {row.event_key for row in rows}
    hypothesis_ids = {row.hypothesis_id for row in rows}
    assert first.event_key in keys
    assert second.event_key in keys
    assert len(hypothesis_ids) == len(rows)
