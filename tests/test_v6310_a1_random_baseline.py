from src.research.a1_edge.forward_metrics import A1ForwardMetricsAnalyzer
from src.research.a1_edge.random_baseline import A1RandomBaselineComparator, RandomBaselineSampler
from src.research.a1_edge.schema import A1EdgeEvent


def _klines(n=30):
    return [
        {"timestamp": i * 60, "open": 100 + i, "high": 101 + i, "low": 99 + i, "close": 100 + i, "volume": 1}
        for i in range(n)
    ]


def test_random_samples_seed_exclusion_and_summary():
    event = A1EdgeEvent.from_mapping({"zone_id": "z1", "direction": "BUY", "reaction_event_ts": 300, "last_price": 105, "frozen_low": 104, "frozen_high": 106})
    sampler = RandomBaselineSampler(samples_per_event=3, random_seed=7, exclude_near_a1_minutes=2, windows_sec=[900])
    first = sampler.sample([event], _klines())
    second = RandomBaselineSampler(samples_per_event=3, random_seed=7, exclude_near_a1_minutes=2, windows_sec=[900]).sample([event], _klines())
    assert [r.random_event_ts for r in first] == [r.random_event_ts for r in second]
    assert len(first) == 3
    assert all(abs(r.random_event_ts - event.event_ts) > 120 for r in first)
    assert all(r.source_event_key == event.event_key for r in first)
    assert all(r.risk_mode == "SOURCE_A1_RISK" for r in first)
    a1_metrics = A1ForwardMetricsAnalyzer(windows_sec=[900, 3600]).analyze([event], _klines())
    summary = A1RandomBaselineComparator(min_group_sample_size=30).summarize([event], [r.to_dict() for r in a1_metrics], first)
    assert summary[0]["dimension"] == "ALL"
    assert summary[0]["edge_label"] == "INSUFFICIENT_SAMPLE"


def test_random_baseline_uses_source_a1_risk_not_distant_zone_risk():
    klines = [
        {"timestamp": 0, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1},
        {"timestamp": 60, "open": 150, "high": 153, "low": 149, "close": 152, "volume": 1},
        {"timestamp": 120, "open": 151, "high": 154, "low": 150, "close": 153, "volume": 1},
    ]
    event = A1EdgeEvent.from_mapping({
        "zone_id": "z1",
        "direction": "BUY",
        "reaction_event_ts": 0,
        "last_price": 100,
        "frozen_low": 99,
        "frozen_high": 101,
        "a1_reaction_type": "A",
        "reaction_event_kind": "CONFIRMED",
    })
    baseline = RandomBaselineSampler(samples_per_event=1, random_seed=1, exclude_near_a1_minutes=0, windows_sec=[60]).sample([event], klines)
    assert baseline
    row = baseline[0]
    assert row.entry_price == 151
    assert row.source_risk_u <= 2
    assert row.risk_mode == "SOURCE_A1_RISK"
    assert row.directional_mfe_r >= 3


def test_comparator_groups_by_event_key_not_zone_id():
    event_a = A1EdgeEvent.from_mapping({"zone_id": "same", "direction": "BUY", "reaction_event_ts": 100, "last_price": 100, "frozen_low": 99, "frozen_high": 101, "a1_reaction_type": "A", "reaction_event_kind": "CONFIRMED"})
    event_b = A1EdgeEvent.from_mapping({"zone_id": "same", "direction": "BUY", "reaction_event_ts": 101, "last_price": 100, "frozen_low": 99, "frozen_high": 101, "a1_reaction_type": "A", "reaction_event_kind": "FAILED"})
    event_c = A1EdgeEvent.from_mapping({"direction": "BUY", "reaction_event_ts": 100, "last_price": 100, "frozen_low": 99, "frozen_high": 101, "a1_reaction_type": "A", "reaction_event_kind": "CONFIRMED"})
    event_d = A1EdgeEvent.from_mapping({"direction": "BUY", "reaction_event_ts": 101, "last_price": 100, "frozen_low": 99, "frozen_high": 101, "a1_reaction_type": "A", "reaction_event_kind": "CONFIRMED"})
    a1_metrics = [
        {"event_key": event_a.event_key, "zone_id": "same", "window_sec": 900, "directional_mfe_r": 1, "hit_plus_1r": True, "total_range_u": 2},
        {"event_key": event_b.event_key, "zone_id": "same", "window_sec": 900, "directional_mfe_r": 10, "hit_plus_1r": True, "total_range_u": 20},
        {"event_key": event_c.event_key, "zone_id": "", "window_sec": 900, "directional_mfe_r": 2, "hit_plus_1r": True, "total_range_u": 4},
        {"event_key": event_d.event_key, "zone_id": "", "window_sec": 900, "directional_mfe_r": 3, "hit_plus_1r": True, "total_range_u": 6},
    ]
    random_rows = [
        {"source_event_key": event_a.event_key, "window_sec": 900, "directional_mfe_r": 1, "hit_plus_1r": False, "total_range_u": 1},
        {"source_event_key": event_b.event_key, "window_sec": 900, "directional_mfe_r": 10, "hit_plus_1r": False, "total_range_u": 10},
        {"source_event_key": event_c.event_key, "window_sec": 900, "directional_mfe_r": 2, "hit_plus_1r": False, "total_range_u": 2},
        {"source_event_key": event_d.event_key, "window_sec": 900, "directional_mfe_r": 3, "hit_plus_1r": False, "total_range_u": 3},
    ]
    summary = A1RandomBaselineComparator(min_group_sample_size=1).summarize(
        [event_a, event_b, event_c, event_d],
        a1_metrics,
        random_rows,
    )
    failed = next(row for row in summary if row["dimension"] == "reaction_event_kind" and row["group"] == "FAILED")
    confirmed = next(row for row in summary if row["dimension"] == "reaction_event_kind" and row["group"] == "CONFIRMED")
    assert failed["a1_sample_count"] == 1
    assert failed["a1_avg_mfe_r_15m"] == 10
    assert confirmed["a1_sample_count"] == 3
