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
    a1_metrics = A1ForwardMetricsAnalyzer(windows_sec=[900, 3600]).analyze([event], _klines())
    summary = A1RandomBaselineComparator(min_group_sample_size=30).summarize([event], [r.to_dict() for r in a1_metrics], first)
    assert summary[0]["dimension"] == "ALL"
    assert summary[0]["edge_label"] == "INSUFFICIENT_SAMPLE"
