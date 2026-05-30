import pytest

from tools import backtest_local_data as backtest


def _cleaner(bucket_ms: float = 100.0, depth_limit: int = 400):
    stats = backtest.Stats()
    options = backtest.BookCleaningOptions(bucket_ms=bucket_ms, depth_limit=depth_limit)
    return backtest.BookEventCleaner(options, stats), stats


def _delta(ts: float, bids=None, asks=None):
    return {"ts": ts, "recv_ts": ts, "bids": bids or [], "asks": asks or [], "_mode": "delta"}


def _snapshot(ts: float, bids=None, asks=None):
    return {"ts": ts, "recv_ts": ts, "bids": bids or [], "asks": asks or [], "_mode": "snapshot"}


def test_10ms_rows_coalesce_into_one_100ms_bucket():
    cleaner, stats = _cleaner(bucket_ms=100.0)
    emitted = []
    for idx in range(10):
        emitted.extend(cleaner.push(_delta(idx * 0.01, bids=[[100.0 + idx, 1.0]])))
    emitted.extend(cleaner.flush())

    assert len(emitted) == 1
    assert len(emitted[0]["bids"]) == 10
    assert stats.book_bucket_coalesces > 0


def test_cross_bucket_rows_emit_multiple_buckets():
    cleaner, _ = _cleaner(bucket_ms=100.0)
    emitted = []
    for ts in [0.0, 0.01, 0.09, 0.1, 0.11]:
        emitted.extend(cleaner.push(_delta(ts, bids=[[100.0 + ts, 1.0]])))
    emitted.extend(cleaner.flush())

    assert len(emitted) >= 2
    assert emitted[0]["ts"] == pytest.approx(0.09)
    assert emitted[1]["ts"] == pytest.approx(0.11)


def test_size_zero_deletes_previous_sent_level():
    cleaner, stats = _cleaner(bucket_ms=100.0)
    assert cleaner.push(_snapshot(0.0, bids=[[100.0, 1.0]])) == []
    first = cleaner.push(_delta(0.1, bids=[[100.0, 0.0]]))
    assert len(first) == 1
    assert first[0]["bids"] == [[100.0, 1.0]]

    deleted = cleaner.flush()
    assert len(deleted) == 1
    assert deleted[0]["bids"] == [[100.0, 0.0]]
    assert 100.0 not in cleaner.prev_sent_bids_state
    assert stats.book_zero_delete_levels == 1


def test_snapshot_and_delta_in_same_bucket_finalize_to_combined_state():
    cleaner, _ = _cleaner(bucket_ms=100.0)
    emitted = []
    emitted.extend(cleaner.push(_snapshot(0.0, bids=[[100.0, 1.0]], asks=[[101.0, 1.0]])))
    emitted.extend(cleaner.push(_delta(0.01, bids=[[99.0, 2.0]], asks=[[102.0, 3.0]])))
    emitted.extend(cleaner.flush())

    assert len(emitted) == 1
    assert emitted[0]["bids"] == [[100.0, 1.0], [99.0, 2.0]]
    assert emitted[0]["asks"] == [[101.0, 1.0], [102.0, 3.0]]


def test_snapshot_depth_limit_keeps_only_top_400_levels():
    cleaner, _ = _cleaner(bucket_ms=100.0, depth_limit=400)
    bids = [[1000.0 - idx, 1.0] for idx in range(450)]
    asks = [[1000.0 + idx, 1.0] for idx in range(450)]

    cleaner.push(_snapshot(0.0, bids=bids, asks=asks))
    emitted = cleaner.flush()

    assert len(emitted) == 1
    assert len(emitted[0]["bids"]) == 400
    assert len(emitted[0]["asks"]) == 400
    assert len(cleaner.prev_sent_bids_state) == 400
    assert len(cleaner.prev_sent_asks_state) == 400
    assert min(cleaner.prev_sent_bids_state) == pytest.approx(601.0)
    assert max(cleaner.prev_sent_asks_state) == pytest.approx(1399.0)


def _old_trim_path(state, side, depth_limit):
    return backtest.levels_to_state(
        backtest.sort_levels_from_state(state, side=side, depth_limit=depth_limit),
        side=side,
        depth_limit=depth_limit,
    )


@pytest.mark.parametrize("side", ["bids", "asks"])
@pytest.mark.parametrize("depth_limit", [400, 10, 2000, 0, -1])
def test_trim_state_to_depth_matches_old_sort_and_state_path(side, depth_limit):
    state = {}
    for idx in range(1000):
        price = float(1000 + ((idx * 37) % 1000))
        if idx % 17 == 0:
            size = 0.0
        elif idx % 29 == 0:
            size = -float(idx + 1)
        else:
            size = float(idx + 1) / 10.0
        state[price] = size

    assert backtest.trim_state_to_depth(state, side=side, depth_limit=depth_limit) == _old_trim_path(state, side, depth_limit)


def test_finalize_bucket_matches_old_path_for_snapshot_delta_delete_update_insert():
    cleaner, _stats = _cleaner(bucket_ms=100.0, depth_limit=3)
    cleaner.push(
        _snapshot(
            0.0,
            bids=[[100.0, 1.0], [99.0, 2.0], [98.0, 3.0]],
            asks=[[101.0, 1.0], [102.0, 2.0], [103.0, 3.0]],
        )
    )
    cleaner.push(
        _delta(
            0.01,
            bids=[[100.0, 0.0], [99.0, 5.0], [101.0, 4.0]],
            asks=[[101.0, 0.0], [102.0, 6.0], [100.5, 7.0]],
        )
    )

    expected_bids = _old_trim_path(cleaner.bucket_bids_state, "bids", cleaner.options.depth_limit)
    expected_asks = _old_trim_path(cleaner.bucket_asks_state, "asks", cleaner.options.depth_limit)
    expected_bid_delta, _ = backtest.diff_states(cleaner.prev_sent_bids_state, expected_bids, side="bids")
    expected_ask_delta, _ = backtest.diff_states(cleaner.prev_sent_asks_state, expected_asks, side="asks")

    emitted = cleaner.flush()

    assert len(emitted) == 1
    assert emitted[0]["bids"] == expected_bid_delta
    assert emitted[0]["asks"] == expected_ask_delta
    assert cleaner.prev_sent_bids_state == expected_bids
    assert cleaner.prev_sent_asks_state == expected_asks


def test_parse_levels_and_normalize_book_accept_supported_level_shapes():
    options = backtest.BookCleaningOptions(event_mode="snapshot", depth_limit=10)
    row = {
        "ts": "1700000000000",
        "bids": [
            {"px": "100.0", "sz": "2"},
            ("99.5", "3"),
            ["99.0", "0", "ignored"],
            {"price": "bad", "size": "4"},
            ["98.5", ""],
        ],
        "asks": '[["101.0", "1", "0", "1"], {"price": "102.0", "size": "2"}]',
        "action": "snapshot",
    }

    normalized = backtest.normalize_book(row, "ETH-USDT-SWAP", 0.1, options)

    assert normalized is not None
    assert normalized["bids"] == [[100.0, 0.2], [99.5, 0.30000000000000004], [99.0, 0.0], [98.5, 0.0]]
    assert normalized["asks"] == [[101.0, 0.1], [102.0, 0.2]]


def test_book_cleaner_push_timing_does_not_cross_generator_yield(monkeypatch, tmp_path):
    clock = {"value": 100.0}

    def fake_perf_counter():
        return clock["value"]

    class FakeCleaner:
        def __init__(self, options, stats, profiler=None):
            self.options = options
            self.stats = stats
            self.profiler = profiler

        def push(self, raw_book):
            clock["value"] += 0.001
            return [{"bids": [[100.0, 1.0]], "asks": [], "ts": raw_book["ts"], "recv_ts": raw_book["recv_ts"]}]

        def flush(self):
            clock["value"] += 0.002
            return []

    def fake_rows(*args, **kwargs):
        yield {"bids": [], "asks": [], "ts": 1.0, "recv_ts": 1.0, "_mode": "delta"}

    monkeypatch.setattr(backtest.time, "perf_counter", fake_perf_counter)
    monkeypatch.setattr(backtest, "BookEventCleaner", FakeCleaner)
    monkeypatch.setattr(backtest, "iter_normalized_book_rows_from_file", fake_rows)

    stats = backtest.Stats()
    profiler = backtest.ReplayProfiler()
    iterator = backtest.iter_book_events(
        [tmp_path / "books.jsonl"],
        "ETH-USDT-SWAP",
        0.1,
        stats,
        backtest.BookCleaningOptions(),
        backtest.TimeFilter(),
        profiler=profiler,
    )

    event = next(iterator)
    clock["value"] += 10.0
    remaining = list(iterator)

    assert event.kind == "book"
    assert remaining == []
    assert stats.raw_book_rows == 1
    assert stats.books == 1
    assert profiler.book_cleaner_push_sec == pytest.approx(0.003)
