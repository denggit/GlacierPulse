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
