import time

from tools import backtest_local_data as backtest


SYMBOL = "ETH-USDT-SWAP"


def _write_trade_csv(path, rows):
    path.write_text("instId,px,sz,side,ts\n" + "\n".join(rows) + "\n", encoding="utf-8")


def test_coverage_cache_miss_hit_and_mtime_invalidation(tmp_path):
    trades_dir = tmp_path / "trades"
    trades_dir.mkdir()
    path = trades_dir / "trades.csv"
    _write_trade_csv(
        path,
        [
            "ETH-USDT-SWAP,2500,10,buy,1776864000082",
            "ETH-USDT-SWAP,2501,5,sell,1776864060000",
        ],
    )
    cache_path = tmp_path / "coverage.json"
    requested = backtest.TimeFilter(1776863000.0, 1776865000.0)
    cache = backtest.load_coverage_cache(cache_path)
    stats = backtest.CoverageCacheStats()

    first = backtest.select_files_for_replay(
        trades_dir,
        "trade",
        SYMBOL,
        0.1,
        backtest.BookCleaningOptions(),
        requested,
        auto_discover=True,
        coverage_tolerance_sec=60.0,
        cache=cache,
        cache_enabled=True,
        cache_stats=stats,
    )
    assert first.first_ts == 1776864000.082
    assert stats.misses == 1
    assert stats.hits == 0
    backtest.save_coverage_cache(cache_path, cache)

    stats2 = backtest.CoverageCacheStats()
    second = backtest.select_files_for_replay(
        trades_dir,
        "trade",
        SYMBOL,
        0.1,
        backtest.BookCleaningOptions(),
        requested,
        auto_discover=True,
        coverage_tolerance_sec=60.0,
        cache=backtest.load_coverage_cache(cache_path),
        cache_enabled=True,
        cache_stats=stats2,
    )
    assert second.first_ts == first.first_ts
    assert stats2.hits == 1
    assert stats2.misses == 0

    time.sleep(0.001)
    with path.open("a", encoding="utf-8") as f:
        f.write("ETH-USDT-SWAP,2502,1,buy,1776864120000\n")

    cache3 = backtest.load_coverage_cache(cache_path)
    stats3 = backtest.CoverageCacheStats()
    third = backtest.select_files_for_replay(
        trades_dir,
        "trade",
        SYMBOL,
        0.1,
        backtest.BookCleaningOptions(),
        requested,
        auto_discover=True,
        coverage_tolerance_sec=60.0,
        cache=cache3,
        cache_enabled=True,
        cache_stats=stats3,
    )
    assert third.last_ts == 1776864120.0
    assert stats3.misses == 1


def test_corrupt_cache_is_ignored_and_rewritten(tmp_path):
    cache_path = tmp_path / "coverage.json"
    cache_path.write_text("{not valid json", encoding="utf-8")

    cache = backtest.load_coverage_cache(cache_path)

    assert cache == {}
    cache["x"] = {"path": "dummy"}
    backtest.save_coverage_cache(cache_path, cache)
    assert '"dummy"' in cache_path.read_text(encoding="utf-8")
