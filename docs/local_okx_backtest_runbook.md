# Local OKX Historical Data Backtest Runbook

This runbook describes the offline research flow for GlacierPulse.

Goal:

```text
OKX official historical data
  -> local raw files
  -> local replay through MarketContext + A1AbsorptionEngine
  -> reports/backtests/<run-name>/signals.jsonl + summary.json
```

This flow does not start `main.py`, does not connect WebSocket, and does not use `IcebergTrader`.

## 1. Download OKX official files

OKX official historical data pages expose tick-level trades and high-resolution L2 book downloads, but concrete file URLs may change. Use one of the following modes.

### Option A: URL template mode

Copy one official download URL from OKX and replace its date part with `{date}` or `{yyyymmdd}`.

```bash
python tools/download_okx_historical_data.py \
  --kind trades \
  --symbol ETH-USDT-SWAP \
  --start-date 2025-05-01 \
  --end-date 2026-05-01 \
  --url-template '<OFFICIAL_OKX_URL_WITH_{date}_OR_{yyyymmdd}>'
```

The files will be saved to:

```text
data/okx/raw/trades/ETH-USDT-SWAP/
```

For books:

```bash
python tools/download_okx_historical_data.py \
  --kind books \
  --symbol ETH-USDT-SWAP \
  --start-date 2025-05-01 \
  --end-date 2026-05-01 \
  --url-template '<OFFICIAL_OKX_BOOK_URL_WITH_{date}_OR_{yyyymmdd}>'
```

The files will be saved to:

```text
data/okx/raw/books/ETH-USDT-SWAP/
```

### Option B: manifest mode

Create a manifest with one official URL per line:

```text
data/okx/manifests/eth_trades_urls.txt
```

Then run:

```bash
python tools/download_okx_historical_data.py \
  --kind trades \
  --symbol ETH-USDT-SWAP \
  --manifest data/okx/manifests/eth_trades_urls.txt
```

A JSONL download manifest is appended under the data directory:

```text
data/okx/raw/trades/ETH-USDT-SWAP/download_manifest.jsonl
```

It records URL, local path, file size, SHA256, status, and download time.

## 2. Run local replay backtest

With trades and books:

```bash
python tools/backtest_local_data.py \
  --symbol ETH-USDT-SWAP \
  --trades-dir data/okx/raw/trades/ETH-USDT-SWAP \
  --books-dir data/okx/raw/books/ETH-USDT-SWAP \
  --run-name eth_swap_1y_v1
```

Output:

```text
reports/backtests/eth_swap_1y_v1/
  signals.jsonl
  summary.json
```

Trades-only smoke test:

```bash
python tools/backtest_local_data.py \
  --symbol ETH-USDT-SWAP \
  --trades-dir data/okx/raw/trades/ETH-USDT-SWAP \
  --run-name eth_swap_trades_only_smoke \
  --allow-missing-books \
  --max-events 100000
```

Important: A1 iceberg settlement depends on order book updates. Trades-only mode is only a parser/replay smoke test, not a full A1 evaluation.

## 3. Supported input file types

`tools/backtest_local_data.py` supports:

```text
.csv
.jsonl
.ndjson
.json
.gz
.zip
```

The parser tries common OKX-style fields:

Trades:

```text
instId / inst_id / symbol
px / price
sz / size / qty / amount
side
ts / timestamp / time
```

Books:

```text
instId / inst_id / symbol
bids / bid / bid_levels
asks / ask / ask_levels
ts / timestamp / time
```

For `ETH-USDT-SWAP`, the default contract multiplier is `0.1`, matching the project assumption that one OKX ETH swap contract is 0.1 ETH.

## 4. Recommended first validation

Start with one day of data:

```bash
python tools/backtest_local_data.py \
  --symbol ETH-USDT-SWAP \
  --trades-dir data/okx/raw/trades/ETH-USDT-SWAP/one_day \
  --books-dir data/okx/raw/books/ETH-USDT-SWAP/one_day \
  --run-name eth_swap_one_day_parser_check \
  --progress-every 50000
```

Then inspect:

```text
reports/backtests/eth_swap_one_day_parser_check/summary.json
reports/backtests/eth_swap_one_day_parser_check/signals.jsonl
```

If `malformed_rows` is high, inspect the raw OKX file format and adjust field mapping in `normalize_trade()` or `normalize_book()`.

## 5. Design rule

Keep this research path separate from live trading:

```text
main.py                      = live WebSocket runtime
backtest_local_data.py        = local replay runtime
download_okx_historical_data.py = official file acquisition
```

Do not import or instantiate `IcebergTrader` in the local replay entry.
