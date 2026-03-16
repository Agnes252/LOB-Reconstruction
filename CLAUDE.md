# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run all tests
python -m pytest tests/ -v

# Run a single test file
python -m pytest tests/test_order_book.py -v

# Run a single test by name
python -m pytest tests/test_auction_engine.py::test_szse_auction_match -v

# Run with coverage
python -m pytest tests/ --cov=lob --cov=config

# Single stock pipeline
python scripts/run_single.py --exchange SZSE --security 000001 \
    --orders data/orders.csv --trades data/trades.csv --output out/000001.parquet

# Batch pipeline
python scripts/run_batch.py --exchange SSE --data-dir data/ --output-dir out/

# End-to-end test with sample data (data_test/)
python scripts/test_data_test.py
```

Install dev deps: `pip install -e ".[dev]"`

## Architecture

The pipeline has five layers that execute in sequence per tick event:

```
CSV files
  └─ io/reader.py          # chunked CSV → raw DataFrames (pandas)
       └─ parsers/          # raw rows → Order / Trade objects
            └─ pipeline.py  # merge two streams (order + trade) into one sorted iterator
                 └─ engine/ # update OrderBook, emit OrderEvent/TradeEvent/CancelEvent
                      └─ resampler/ # bucket events into 50ms snapshots
                           └─ factors/ # compute snapshot-level metrics
                                └─ io/writer.py  # Parquet output
```

Optional cross-cutting modules:
- `engine/channel_buffer.py` — per-channel sequence-ordered delivery (乱序保序)
- `pipeline/channel_coordinator.py` — multi-security mutual-trigger snapshot alignment
- `validator/validator.py` — compare reconstructed snapshots against official L2 3s data

### Key Design Invariants

**Prices are always integers × 10000.** Never use float prices internally. `best_bid()` / `best_ask()` return int or None.

**`OrderBook` internals:**
- `bids`: `SortedDict` keyed by **negative** price → `keys()[0]` = best bid
- `asks`: `SortedDict` keyed by **positive** price → `keys()[0]` = best ask
- `order_index`: `seq_num → (Side, price_int)` — O(1) cancel/reduce lookup
- `order_no_index`: `OrderNo → seq_num` — SSE-only; needed because SSE trade records reference `OrderNo`, not `ApplSeqNum`
- `pending_orders`: `seq_num → (side, price, qty)` — SZSE price-cage buffer; released when reference price moves into range
- `ghost_orders`: SSE only; fully-filled aggressive orders that never appear in the order stream, synthesized from trades
- `anomaly_count`: incremented when a cancel/trade cannot find its order in `order_index`

**Event merge differs by exchange** (`pipeline.py`):
- SZSE: heap key `(timestamp_ns, seq_num, priority)` — ApplSeqNum is unified across order/trade streams within the same ChannelNo
- SSE: heap key `(biz_index, priority, timestamp_ns)` — BizIndex is the only correct ordering key; timestamp alone is not sufficient

### Exchange Engine Differences

| Concern | SZSE (`szse_engine.py`) | SSE (`sse_engine.py`) |
|---|---|---|
| Cancel location | Trade stream (`ExecType='4'`) | Order stream (`cancel_flag='D'`) |
| Cancel key | `bid_seq` or `ask_seq` = `seq_num` directly | `order.order_no` → `order_no_index` → `seq_num` |
| Trade→order key | `BidApplSeqNum` = `seq_num` directly | `BuyNo`/`SellNo` = `OrderNo` → must translate via `order_no_index` |
| OWN_BEST orders | `OrdType='U'`: passive limit at same-side best price; void if same side is empty | Not supported |
| Market orders | IOC sweep, remainder discarded | IOC sweep, remainder discarded |
| Ghost orders | None | Fully-filled aggressive orders never appear in order stream; synthesized from trade records |
| Qty semantics | Original qty | Continuous session: **remaining qty** after partial fills |
| Price cage | `enable_price_cage=True` → orders outside ±10% of last trade price enter `pending_orders` | Not applicable |

### Resampler Boundary Logic

`LOBResampler` fires a snapshot when `event.timestamp_ns >= next_boundary_ns`. The boundary is the **end** of the current 50ms window. On boundary cross: build snapshot → compute factors → reset `IntervalAccumulator` → advance boundary. Silent intervals (no events) carry forward the previous snapshot's book state with zeroed flow factors.

**`fill_to_end(book, phase)`** must be called at day-end instead of `flush()`. It generates carry-forward snapshots for every remaining 50ms slot from the last event to `end_ms` (default 15:00:00), ensuring complete time-axis coverage for cross-security JOIN. After calling `fill_to_end()`, `acc` is set to `None`.

### Factor Layers

- `static_factors.py`: computed from book snapshot alone — `mid_price`, `spread`, `sheet_diff`
- `dynamic_factors.py`: per-level order/match/cancel volumes from `IntervalAccumulator` — mapped to current book levels at snapshot time
- `derived_indicators.py`: aggregated diffs — `match_diff`, `order_diff`, `cancel_diff`
- `ofi.py`: `compute_ofi()` / `compute_ofi_normalized()` — best-quote net flow imbalance over the interval

**`LOBSnapshot` carries** (beyond OHLCV and ten-level bid/ask arrays): `ofi`, `ofi_norm`, `is_anomaly`, `anomaly_count`, `last_price`, `cum_volume`, `cum_turnover`.

### Phase Classification

`PhaseClassifier` maps `timestamp_ns` (nanoseconds since midnight) to `TradingPhase`. Phase transitions trigger `PhaseTransitionHandler.handle_auction_close()` which runs the final auction matching algorithm and synthesizes trades before continuous trading begins.

### Multi-Security Channel Coordination

`ChannelCoordinator` (`pipeline/channel_coordinator.py`) implements mutual-trigger snapshot alignment: when any security on a channel crosses a 50ms boundary, all other securities on the same channel are also flushed to that boundary. Use `coordinator.ingest(event)` instead of individual pipeline calls; call `flush_all()` at day-end.

### Correctness Validation

`LOBValidator` (`validator/validator.py`) compares reconstructed snapshots against official exchange L2 3s snapshots:
- `compare_day(reconstructed_df, reference_df)` → `DayCompareReport` with per-level price hit-rate and volume coverage
- `compare_snapshot(reconstructed_df, ref_row)` → `SnapshotCompareResult` for a single timestamp
- Default tolerance: ±50ms time matching, exact price matching

### Adding a New Exchange

1. Add column maps to `config/exchange_config.py`
2. Implement `Parser` subclass in `lob/parsers/`
3. Implement `Engine` subclass handling `process_order`, `process_trade`, `process_cancel`
4. Add reader functions in `lob/io/reader.py`
5. Add exchange-specific `_merge_events_*` function in `lob/pipeline/pipeline.py`
6. Wire up in `SingleSecurityPipeline.__init__`

### SZSE Actual Data Column Names

The real SZSE CSV files use **different column names** than the legacy STEP spec. `config/exchange_config.py` maps the actual names:

| Logical field | orders.csv column | trans.csv column | Notes |
|---|---|---|---|
| timestamp | `MDTime` | `MDTime` | HHMMSSMMM int (e.g. `091503750`) |
| price | `OrderPrice` | `TradePrice` | **float** in yuan → parser does `round(x * 10000)` |
| qty | `OrderQty` | `TradeQty` | |
| side | `OrderBSFlag` | — | `1`=买 `2`=卖 |
| order_type | `OrderType` | — | `1`=市价 `2`=限价 `3`=本方最优 |
| buy_order_seq | — | `TradeBuyNo` | ApplSeqNum of buyer (0 if N/A) |
| sell_order_seq | — | `TradeSellNo` | ApplSeqNum of seller (0 if N/A) |
| cancel/trade | — | `TradeType` | **`1`=撤销 `2`=成交** |
| aggressor side | — | `TradeBSFlag` | `1`=主买→`'B'` `2`=主卖→`'S'` |
| security | `SecurityID` | `SecurityID` | Format `000400.SZ`; parser strips `.SZ` |
| seq | `ApplSeqNum` | `ApplSeqNum` | Primary ordering key |
