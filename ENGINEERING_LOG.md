# Engineering Log — Quant Singularity Data Engine

> Compiled from all development conversations: initial data exploration, EDA/validation design, warehouse architecture (Phase 3), API layer (Phase 4/5), benchmarking (Phase 6), and post-submission optimizations.

---

## Phase 1 — Project Setup & Initial Exploration
**Session:** `782cafa0` (2026-05-10)

- Analysed the full `intern_data_db` bundle covering **7 trading days (2025-08-22 → 2025-09-02)**.
- Identified the five data sources: `nifty_spot`, `nifty_futures`, `options_chain`, `india_vix`, `fii_dii_flow`.
- Key observation: Options files are named `<trade_date>_<expiry_date>.csv` — the expiry date must be parsed from the filename and injected as a column; it is not present in the raw data.
- Established `requirements.txt` with: `pandas`, `duckdb`, `pyarrow`, `mlflow`, `numpy`.

---

## Phase 2 — EDA & Anomaly Hunt
**Session:** `45a66918` (2026-05-15)

### 2.1 Return Matrix Bug (Signed vs. Absolute)
- Initial `create_return_matrix` used `.abs()` on pct_change, causing all detected spikes to appear as upward moves even when they were flash crashes.
- **Fix:** Removed `.abs()` so direction is preserved. This is critical for correct triangulation direction-of-error attribution.

### 2.2 Futures Column Structure Discovery
- Futures CSV columns use `near_month_*` / `mid_month_*` prefixes (not a generic `close`), which broke early validation joins.
- **Fix:** All cross-instrument merges explicitly reference `near_month_close` for the near contract.

### 2.3 Batch-Wise 5-Minute Resampling
- Decided to process each daily CSV individually (batch-wise) rather than concatenating all days into a single DataFrame, avoiding potential OOM issues at production scale (2-year horizon).

### 2.4 Volatility-Aware Triangular Validation — Design
- **Insight:** A spot spike could be: (a) a real market event, (b) a bad vendor tick, or (c) a latency artefact in one instrument's feed.
- **Method:** Delta-based triangulation — compute minute-to-minute changes (Δ) for Spot, Futures, and a synthetic future (C − P from options) at each timestamp. If two agree but one diverges beyond a threshold, the outlier is flagged.
- **VIX integration:** Static thresholds were replaced with dynamic ones scaled by `VIX / 15.0` (where 15.0 is the assumed "calm" baseline). This prevents flagging natural high-volatility noise during market stress periods.
- **OTM strike handling:** The synthetic future is computed as the **median** Δ(C − P) across all strikes for each timestamp. This ensures deep-OTM stale prints don't dominate the signal, while benefiting from ATM liquidity.
- **Theta neutrality of synthetic future:** The `C − P` spread is immune to standard theta decay because long-call and short-put theta cancel exactly. Confirmed as safe to use intraday.

### 2.5 Triangulation Error Vectors
Three error vectors computed on the unified Spot/Futures/Options DataFrame:
- `E_SF` = `F - S × e^((r-q)×t)` — Cost-of-Carry deviation (Spot vs Futures)
- `E_FO` = `(C - P) - (F - K) × e^(-r×t)` — Put-Call Parity deviation (Futures vs Options)
- `E_SO` = `(C - P) - (S × e^((r-q)×t) - K) × e^(-r×t)` — (Spot vs Options)

Attribution logic:
- `E_FO` small, `E_SF` & `E_SO` large → **Spot is the anomaly** (Code: 256)
- `E_SO` small, `E_SF` & `E_FO` large → **Futures is the anomaly** (Code: 512)
- `E_SF` small, `E_FO` & `E_SO` large → **Options is the anomaly** (Code: 1024)

---

## Phase 3 — Warehouse Architecture
**Session:** `d2c08b9f` (2026-05-16, first half)

### 3.1 Single Canonical Warehouse Decision
- **Considered:** Two separate warehouses (one per consumer — Strategy Team vs. Feature Engine).
- **Rejected because:**
  1. The brief explicitly asks to "show how one layout serves two conflicting patterns" — dual warehouses sidestep the architectural challenge.
  2. Double storage cost. At 2-year scale with options chain depth, duplication is untenable.
- **Decision:** One canonical warehouse with **asset-class-specific partitioning** per the dominant access pattern for each instrument.

### 3.2 Partitioning Scheme — Defensible Rationale

| Asset | Partition | Internal Sort | Reason |
|---|---|---|---|
| Spot | `trade_date=` | `timestamp` | Feature Engine pulls full days; Strategy Team filters by date first |
| Futures | `contract_type=` / `trade_date=` | `timestamp` | Separates near/mid contracts cleanly without duplication |
| Options | `trade_date=` / `expiry_str=` | `timestamp, strike, side` | Prevents small-file explosion from strike-level partitioning |
| VIX | `trade_date=` | `timestamp` | Mirrors spot cadence |
| FII/DII | flat file | — | Daily cadence; no sub-daily access pattern |

### 3.3 Options Partitioning Anti-Pattern Avoided
- Partitioning Options by `strike` would create **thousands of tiny 5 KB files** per day — a catastrophic pattern for Parquet (file open overhead dominates read time). Industry practice (HFT/quant funds) keeps the entire daily snapshot intact.

### 3.4 Row Group Size Tuning
- Default row group size (100,000 rows) caused all options data for a full day (~4,500 rows) to fall in **one row group**, eliminating DuckDB's ability to skip time chunks.
- **Decision:** Set `row_group_size=1000` for options → creates ~5 row groups per day → DuckDB skips 80%+ of disk I/O on timestamp-filtered queries.
- Spot/Futures/VIX: `row_group_size=10000` (adequate for their 375 rows/day).

### 3.5 Dictionary Encoding
- Applied `use_dictionary` on low-cardinality columns:
  - `side` (CE/PE) — 2 values; encodes to 1 bit
  - `contract_type` (near/mid) — 2 values
  - `anomaly_code` — sparse integer; dictionary saves significant space

### 3.6 Compression
- **zstd** selected over snappy/gzip for all assets: better compression ratio, hardware-accelerated decompression, lower CPU than gzip.

### 3.7 Idempotency Guarantee
- Pipeline wipes `warehouse/` directory completely at the start (`shutil.rmtree`).
- SHA-256 checksum manifest (`ingestion_manifest.parquet`) generated from raw source files — not from wall-clock timestamps, ensuring byte-identical manifests on re-runs.
- Deterministic sort before write (`sort_values` by `['timestamp', 'strike', 'side']`) guarantees byte-identical Parquet row group boundaries.

### 3.8 DuckDB Logical Views
- Five read-only views created: `v_spot`, `v_futures`, `v_options`, `v_vix`, `v_fii_dii`.
- Views built with **absolute paths** embedded at creation time so the `.duckdb` file is portable and can be queried from any working directory.

### 3.9 MLflow Experiment Tracking
- Every pipeline run is logged to a `Data_Engine_Ingestion` MLflow experiment.
- Metrics logged: `anomalies_spot`, `anomalies_fut`, `anomalies_opt`, `ingestion_time_seconds`.
- Provides audit trail for every warehouse rebuild.

---

## Phase 4/5 — Data Access Layer (API)
**Session:** `d2c08b9f` (2026-05-16, second half)

### 4.1 `get_price(timestamp)` — Edge Cases
- Returns a single flat row (Spot + Near Future + Mid Future) via a double `LEFT JOIN` on `v_futures` with `contract_type` filters.
- **Pre-open edge case:** Requests at 09:05 (pre-open auction) return an empty DataFrame because the strict `WHERE timestamp = ...` finds no row — downstream algorithms are protected from auction ghost prints.

### 4.2 `get_signals(timestamp)` — AS-OF Join
- Options snapshots are at 5-minute intervals. A request at `10:17` must return the `10:15` snapshot.
- **Implementation:** Scans back up to 5 minutes for the nearest valid snapshot (`MAX(timestamp) WHERE timestamp <= requested AND timestamp >= requested - 5min`).
- Returns empty if the previous snapshot is older than 5 minutes (stale state guard).

### 4.3 `get_features(timestamp)` — Lookback Window
- Returns a 30-minute rolling window of Spot OHLCV + anomaly codes.
- If the lookback window extends before available data (e.g., `2025-08-22 09:45`), returns what is available — padded with NaNs rather than raising an error.

### 4.4 `get_features_batch(timestamps)` — Zero-Copy Vectorized Batch
- Registers the requested timestamp list as an **in-memory virtual table** in DuckDB (`con.register()`).
- Executes a single vectorized `LEFT JOIN` across thousands of timestamps in one pass.
- **Result: 0.017 ms amortized per timestamp** (vs. ~8 ms for individual `get_price` calls).
- Missing timestamps yield `NULL` rows, preserving input alignment for downstream ML pipelines.

### 4.5 Path Portability Fix
- Initial API used a relative `warehouse/` path. When called from the project root, it couldn't find the `.duckdb` file.
- **Fix:** Both `api.py` and `run_pipeline.py` use `os.path.abspath(__file__)` to resolve all paths relative to the script's location, making both CWD-agnostic.

---

## Phase 6 — Benchmarking
**Session:** `d2c08b9f` (2026-05-16, end)

### Benchmark Results (100 iterations each, 1000 for batch)

| Function | Median (ms) | P99 (ms) | Notes |
|---|---|---|---|
| `get_price` | 7.70 | 9.44 | Single timestamp point lookup |
| `get_signals` | 9.76 | 12.20 | AS-OF join + full chain scan |
| `get_features` | 3.00 | 3.51 | 30-min window lookback |
| `get_features_batch` | **0.017** | **0.017** | 1000 timestamps, amortized |

`get_features_batch` achieves near-constant latency — the overhead is entirely DuckDB's vectorized scan, not per-row Python overhead.

---

## Phase 7 (Post-Submission) — Purely DuckDB-Based Ingestion & Writing

### 7.1 Problem
Parquet stores `datetime64` as `int64` microseconds since epoch. Each timestamp value is ~1.75 × 10¹⁵ µs — a 52-bit number. With default `PLAIN` encoding, this costs a flat 8 bytes per row regardless of the data's structure.
Furthermore, PyArrow's `WriterProperties` API version mismatch can raise unexpected `AttributeError` exceptions in environments with older PyArrow versions.

### 7.2 Why Delta Works Here
For 1-minute NIFTY bars, sorted timestamps produce **constant deltas of exactly 60,000,000 µs**. For 5-minute options snapshots, deltas are 300,000,000 µs. `DELTA_BINARY_PACKED` encoding stores only the first value plus the tiny, near-constant deltas — these pack to ~1–2 bits each after zigzag encoding.

**Expected savings: ~60–70% reduction in timestamp column bytes.**

### 7.3 Implementation: Pure C++ DuckDB COPY Engine
Instead of PyArrow or pandas (`to_parquet`), we register the processed pandas DataFrames directly in an active DuckDB database connection and write partitioned Parquet files using the native C++ engine via `COPY ... TO`:

```sql
COPY df_spot TO 'warehouse/spot' (
    FORMAT PARQUET, 
    PARTITION_BY (trade_date), 
    COMPRESSION zstd, 
    PARQUET_VERSION 'V2', 
    ROW_GROUP_SIZE 10000
)
```

**Benefits:**
- **Zero PyArrow dependencies** for writing/encoding, completely bypassing version mismatch errors.
- Native `PARQUET_VERSION 'V2'` automatically enables `DELTA_BINARY_PACKED` encoding for monotonic numeric columns (timestamps, snapshots, index offsets).
- **23% ingestion speedup**: Pipeline completion time dropped from **0.86 seconds** to **0.66 seconds**!

### 7.4 Columns Delta-Encoded Per Table

| Table | Delta-Encoded Columns | Rationale |
|---|---|---|
| Spot | `timestamp` | Uniform 60s deltas |
| Futures | `timestamp` | Uniform 60s deltas |
| Options | `timestamp`, `expiry`, `snapshot_id` | 300s deltas; `expiry` constant per partition; `snapshot_id` monotone int |
| VIX | `timestamp` | Uniform 60s deltas |
| FII/DII | — | Daily cadence, no intra-day timestamps |

### 7.6 Strict Schema Downcasting & Enum Encoding
Further optimized the warehouse physical schema by wrapping the `COPY` statements with explicit `SELECT` projections:
- **Int64 → UInt32 Downcasting:** `volume`, `oi`, `strike`, and `snapshot_id` columns were downcasted to 32-bit unsigned integers. A 1-minute NIFTY volume or strike will never exceed 4.2 billion, instantly saving 4 bytes per row.
- **Explicit Dictionary Encoding (Enum):** DuckDB automatically dictionary-encodes `ENUM` types. We explicitly cast `side AS ENUM('CE', 'PE')` and `contract_type AS ENUM('near', 'mid')` during the export. This ensures these categorical strings consume only 1 bit of storage per row, rather than being stored as full repeated strings or variable-length characters.
- **Partition Columns:** Partition columns (`trade_date` and `expiry_str`) are natively encoded as directory structures (`trade_date=2025-08-22/`) and therefore cost **0 bytes** of per-row storage inside the Parquet data pages. No further encoding is needed for them.

---

## Validation Module — Full Error Code Dictionary

| Bit | Code | Check | Applied To |
|---|---|---|---|
| 0 | 1 | Backward time jump | Spot, Futures, Options |
| 1 | 2 | Duplicate timestamp | Spot, Futures |
| 2 | 4 | Missing 1-min candle (gap-filled) | Spot |
| 3 | 8 | OHLC integrity violation (Low > High, etc.) | Spot, Futures, Options |
| 4 | 16 | Price spike > 1% in single candle | Spot |
| 5 | 32 | Abnormal futures spread > 5% (near vs mid) | Futures |
| 6 | 64 | Stale price (no change for 5+ mins with volume > 0) | Futures |
| 7 | 128 | Invalid option strike (≤ 0) or invalid side (not CE/PE) | Options |
| 8 | 256 | Triangulation: Isolated Spot anomaly | Spot |
| 9 | 512 | Triangulation: Isolated Futures anomaly | Futures |
| 10 | 1024 | Triangulation: Isolated Options anomaly | Options |

Codes are **composable via bitwise OR** — a row flagged for duplicate timestamp (2) AND OHLC violation (8) gets `anomaly_code = 10`. `decode_anomalies(10)` returns both descriptions.

---

## Key Architectural Decisions Summary

| Decision | Choice | Alternative Considered | Reason |
|---|---|---|---|
| Warehouse count | Single canonical | Dual (per-consumer) | Avoids data duplication; addresses brief's explicit tradeoff challenge |
| Options partitioning | `trade_date / expiry_str` | By strike | Strike-level = thousands of tiny files; kills Parquet performance |
| Row group size (options) | 2,048 rows (DuckDB min) | Default 122,880 | Enables DuckDB row-group skipping; skips 80%+ I/O on point queries |
| Compression | zstd | snappy, gzip | Best ratio + hardware-accelerated decompression |
| Ingestion Engine | **DuckDB COPY Engine** | PyArrow / pandas | Resolves PyArrow version/attribute bugs; 23% faster write speed |
| Timestamp encoding | DELTA_BINARY_PACKED | PLAIN (default) | ~60–70% byte savings on monotone int64 columns via PARQUET_VERSION 'V2' |
| Enum / Dict Encoding | `CAST(col AS ENUM)` | String/Varchar | Drops 2-value categoricals (`side`, `contract_type`) to 1-bit per row |
| Integer Downcasting | `CAST(col AS UINT32)` | Int64 | Saves 4 bytes per row on `volume`, `oi`, `strike`, `snapshot_id` |
| Batch API | Polars Virtual table + single JOIN | Per-timestamp loop | 0.053 ms/ts vs ~8 ms/ts; 150× speedup |
| Triangulation threshold | VIX-dynamic | Static | Prevents flagging natural high-vol noise as data errors |
| Idempotency | Full directory wipe + SHA-256 manifest | Append + upsert | Guarantees byte-identical output; no ghost rows from prior runs |

---

## Final Performance & Size Metrics

### Physical Size Reduction Summary
- **Raw CSV Files:** 4,446.1 KB (4.342 MB)
- **Optimized Parquet Warehouse:** **1,083.2 KB (1.058 MB)**
- **Reduction Ratio:** **75.6% reduction** (4.10x smaller footprint)

### Verified Page-Level Storage Encoding Profile
- **`timestamp`, `expiry`, `snapshot_id`**: `DELTA_BINARY_PACKED` / `RLE_DICTIONARY` compression.
- **`side`, `contract_type`, `trade_date`**: `RLE_DICTIONARY` (effectively 1-bit per row).
- **`open`, `high`, `low`, `close`, `iv`, `vix_close`**: Native `BYTE_STREAM_SPLIT` to compress floating point values efficiently.

### API Speed Metrics & Polars Integration
- `get_price`: **~7.7 ms** (Median) / **~8.9 ms** (P99)
- `get_signals`: **~9.9 ms** (Median) / **~13.8 ms** (P99)
- `get_features` (Lookback window): **~3.2 ms** (Median) / **~4.3 ms** (P99)
- `get_features_batch`: **0.048 ms/timestamp** (Median) / **0.048 ms/timestamp** (P99)

**Zero-Copy Polars Integration:**
All data API functions now return Polars DataFrames (`pl.DataFrame`) via DuckDB's native `.pl()` zero-copy Arrow exchange, entirely bypassing Pandas memory allocations.
Furthermore, `run_pipeline.py` was updated to utilize `pl.scan_csv()` and `pl.read_csv()` for raw data loading, capitalizing on Rust-based multithreaded I/O to parse thousands of CSV rows and execute the entire ingestion, validation, and Parquet storage pipeline in **~1.06 seconds**.
