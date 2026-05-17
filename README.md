# Quant Singularity Data Engine Intern Project

This repository contains the completed data layer for the Quant Singularity Intern Screening Project. It builds a **fully idempotent, validated, and storage-optimized Parquet + DuckDB warehouse** tailored for two conflicting downstream consumers: a Strategy Team (date-first, symbol-level) and a Feature Engine (cross-sectional, timestamp-first).

---

## Project Structure

```
main_folder/
├── run_pipeline.py        # Core ingestion, validation, and warehouse build script
├── main.py                # Single-command entry point (python main.py)
├── benchmark.py           # API latency benchmarking suite
├── src/
│   ├── validation.py      # Bitmask-driven validation engine (11 error codes)
│   └── api.py             # Typed data access layer (4 access functions)
├── warehouse/             # Main: Partitioned Parquet files + DuckDB logical views
├── anomaly_warehouse/     # Anomalies: Flags, original columns, and anomaly rows
├── ENGINEERING_LOG.md     # Full decision log across all dev sessions
├── EDA_notebook.ipynb     # "Anomaly Hunt" journal — data integrity deep-dive
├── API_Test.ipynb         # Interactive edge-case testing of all API functions
└── benchmark.csv          # Measured P99 and median latency results
```

---

## Getting Started

### 1. Installation
Python 3.11+ recommended.
```bash
pip install -r requirements.txt
```

### 2. Single Command Run

The entire pipeline (ingest → validate → write warehouse → build DuckDB views) is idempotent and runs via:

```bash
python main.py
```

**What this does, in order:**
1. Reads all raw CSVs (`nifty_spot`, `nifty_futures`, `options_chain`, `india_vix`, `fii_dii_flow`).
2. Runs the 11-check validation suite → stamps each row with a composable bitmask `anomaly_code`.
3. Performs **Volatility-Aware Triangular Validation** across all three instruments simultaneously.
4. Writes optimized, Hive-partitioned Parquet files to `warehouse/`.
5. Isolates and writes flagged anomalous rows to a parallel `anomaly_warehouse/` (preserving exact layouts, schemas, and compression).
6. Generates `ingestion_manifest.parquet` with SHA-256 checksums for reproducibility.
7. Creates DuckDB logical views (`v_spot`, `v_futures`, `v_options`, `v_vix`, `v_fii_dii`).
8. Logs all run metrics to MLflow (`Data_Engine_Ingestion` experiment).

### 3. Running the API

```python
import sys
sys.path.append('path/to/main_folder')
from src.api import DataAPI
import pandas as pd

api = DataAPI()

# Point-in-time price lookup
print(api.get_price(pd.Timestamp('2025-08-22 10:15:00')))

# AS-OF options chain (handles between-snapshot requests)
print(api.get_signals(pd.Timestamp('2025-08-22 10:17:00')))

# 30-minute feature lookback
print(api.get_features(pd.Timestamp('2025-08-22 11:00:00')))

# Vectorized batch — 1000 timestamps in ~17ms total
timestamps = [pd.Timestamp('2025-08-22 10:15:00'), pd.Timestamp('2025-08-22 10:16:00')]
print(api.get_features_batch(timestamps))
```

### 4. Running the Benchmarks

```bash
python benchmark.py
```

Generates `benchmark.csv` with P99 and median latencies over 100+ iterations.

---

## Architecture & Key Engineering Decisions

The warehouse is specifically designed to balance two conflicting access patterns without data duplication.

### 1. Single Canonical Warehouse 



**The chosen design uses asset-class-specific partitioning instead:**

| Asset | Partition | Internal Sort |
|---|---|---|
| Spot | `trade_date=` | `timestamp` |
| Futures | `contract_type=` / `trade_date=` | `timestamp` |
| Options | `trade_date=` / `expiry_str=` | `timestamp, strike, side` |
| VIX | `trade_date=` | `timestamp` |

### 2. Options: Anti-Strike-Partition Design

Partitioning Options by `strike` would create **thousands of tiny ~5 KB files per day** — a well-known Parquet anti-pattern. File-open overhead completely dominates read time. The chosen `trade_date / expiry_str` layout keeps the full daily snapshot in one file, matching how HFT desks and quant funds actually store options chains.

### 3. Row Group Size Tuning → Predicate Pushdown

| Asset | Row Group Size | Why |
|---|---|---|
| Options | **2,048 rows** | DuckDB's minimum row group size (corresponds to internal vector size). ~2 row groups/day → enables granular DuckDB row-group skipping. |
| Spot / Futures / VIX | 10,000 rows | Their 375-row/day depth is coarse and naturally fast to parse. |

The core insight: DuckDB evaluates Parquet `min/max` statistics **per row group**. Smaller groups = finer-grained statistics = more I/O skipped.

### 4. Bitmask Anomaly Encoding

All errors are encoded as a single `uint32 anomaly_code` column using powers of 2:

| Code | Description |
|---|---|
| `1` | Backward time jump |
| `2` | Duplicate timestamp |
| `4` | Missing 1-min candle |
| `8` | OHLC violation (Low > High, etc.) |
| `16` | Price spike > 1% in one candle |
| `32` | Futures spread > 5% (near vs. mid) |
| `64` | Stale price (no change, 5+ min, volume > 0) |
| `128` | Invalid option strike or side |
| `256` | Triangulation: Isolated **Spot** anomaly |
| `512` | Triangulation: Isolated **Futures** anomaly |
| `1024` | Triangulation: Isolated **Options** anomaly |

Codes compose cleanly: `anomaly_code = 10` means codes `2` AND `8` are both set. Use `decode_anomalies(code)` in `validation.py` to get a human-readable list.

### 5. Volatility-Aware Triangular Validation

This is the most novel check in the suite. It cross-validates Spot, Futures, and the Options synthetic future `(C − P)` simultaneously at each timestamp.

**Why it works:** If the Spot price spikes 200 points, the near-month Future and the median synthetic future across all strikes must also move ~200 points. If two instruments agree but one diverges beyond the threshold, the outlier is isolated as a bad tick.

**VIX-Dynamic Thresholds:** Static tolerances misfire during market stress. During high-VIX periods (wide bid-ask spreads, fast moves), the threshold scales as `T = T_base × (VIX / 15.0)`, preventing flagging of natural volatility noise as data errors.

**OTM Strike Robustness:** The synthetic future is computed as the **median** Δ(C − P) across all strikes. Deep-OTM stale prints have minimal weight; ATM liquidity dominates.

**Theta-neutrality:** `C − P` is immune to theta decay because long-call and short-put theta cancel exactly — making it a safe intraday triangulation signal.

### 6. Pure DuckDB Timestamp Delta Encoding

Parquet stores `datetime64` as `int64` microseconds since epoch (~1.75 × 10¹⁵ µs — a 52-bit number). Default `PLAIN` encoding wastes 8 bytes per row.

By utilizing DuckDB's native C++ Parquet engine with `PARQUET_VERSION 'V2'`, monotone integer sequences (like sorted 1-minute bar timestamps with constant 60s deltas, 5-minute option snaps with constant 300s deltas, and index `snapshot_id` columns) are automatically compressed with `DELTA_BINARY_PACKED`.

**Result:**
- **~60–70% reduction in timestamp column bytes.**
- **23% ingestion write speedup** over PyArrow (completes in **0.66 seconds** total!).
- Zero PyArrow write dependencies, avoiding system-specific `WriterProperties` errors.

### 7. Strict Schema Downcasting & Enum Encoding

Further optimized the physical storage by explicitly wrapping the `COPY` statements with `SELECT` projections:
- **Int64 → UInt32 Downcasting:** `volume`, `oi`, `strike`, and `snapshot_id` columns were downcasted to 32-bit integers, instantly halving their memory/storage footprints (saving 4 bytes per row).
- **Explicit Dictionary Encoding:** By casting categorical columns as DuckDB Enums (`CAST(side AS ENUM('CE', 'PE'))`), DuckDB forces perfect dictionary encoding. These strings consume just 1 bit per row.
- **Zero-Byte Partition Storage:** Columns like `trade_date` and `expiry_str` are entirely omitted from the Parquet data pages because they exist purely in the directory structure (e.g. `trade_date=2025-08-22/`).

### 8. Zero-Copy Batch API (`get_features_batch`)

Instead of looping `get_price` N times, `get_features_batch` registers the full timestamp list as an **in-memory DuckDB virtual table** and executes a single vectorized `LEFT JOIN`. Missing timestamps produce `NULL` rows, preserving input alignment for ML pipelines.

### 9. Idempotency Guarantee

- Full `warehouse/` directory wipe on every run via `shutil.rmtree` (no ghost files from prior runs).
- SHA-256 checksums of raw source files stored in `ingestion_manifest.parquet` — not wall-clock timestamps.
- Deterministic `sort_values` before write guarantees **byte-identical Parquet row group boundaries** on repeated runs.

## Benchmark & Storage Metrics

### 1. Storage Size Comparison
- **Raw CSV Size (Uncompressed Text):** 4.34 MB
- **Optimized Parquet Warehouse Size:** **1.06 MB**
- **Space Reduction:** **75.6% savings** (4.10x smaller footprint!)
- **Physical Encoding Profile (Page Level):**
  - Monotonically increasing `timestamp`, `expiry`, and `snapshot_id` columns use `DELTA_BINARY_PACKED` or `RLE_DICTIONARY` compression.
  - Floating point `open`, `high`, `low`, `close`, `iv`, and `vix_close` columns utilize native `BYTE_STREAM_SPLIT` to optimize mantissa & exponent encoding before Zstd compression.
  - Categorical columns (like `side` and `contract_type`) use `RLE_DICTIONARY` (effectively ~1-bit/row storage).

### 2. API Access Benchmarks
_Generated by `benchmark.py` over 100 iterations (batch tests using 1,000 timestamps)._

| Function | Median (ms) | P99 (ms) | Notes |
|---|---|---|---|
| `get_price` | 8.16 | 10.38 | Spot + Near/Mid Futures in one query |
| `get_signals` | 9.58 | 13.19 | AS-OF join across the full options chain |
| `get_features` | 2.99 | 4.08 | 30-min lookback window |
| `get_features_batch` | **0.053** | **0.053** | 1,000 timestamps, amortized |

*   **Polars Optimization:** The internal DataFrame creation in `get_features_batch` was migrated from Pandas to Polars (`pl.DataFrame`). Polars uses zero-copy Apache Arrow-backed memory, eliminating slow list comprehensions and datetime-string conversions to **reduce the in-memory dataframe instantiation time by 55%** (dropping from 6.3ms to 2.8ms).
*   `get_features_batch` remains **~150× faster** than querying individual timestamps in a loop.

---

## Data Anomalies Discovered

Full observations are in `EDA_notebook.ipynb`. Key findings:

- **False Spot Spikes (Code 256):** Several intraday Spot movements of 200+ points had zero echo in near-month Futures AND zero echo in the options synthetic future — confirming them as vendor feed errors, not real market events.
- **Stale Futures Prints (Code 64):** Consecutive bars with identical `near_month_close` despite non-zero volume detected and flagged.
- **Options on Expiry Day:** OTM options near expiry show artificially wide `E_FO` errors even when data is valid (near-zero time value + wide spreads). The VIX-dynamic threshold handles this by increasing tolerance automatically as volatility rises.

*For an interactive walkthrough of extreme edge cases (pre-open auctions, future timestamps, missing snapshots), see `API_Test.ipynb`.*

---

## For a Detailed Engineering Decision Log

See [`ENGINEERING_LOG.md`](ENGINEERING_LOG.md) — a chronological record of every significant architectural decision, bug discovered, design tradeoff made, and performance optimization applied across all development sessions.
