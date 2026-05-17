# Phase 3: Warehouse Architecture & Schema Design

## 1. Detailed To-Do List

- [ ] **Finalize Partitioning Strategy**: Define the exact directory structure for Parquet files that balances the Strategy Team's and Feature Engine's access patterns.
- [ ] **Establish Sorting Keys**: Define the precise sorting hierarchy for each asset class to guarantee fast row-group pruning and idempotency.
- [ ] **Design DuckDB Logical Schema**: Write the SQL view definitions that map DuckDB to the underlying partitioned Parquet files without duplicating storage.
- [ ] **Develop Idempotency Protocol**: Establish strict rules for the ingestion script (deterministic sorting, overwrite-by-partition, metadata stripping) to ensure byte-identical outputs.
- [ ] **Address Data Quality Flags**: Decide whether anomalies identified in Phase 2 are dropped, clamped, or simply flagged with an `is_anomaly` boolean column in the warehouse.

---

## 2. Partitioning Strategy & Defense

### The Conflicting Access Patterns
1.  **Strategy Team**: Slices by `symbol` and `date range` (e.g., "Give me all NIFTY options for the month of August").
2.  **Feature Engine**: Slices cross-sectionally by `timestamp` (e.g., "Give me the entire options chain and spot price at exactly 10:15 AM across all symbols").

### Chosen Strategy: Time-First Hive Partitioning with Internal Sorting
**Structure**: `warehouse/<asset_class>/year=YYYY/month=MM/date=YYYY-MM-DD/data.parquet`

**Internal Parquet Sorting**: Within `data.parquet`, the data MUST be sorted by `[symbol, timestamp]` (for Spot/Futures) or `[symbol, timestamp, strike, side]` (for Options).

### Defense & Scale Reasoning
If we partition the filesystem by `symbol` (e.g., `symbol=NIFTY/date=...`), the Feature Engine would be crippled. To get a cross-sectional snapshot at 10:15 AM for 2,000 NSE instruments, it would have to open 2,000 separate Parquet files simultaneously. This breaks file handle limits and involves massive I/O overhead.

By partitioning by `date`, the Feature Engine opens exactly one file (or a few files) per day and reads the cross-section. 
"But doesn't this hurt the Strategy Team?" No. Because we strictly internally sort the Parquet file by `symbol` first, Parquet's columnar metadata (Row Group Min/Max statistics) will instantly know exactly which byte-ranges inside that daily file contain the `NIFTY` data. The Strategy Team's query will skip 99% of the file without reading it into memory.

**What breaks at 2-year scale across all NSE instruments?**
If we put 2,000 instruments' 1-minute options data into a *single* daily file, the file might become too large (multiple GBs per day), causing memory issues during the initial write/sort phase. At that scale, we would need to introduce a secondary partition layer (e.g., `date=YYYY-MM-DD/bucket=XX/`), where `bucket` is a hash of the symbol, keeping file sizes in the sweet spot of ~250MB to 1GB.

---

## 3. DuckDB Schema Design

We will not load data *into* a DuckDB `.db` file. Doing so violates the single-source-of-truth principle and doubles storage costs. Instead, DuckDB will act as a compute engine querying the Parquet warehouse directly using Views.

### Schema Definition
```sql
CREATE VIEW v_spot AS 
SELECT * FROM read_parquet('warehouse/spot/*/*/*/*.parquet', hive_partitioning=true);

CREATE VIEW v_futures AS 
SELECT * FROM read_parquet('warehouse/futures/*/*/*/*.parquet', hive_partitioning=true);

CREATE VIEW v_options AS 
SELECT * FROM read_parquet('warehouse/options/*/*/*/*.parquet', hive_partitioning=true);

CREATE VIEW v_vix AS 
SELECT * FROM read_parquet('warehouse/aux/vix/*/*/*/*.parquet', hive_partitioning=true);
```

### Defense
DuckDB's `read_parquet` with `hive_partitioning=true` automatically reads the `year`, `month`, and `date` from the directory structure and exposes them as columns. When the Strategy Team queries `WHERE date >= '2025-08-22'`, DuckDB's optimizer pushes this filter down to the filesystem, entirely skipping the directories of other dates.

---

## 4. Idempotency Plan

The brief mandates that "rerunning on the same input produces byte-identical output." This is notoriously difficult due to non-deterministic writing.

### The Protocol
1.  **Stateless Partition Overwrite**: The ingestion script will never "append" to a file. If a script is run for `2025-08-22`, it will aggressively delete the existing `date=2025-08-22` directory and rewrite it from scratch. Appends create new row groups and break byte-identity.
2.  **Absolute Deterministic Sorting**: Before writing, Polars/Pandas will `sort()` the dataframe on a comprehensive key: `['timestamp', 'strike', 'side']`. If rows have identical timestamps and strikes, their physical order on disk will flip randomly between runs unless explicitly sorted.
3.  **Parquet Metadata Stripping**: Libraries often embed "file creation time" in the Parquet footer. We will configure PyArrow/Polars to write with fixed metadata parameters (e.g., disabling dictionary encoding if it proves non-deterministic, and ensuring `write_statistics=True` is stable).
4.  **Anomaly Handling**: Anomalies identified in Phase 2 (the Triangulation Validation) will NOT be dropped. Dropping data silently is dangerous. Instead, we will add a boolean column `is_anomaly` and a string column `anomaly_reason`. The data is stored deterministically; downstream consumers can choose `WHERE is_anomaly = false`.
