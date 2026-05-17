# Project Workflow: Quant Singularity Data Engine Intern Screening

This document outlines the phased approach to completing the internship challenge. The goal is to build a robust, production-grade data layer with a heavy emphasis on data validation and warehouse design.

---

## Phase 1: Setup & Initialization
- [x] **Environment Configuration**: Create `requirements.txt` with dependencies (`pandas`, `polars`, `duckdb`, `pyarrow`, `mlflow`, `pytest`).
- [x] **MLflow Integration**: Initialize MLflow tracking to log every run from the start.
- [x] **Repository Structure**: Define the directory layout (e.g., `src/ingestion`, `src/validation`, `src/api`).

## Phase 2: Data Exploration & "Anomaly Hunt"
- [x] **Exploratory Data Analysis (EDA)**: Use `main.ipynb` to inspect each CSV in `intern_data_db`.
- [x] **Anomaly Identification**: Locate specific data issues (gaps, price spikes, incorrect expiries, volume mismatches).
- [x] **Validation Logic Definition**: Define rules for the Validation Module based on findings.

## Phase 3: Warehouse Architecture & Schema Design
- [x] **Partitioning Strategy**: Decide on the Parquet partitioning scheme (e.g., `date/asset_class/symbol`) to balance conflicting access patterns.
- [x] **DuckDB Schema**: Design the tables/views for efficient querying.
- [x] **Idempotency Plan**: Design the ingestion script to ensure byte-identical output on repeated runs.

## Phase 4: Ingestion & Validation Pipeline
- [x] **Ingestion Module**: Code the logic to read raw CSVs and convert them to cleaned Parquet files.
- [ ] **Validation Module**: Implement the check-suite that flags/handles anomalies found in Phase 2.
- [ ] **Single Command Run**: Ensure a single entry point (e.g., `python main.py`) triggers the entire pipeline.

## Phase 5: Data Access Layer (API)
- [ ] **Contract Definition**: Define edge-case behavior for all functions (missing data, non-trading hours).
- [ ] **Implement `get_price`**: Fetch OHLCV for specific symbols/timestamps.
- [ ] **Implement `get_features`**: Handle lookback windows and feature computation.
- [ ] **Implement `get_signals`**: Handle options chain logic.
- [ ] **Implement `get_features_batch`**: Optimize for high-throughput batch retrieval.

## Phase 6: Benchmarking & Stress Testing
- [ ] **Latency Suite**: Measure median and P99 latency across 100+ timestamps.
- [ ] **Batch Throughput**: Benchmark `get_features_batch` for 1,000 timestamps.
- [ ] **Adversarial Testing**: Test functions against pre-open sessions, expiry days, and missing snapshots.

## Phase 7: Final Deliverables & Report
- [ ] **The Written Report (PDF)**: 
    - [ ] Section 1: Warehouse design & scale analysis.
    - [ ] Section 2: Detailed "What the validation module found".
    - [ ] Section 3: Access function edge-case contracts.
    - [ ] Section 4: Benchmark results analysis.
    - [ ] Section 5: Production readiness & confidence intervals.
    - [ ] Section 6: Correctness proof & adversarial results.
- [ ] **Code Cleanup**: Finalize README, requirements, and ensure clean execution.
- [ ] **Private GitHub Repo**: Push code, MLflow artifacts, and benchmark results.
