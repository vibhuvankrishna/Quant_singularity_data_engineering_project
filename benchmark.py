import time
import polars as pl
import numpy as np
import sys
import os
import mlflow

sys.path.append(os.path.abspath('.'))

from src.api import DataAPI

def run_benchmarks():
    pl.Config.set_tbl_formatting("ASCII_MARKDOWN")
    print("Initializing API for benchmarking...")
    api = DataAPI()

    # Get a list of 100 valid timestamps from the spot table
    print("Fetching 100 distinct timestamps for testing...")
    query = "SELECT DISTINCT timestamp FROM v_spot ORDER BY timestamp LIMIT 100"
    ts_df = api.con.execute(query).pl()
    timestamps_100 = ts_df['timestamp'].to_list()
    
    # Get 1000 valid timestamps for batch testing
    query_1000 = "SELECT DISTINCT timestamp FROM v_spot ORDER BY timestamp LIMIT 1000"
    ts_df_1000 = api.con.execute(query_1000).pl()
    timestamps_1000 = ts_df_1000['timestamp'].to_list()

    results = []

    # 1. get_price
    print(f"Benchmarking get_price across {len(timestamps_100)} timestamps...")
    latencies = []
    for ts in timestamps_100:
        start = time.perf_counter()
        _ = api.get_price(ts)
        latencies.append((time.perf_counter() - start) * 1000) # in ms
    results.append({
        'Function': 'get_price',
        'Count': len(timestamps_100),
        'Median_Latency_ms': np.median(latencies),
        'P99_Latency_ms': np.percentile(latencies, 99),
        'Total_WallClock_ms': sum(latencies)
    })

    # 2. get_signals
    print(f"Benchmarking get_signals across {len(timestamps_100)} timestamps...")
    latencies = []
    for ts in timestamps_100:
        start = time.perf_counter()
        _ = api.get_signals(ts)
        latencies.append((time.perf_counter() - start) * 1000)
    results.append({
        'Function': 'get_signals',
        'Count': len(timestamps_100),
        'Median_Latency_ms': np.median(latencies),
        'P99_Latency_ms': np.percentile(latencies, 99),
        'Total_WallClock_ms': sum(latencies)
    })

    # 3. get_features
    print(f"Benchmarking get_features across {len(timestamps_100)} timestamps...")
    latencies = []
    for ts in timestamps_100:
        start = time.perf_counter()
        _ = api.get_features(ts)
        latencies.append((time.perf_counter() - start) * 1000)
    results.append({
        'Function': 'get_features',
        'Count': len(timestamps_100),
        'Median_Latency_ms': np.median(latencies),
        'P99_Latency_ms': np.percentile(latencies, 99),
        'Total_WallClock_ms': sum(latencies)
    })

    # 4. get_features_batch
    print(f"Benchmarking get_features_batch across {len(timestamps_1000)} timestamps...")
    start = time.perf_counter()
    _ = api.get_features_batch(timestamps_1000)
    batch_wall_clock = (time.perf_counter() - start) * 1000
    results.append({
        'Function': 'get_features_batch',
        'Count': len(timestamps_1000),
        'Median_Latency_ms': batch_wall_clock / len(timestamps_1000), # Amortized
        'P99_Latency_ms': batch_wall_clock / len(timestamps_1000),    # Amortized
        'Total_WallClock_ms': batch_wall_clock
    })

    # Save Results
    results_df = pl.DataFrame(results)
    results_df = results_df.with_columns([
        pl.col(pl.Float64).round(3)
    ])
    results_df.write_csv("benchmark.csv")
    
    print("\n=== Benchmark Results ===")
    print(results_df)
    print("\nSaved to benchmark.csv")

    # --- Log to MLflow ---
    print("\nLogging results to MLflow...")
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    mlflow_dir = os.path.join(BASE_DIR, "mlflow_data").replace("\\", "/")
    mlflow.set_tracking_uri(f"file:///{mlflow_dir}")
    mlflow.set_experiment("Data_Engine_Benchmarks")

    with mlflow.start_run(run_name=f"Benchmark_{time.strftime('%Y%m%d_%H%M%S')}"):
        # Log metrics for each function
        for res in results:
            prefix = res['Function']
            mlflow.log_metric(f"{prefix}_median_ms", res['Median_Latency_ms'])
            mlflow.log_metric(f"{prefix}_p99_ms", res['P99_Latency_ms'])
            mlflow.log_metric(f"{prefix}_wallclock_ms", res['Total_WallClock_ms'])
            
        # Log the full CSV as a tracked artifact
        mlflow.log_artifact("benchmark.csv")
        print("MLflow logging complete!")

if __name__ == "__main__":
    run_benchmarks()
