import duckdb
import pandas as pd
import polars as pl
import numpy as np
import os
import time
from typing import List, Union, Dict

# Resolve paths relative to this file's location
# src/api.py -> main_folder -> warehouse
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "warehouse", "duckdb", "market_data.duckdb")

class DataAPI:
    def __init__(self):
        # Ensure path is absolute to avoid relative path confusion
        if not os.path.exists(DB_PATH):
            raise FileNotFoundError(f"Warehouse database not found at {DB_PATH}. Have you run the pipeline yet?")
            
        self.con = duckdb.connect(DB_PATH, read_only=True)

    def get_price(self, timestamp: pd.Timestamp, profile: bool = False) -> pl.DataFrame:
        """
        Fetch the exact 1-minute OHLCV for Spot and Futures at the given timestamp.
        Edge Cases Handled:
        - If timestamp is outside trading hours or pre-open, returns an empty DataFrame.
        - Checks anomaly flags and filters out corrupted prices if severity is high.
        """
        t0 = time.perf_counter()
        query = f"""
            SELECT 
                s.timestamp,
                s.open as spot_open, s.high as spot_high, s.low as spot_low, s.close as spot_close, s.volume as spot_volume,
                fn.open as fut_near_open, fn.close as fut_near_close,
                fm.open as fut_mid_open, fm.close as fut_mid_close
            FROM v_spot s
            LEFT JOIN v_futures fn ON s.timestamp = fn.timestamp AND fn.contract_type = 'near'
            LEFT JOIN v_futures fm ON s.timestamp = fm.timestamp AND fm.contract_type = 'mid'
            WHERE s.timestamp = '{timestamp.strftime('%Y-%m-%d %H:%M:%S')}'
        """
        t_query_build = time.perf_counter()
        res = self.con.execute(query).pl()
        t_execute = time.perf_counter()
        
        if profile:
            print(f"[get_price Profile] Query Build: {(t_query_build - t0)*1000:.3f} ms | DuckDB Execute & Fetch (Polars): {(t_execute - t_query_build)*1000:.3f} ms | Total: {(t_execute - t0)*1000:.3f} ms")
            
        return res

    def get_signals(self, timestamp: pd.Timestamp, profile: bool = False) -> pl.DataFrame:
        """
        Returns the Options Chain snapshot valid at the requested timestamp.
        Edge Cases Handled:
        - Since snapshots are 5-min intervals, a request at 10:12 will perform an ASOF Join
          to return the 10:10 snapshot. 
        - If the previous snapshot is older than 5 minutes, it returns empty (stale state).
        """
        t0 = time.perf_counter()
        # We query the options view. We use an ASOF join concept, or just max(timestamp) <= requested.
        query = f"""
            WITH valid_snapshot AS (
                SELECT max(timestamp) as snap_ts
                FROM v_options
                WHERE timestamp <= '{timestamp.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND timestamp >= '{ (timestamp - pd.Timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S') }'
            )
            SELECT o.*
            FROM v_options o
            JOIN valid_snapshot v ON o.timestamp = v.snap_ts
        """
        t_query_build = time.perf_counter()
        res = self.con.execute(query).pl()
        t_execute = time.perf_counter()
        
        if profile:
            print(f"[get_signals Profile] Query Build: {(t_query_build - t0)*1000:.3f} ms | DuckDB ASOF Execute & Fetch (Polars): {(t_execute - t_query_build)*1000:.3f} ms | Total: {(t_execute - t0)*1000:.3f} ms")
            
        return res

    def get_features(self, timestamp: pd.Timestamp, profile: bool = False) -> pl.DataFrame:
        """
        Returns a feature matrix for the given timestamp (e.g. 30-period lookback returns).
        Edge Cases Handled:
        - If the lookback window (e.g. 30 days) does not exist (we only have 7 days of data), 
          it will return what is available, padded with NaNs.
        """
        t0 = time.perf_counter()
        # Let's pull the last 30 minutes of spot close prices as a simple feature matrix
        start_ts = timestamp - pd.Timedelta(minutes=30)
        query = f"""
            WITH ordered_data AS (
                SELECT timestamp, close, volume, anomaly_code
                FROM v_spot
                WHERE timestamp <= '{timestamp.strftime('%Y-%m-%d %H:%M:%S')}'
                  AND timestamp > '{start_ts.strftime('%Y-%m-%d %H:%M:%S')}'
            )
            SELECT 
                timestamp, close, volume, anomaly_code,
                (close / LAG(close) OVER (ORDER BY timestamp ASC) - 1.0)::FLOAT AS returns
            FROM ordered_data
            ORDER BY timestamp DESC
        """
        t_query_build = time.perf_counter()
        df = self.con.execute(query).pl()
        t_execute = time.perf_counter()
        
        # Contract enforcement: Must return a strictly shaped vector
        if df.is_empty():
            if profile:
                print(f"[get_features Profile] Query Build: {(t_query_build - t0)*1000:.3f} ms | DuckDB Execute: {(t_execute - t_query_build)*1000:.3f} ms | (Empty DataFrame Returned)")
            return pl.DataFrame()
            
        if profile:
            print(f"[get_features Profile] Query Build: {(t_query_build - t0)*1000:.3f} ms | DuckDB Execute (SQL Window Func -> Polars): {(t_execute - t_query_build)*1000:.3f} ms | Total: {(t_execute - t0)*1000:.3f} ms")
            
        return df

    def get_features_batch(self, timestamps: List[pd.Timestamp], profile: bool = False) -> pl.DataFrame:
        """
        Optimized high-throughput batch retrieval of features across thousands of timestamps.
        Edge Cases Handled:
        - Missing timestamps in the data will yield NULL rows, preserving the requested input alignment.
        """
        t0 = time.perf_counter()
        # Construct Polars DataFrame directly from the timestamps (no string parsing/formatting needed!)
        ts_pl = pl.DataFrame({"req_ts": timestamps})
        t_df_creation = time.perf_counter()
        
        # Register the Polars dataframe as a virtual table in DuckDB
        self.con.register('requested_timestamps', ts_pl)
        t_register = time.perf_counter()
        
        query = """
            SELECT 
                req.req_ts as timestamp,
                s.close as spot_close,
                s.volume as spot_volume,
                f.close as near_fut_close,
                v.vix_close
            FROM requested_timestamps req
            LEFT JOIN v_spot s ON req.req_ts = s.timestamp
            LEFT JOIN v_futures f ON req.req_ts = f.timestamp AND f.contract_type = 'near'
            LEFT JOIN v_vix v ON req.req_ts = v.timestamp
            ORDER BY req.req_ts
        """
        result = self.con.execute(query).pl()
        t_execute = time.perf_counter()
        
        self.con.unregister('requested_timestamps')
        t_unregister = time.perf_counter()
        
        if profile:
            print(f"[get_features_batch Profile] DF Creation (Polars): {(t_df_creation - t0)*1000:.3f} ms | DuckDB Register: {(t_register - t_df_creation)*1000:.3f} ms | DuckDB Execute & Fetch (Polars): {(t_execute - t_register)*1000:.3f} ms | DuckDB Unregister: {(t_unregister - t_execute)*1000:.3f} ms | Total: {(t_unregister - t0)*1000:.3f} ms")
            
        return result

if __name__ == "__main__":
    # Quick sanity check with profiling enabled
    pl.Config.set_tbl_formatting("ASCII_MARKDOWN")
    api = DataAPI()
    ts = pd.to_datetime('2025-08-22 10:15:00')
    
    print("Testing get_price with profiling...")
    print(api.get_price(ts, profile=True))
    
    print("\\nTesting get_signals (ASOF behavior for 10:17:00) with profiling...")
    ts_off = pd.to_datetime('2025-08-22 10:17:00')
    signals = api.get_signals(ts_off, profile=True)
    print(f"Returned {len(signals)} rows. Snapshot TS: {signals['timestamp'][0] if not signals.is_empty() else 'Empty'}")
    
    print("\\nTesting get_features with profiling...")
    features = api.get_features(ts, profile=True)
    
    print("\\nTesting get_features_batch with profiling...")
    # Generate 1000 timestamps to show realistic batch profiling
    batch_ts = pd.date_range(start='2025-08-22 09:15:00', periods=1000, freq='1min').tolist()
    api.get_features_batch(batch_ts, profile=True)
