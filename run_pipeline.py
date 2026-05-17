import pandas as pd
import polars as pl
import numpy as np
import os
import glob
# pyrefly: ignore [missing-import]
import duckdb
import shutil
import mlflow
import time
import hashlib
from datetime import datetime

from src.validation import validate_spot, validate_futures, validate_options, validate_triangulation, decode_anomalies, get_deleted_data

# Resolve paths relative to this file's location
# run_pipeline.py is in main_folder/
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "Data Engine Intern Project", "intern_data_db"))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "warehouse")
ANOMALY_WAREHOUSE_DIR = os.path.join(BASE_DIR, "anomaly_warehouse")
DELETED_WAREHOUSE_DIR = os.path.join(BASE_DIR, "deleted_values_warehouse")

# Ensure clean slate for idempotency
if os.path.exists(WAREHOUSE_DIR):
    shutil.rmtree(WAREHOUSE_DIR)
if os.path.exists(ANOMALY_WAREHOUSE_DIR):
    shutil.rmtree(ANOMALY_WAREHOUSE_DIR)
if os.path.exists(DELETED_WAREHOUSE_DIR):
    shutil.rmtree(DELETED_WAREHOUSE_DIR)

os.makedirs(WAREHOUSE_DIR, exist_ok=True)
os.makedirs(os.path.join(WAREHOUSE_DIR, "spot"), exist_ok=True)
os.makedirs(os.path.join(WAREHOUSE_DIR, "futures"), exist_ok=True)
os.makedirs(os.path.join(WAREHOUSE_DIR, "options"), exist_ok=True)
os.makedirs(os.path.join(WAREHOUSE_DIR, "vix"), exist_ok=True)
os.makedirs(os.path.join(WAREHOUSE_DIR, "fii_dii"), exist_ok=True)
os.makedirs(os.path.join(WAREHOUSE_DIR, "metadata"), exist_ok=True)
os.makedirs(os.path.join(WAREHOUSE_DIR, "validation"), exist_ok=True)
os.makedirs(os.path.join(WAREHOUSE_DIR, "duckdb"), exist_ok=True)

os.makedirs(ANOMALY_WAREHOUSE_DIR, exist_ok=True)
os.makedirs(os.path.join(ANOMALY_WAREHOUSE_DIR, "spot"), exist_ok=True)
os.makedirs(os.path.join(ANOMALY_WAREHOUSE_DIR, "futures"), exist_ok=True)
os.makedirs(os.path.join(ANOMALY_WAREHOUSE_DIR, "options"), exist_ok=True)

os.makedirs(DELETED_WAREHOUSE_DIR, exist_ok=True)
os.makedirs(os.path.join(DELETED_WAREHOUSE_DIR, "spot"), exist_ok=True)
os.makedirs(os.path.join(DELETED_WAREHOUSE_DIR, "futures"), exist_ok=True)

def hash_file(filepath):
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        hasher.update(f.read())
    return hasher.hexdigest()

def generate_anomaly_report(df, source_table):
    if 'anomaly_code' not in df.columns:
        return pd.DataFrame()
    anomalous = df[df['anomaly_code'] > 0].copy()
    if anomalous.empty:
        return pd.DataFrame()
    
    records = []
    for _, row in anomalous.iterrows():
        errors = decode_anomalies(row['anomaly_code'])
        for err in errors:
            records.append({
                'timestamp': row.get('timestamp', pd.NaT),
                'source_table': source_table,
                'anomaly_type': err,
                'anomaly_code': row['anomaly_code']
            })
    return pd.DataFrame(records)

def run_ingestion():
    mlflow_dir = os.path.join(BASE_DIR, "mlflow_data").replace("\\", "/")
    mlflow.set_tracking_uri(f"file:///{mlflow_dir}")
    mlflow.set_experiment("Data_Engine_Ingestion")
    with mlflow.start_run():
        start_time = time.time()
        print("Starting pipeline...")
        
        manifest_records = []

        # --- LOAD RAW DATA ---
        print("Loading Spot...")
        spot_files = glob.glob(os.path.join(DATA_DIR, "nifty_spot", "*.csv"))
        df_spot = pl.scan_csv(spot_files).collect().to_pandas() if spot_files else pd.DataFrame()
        for f in spot_files: manifest_records.append({'file': os.path.basename(f), 'sha256': hash_file(f)})

        print("Loading Futures...")
        fut_files = glob.glob(os.path.join(DATA_DIR, "nifty_futures", "*.csv"))
        df_fut = pl.scan_csv(fut_files).collect().to_pandas() if fut_files else pd.DataFrame()
        for f in fut_files: manifest_records.append({'file': os.path.basename(f), 'sha256': hash_file(f)})

        print("Loading Options...")
        opt_files = glob.glob(os.path.join(DATA_DIR, "options_chain", "*.csv"))
        opt_dfs = []
        for f in opt_files:
            # Polars native fast loading
            df = pl.read_csv(f)
            fname = os.path.basename(f).replace('.csv', '')
            trade_dt_str, expiry_str = fname.split('_')
            df = df.with_columns(pl.lit(expiry_str).alias('expiry'))
            opt_dfs.append(df)
            manifest_records.append({'file': os.path.basename(f), 'sha256': hash_file(f)})
        df_opt = pl.concat(opt_dfs).to_pandas() if opt_dfs else pd.DataFrame()
        df_opt['expiry'] = pd.to_datetime(df_opt['expiry'])

        print("Loading Aux...")
        vix_path = os.path.join(DATA_DIR, "aux", "india_vix.csv")
        fii_path = os.path.join(DATA_DIR, "aux", "fii_dii_flow.csv")
        df_vix = pl.read_csv(vix_path).to_pandas()
        df_fii = pl.read_csv(fii_path).to_pandas()
        manifest_records.append({'file': os.path.basename(vix_path), 'sha256': hash_file(vix_path)})
        manifest_records.append({'file': os.path.basename(fii_path), 'sha256': hash_file(fii_path)})

        # Convert timestamps
        for df in [df_spot, df_fut, df_opt, df_vix]:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        # --- VALIDATION ---
        print("Running Validation...")
        df_spot = validate_spot(df_spot)
        df_fut = validate_futures(df_fut)
        df_opt = validate_options(df_opt)
        df_spot, df_fut, df_opt = validate_triangulation(df_spot, df_fut, df_opt, df_vix)

        # Collect deleted (original pre-imputed) rows
        df_deleted_sp, df_deleted_ft = get_deleted_data()

        # Generate anomaly reports
        rep_spot = generate_anomaly_report(df_spot, "spot")
        rep_fut = generate_anomaly_report(df_fut, "futures")
        rep_opt = generate_anomaly_report(df_opt, "options")
        anomaly_report = pd.concat([rep_spot, rep_fut, rep_opt], ignore_index=True)

        mlflow.log_metric("anomalies_spot", len(rep_spot))
        mlflow.log_metric("anomalies_fut", len(rep_fut))
        mlflow.log_metric("anomalies_opt", len(rep_opt))

        # Save validation report
        # Open DuckDB connection for writing & logical views
        db_path = os.path.join(WAREHOUSE_DIR, "duckdb", "market_data.duckdb").replace("\\", "/")
        con = duckdb.connect(db_path)

        # 0. ANOMALY REPORT
        val_path = os.path.join(WAREHOUSE_DIR, "validation", "anomaly_report.parquet").replace("\\", "/")
        if anomaly_report.empty:
            anomaly_report = pd.DataFrame(columns=['timestamp', 'source_table', 'anomaly_type', 'anomaly_code'])
        con.register('anomaly_report', anomaly_report)
        con.execute(f"COPY anomaly_report TO '{val_path}' (FORMAT PARQUET, COMPRESSION zstd, PARQUET_VERSION 'V2')")

        # --- PARTITION & WRITE TO WAREHOUSE ---
        print("Writing to warehouse...")

        # 1. SPOT — FLOAT prices (8→4 bytes each), UINT32 volume
        df_spot['trade_date'] = df_spot['timestamp'].dt.strftime('%Y-%m-%d')
        df_spot = df_spot.sort_values('timestamp')
        con.register('df_spot', df_spot)
        spot_path = os.path.join(WAREHOUSE_DIR, "spot").replace("\\", "/")
        con.execute(f"""
            COPY (
                SELECT
                    timestamp,
                    CAST(open  AS FLOAT) AS open,
                    CAST(high  AS FLOAT) AS high,
                    CAST(low   AS FLOAT) AS low,
                    CAST(close AS FLOAT) AS close,
                    CAST(volume AS UINT32) AS volume,
                    anomaly_code,
                    trade_date
                FROM df_spot
            ) TO '{spot_path}' (FORMAT PARQUET, PARTITION_BY (trade_date), COMPRESSION zstd, PARQUET_VERSION 'V2', ROW_GROUP_SIZE 10000)
        """)

        # 1b. SPOT ANOMALIES
        if (df_spot['anomaly_code'] > 0).any():
            spot_anom_path = os.path.join(ANOMALY_WAREHOUSE_DIR, "spot").replace("\\", "/")
            con.execute(f"""
                COPY (
                    SELECT
                        timestamp,
                        CAST(open  AS FLOAT) AS open,
                        CAST(high  AS FLOAT) AS high,
                        CAST(low   AS FLOAT) AS low,
                        CAST(close AS FLOAT) AS close,
                        CAST(volume AS UINT32) AS volume,
                        anomaly_code,
                        trade_date
                    FROM df_spot
                    WHERE anomaly_code > 0
                ) TO '{spot_anom_path}' (FORMAT PARQUET, PARTITION_BY (trade_date), COMPRESSION zstd, PARQUET_VERSION 'V2', ROW_GROUP_SIZE 10000)
            """)

        # 2. FUTURES — FLOAT prices, DATE expiry (string 14→4 bytes), ENUM contract_type
        common_cols = ['timestamp', 'anomaly_code']
        near_cols = [c for c in df_fut.columns if c.startswith('near_month_')]
        mid_cols  = [c for c in df_fut.columns if c.startswith('mid_month_')]

        df_near = df_fut[common_cols + near_cols].copy()
        df_near.rename(columns=lambda x: x.replace('near_month_', ''), inplace=True)
        df_near['contract_type'] = 'near'

        df_mid = df_fut[common_cols + mid_cols].copy()
        df_mid.rename(columns=lambda x: x.replace('mid_month_', ''), inplace=True)
        df_mid['contract_type'] = 'mid'

        df_fut_m = pd.concat([df_near, df_mid], ignore_index=True)
        df_fut_m['trade_date'] = df_fut_m['timestamp'].dt.strftime('%Y-%m-%d')
        df_fut_m = df_fut_m.sort_values(['timestamp'])
        con.register('df_fut_m', df_fut_m)
        futures_path = os.path.join(WAREHOUSE_DIR, "futures").replace("\\", "/")
        con.execute(f"""
            COPY (
                SELECT
                    timestamp,
                    anomaly_code,
                    CAST(expiry AS DATE)  AS expiry,
                    CAST(open  AS FLOAT)  AS open,
                    CAST(high  AS FLOAT)  AS high,
                    CAST(low   AS FLOAT)  AS low,
                    CAST(close AS FLOAT)  AS close,
                    CAST(volume AS UINT32) AS volume,
                    CAST(contract_type AS ENUM('near', 'mid')) AS contract_type,
                    trade_date
                FROM df_fut_m
            ) TO '{futures_path}' (FORMAT PARQUET, PARTITION_BY (contract_type, trade_date), COMPRESSION zstd, PARQUET_VERSION 'V2', ROW_GROUP_SIZE 10000)
        """)

        # 2b. FUTURES ANOMALIES
        if (df_fut_m['anomaly_code'] > 0).any():
            futures_anom_path = os.path.join(ANOMALY_WAREHOUSE_DIR, "futures").replace("\\", "/")
            con.execute(f"""
                COPY (
                    SELECT
                        timestamp,
                        anomaly_code,
                        CAST(expiry AS DATE)  AS expiry,
                        CAST(open  AS FLOAT)  AS open,
                        CAST(high  AS FLOAT)  AS high,
                        CAST(low   AS FLOAT)  AS low,
                        CAST(close AS FLOAT)  AS close,
                        CAST(volume AS UINT32) AS volume,
                        CAST(contract_type AS ENUM('near', 'mid')) AS contract_type,
                        trade_date
                    FROM df_fut_m
                    WHERE anomaly_code > 0
                ) TO '{futures_anom_path}' (FORMAT PARQUET, PARTITION_BY (contract_type, trade_date), COMPRESSION zstd, PARQUET_VERSION 'V2', ROW_GROUP_SIZE 10000)
            """)

        # 3. OPTIONS — ENUM side, DATE expiry (ts[ns]→4 bytes), FLOAT prices & IV
        #    Sort: (side, timestamp, strike) — groups all CE rows then all PE rows.
        #    This maximises RLE on the 2-value side column within each row group and
        #    keeps timestamp monotone within each side block for DELTA_BINARY_PACKED.
        #
        #    NOTE: CAST(col AS ENUM) in a subquery is dropped by DuckDB during COPY.
        #    We must materialise a typed table first so the ENUM type is schema-level.
        df_opt['trade_date'] = df_opt['timestamp'].dt.strftime('%Y-%m-%d')
        df_opt['expiry_str'] = df_opt['expiry'].dt.strftime('%Y-%m-%d')
        df_opt = df_opt.sort_values(['side', 'timestamp', 'strike'])
        df_opt['snapshot_id'] = df_opt.groupby(['trade_date', 'expiry_str', 'timestamp']).ngroup()

        con.register('df_opt_raw', df_opt)
        con.execute("""
            CREATE OR REPLACE TABLE tbl_opt AS
            SELECT
                timestamp,
                CAST(strike AS UINT32)          AS strike,
                CAST(side   AS ENUM('CE','PE')) AS side,
                CAST(open   AS FLOAT)           AS open,
                CAST(high   AS FLOAT)           AS high,
                CAST(low    AS FLOAT)           AS low,
                CAST(close  AS FLOAT)           AS close,
                CAST(volume AS UINT32)          AS volume,
                CAST(oi     AS UINT32)          AS oi,
                CAST(iv     AS FLOAT)           AS iv,
                CAST(expiry AS DATE)            AS expiry,
                anomaly_code,
                CAST(snapshot_id AS UINT32)     AS snapshot_id,
                trade_date,
                expiry_str
            FROM df_opt_raw
        """)
        options_path = os.path.join(WAREHOUSE_DIR, "options").replace("\\", "/")
        con.execute(f"""
            COPY tbl_opt
            TO '{options_path}' (FORMAT PARQUET, PARTITION_BY (trade_date, expiry_str),
                                 COMPRESSION zstd, PARQUET_VERSION 'V2', ROW_GROUP_SIZE 2048)
        """)
        con.execute("DROP TABLE tbl_opt")

        # 3b. OPTIONS ANOMALIES
        if (df_opt['anomaly_code'] > 0).any():
            con.execute("""
                CREATE OR REPLACE TABLE tbl_opt_anom AS
                SELECT
                    timestamp,
                    CAST(strike AS UINT32)          AS strike,
                    CAST(side   AS ENUM('CE','PE')) AS side,
                    CAST(open   AS FLOAT)           AS open,
                    CAST(high   AS FLOAT)           AS high,
                    CAST(low    AS FLOAT)           AS low,
                    CAST(close  AS FLOAT)           AS close,
                    CAST(volume AS UINT32)          AS volume,
                    CAST(oi     AS UINT32)          AS oi,
                    CAST(iv     AS FLOAT)           AS iv,
                    CAST(expiry AS DATE)            AS expiry,
                    anomaly_code,
                    CAST(snapshot_id AS UINT32)     AS snapshot_id,
                    trade_date,
                    expiry_str
                FROM df_opt_raw
                WHERE anomaly_code > 0
            """)
            options_anom_path = os.path.join(ANOMALY_WAREHOUSE_DIR, "options").replace("\\", "/")
            con.execute(f"""
                COPY tbl_opt_anom
                TO '{options_anom_path}' (FORMAT PARQUET, PARTITION_BY (trade_date, expiry_str),
                                          COMPRESSION zstd, PARQUET_VERSION 'V2', ROW_GROUP_SIZE 2048)
            """)
            con.execute("DROP TABLE tbl_opt_anom")

        # 4. VIX — FLOAT vix_close (ratio, 2 decimal places; FLOAT precision is ample)
        df_vix['trade_date'] = df_vix['timestamp'].dt.strftime('%Y-%m-%d')
        df_vix = df_vix.sort_values('timestamp')
        con.register('df_vix', df_vix)
        vix_path = os.path.join(WAREHOUSE_DIR, "vix").replace("\\", "/")
        con.execute(f"""
            COPY (
                SELECT
                    timestamp,
                    CAST(vix_close AS FLOAT) AS vix_close,
                    trade_date
                FROM df_vix
            ) TO '{vix_path}' (FORMAT PARQUET, PARTITION_BY (trade_date), COMPRESSION zstd, PARQUET_VERSION 'V2', ROW_GROUP_SIZE 10000)
        """)

        # 5. FII/DII — DATE, FLOAT flows, ENUM trade_date (forced dict-encoded)
        df_fii['trade_date'] = pd.to_datetime(df_fii['date']).dt.strftime('%Y-%m-%d')
        known_dates = "','".join(sorted(df_fii['trade_date'].unique()))
        con.register('df_fii_raw', df_fii)
        con.execute(f"""
            CREATE OR REPLACE TABLE tbl_fii AS
            SELECT
                CAST(date AS DATE)                        AS date,
                CAST(fii_net AS FLOAT)                   AS fii_net,
                CAST(dii_net AS FLOAT)                   AS dii_net,
                CAST(trade_date AS ENUM('{known_dates}')) AS trade_date
            FROM df_fii_raw
        """)
        fii_path = os.path.join(WAREHOUSE_DIR, "fii_dii", "fii_dii.parquet").replace("\\", "/")
        con.execute(f"COPY tbl_fii TO '{fii_path}' (FORMAT PARQUET, COMPRESSION zstd, PARQUET_VERSION 'V2')")
        con.execute("DROP TABLE tbl_fii")

        # --- DELETED VALUES WAREHOUSE ---
        if not df_deleted_sp.empty:
            df_deleted_sp['trade_date'] = df_deleted_sp['timestamp'].dt.strftime('%Y-%m-%d')
            con.register('df_deleted_sp', df_deleted_sp)
            del_spot_path = os.path.join(DELETED_WAREHOUSE_DIR, "spot").replace("\\", "/")
            con.execute(f"""
                COPY (
                    SELECT
                        timestamp,
                        deleted_reason,
                        CAST(open  AS FLOAT) AS open,
                        CAST(high  AS FLOAT) AS high,
                        CAST(low   AS FLOAT) AS low,
                        CAST(close AS FLOAT) AS close,
                        CAST(volume AS UINT32) AS volume,
                        anomaly_code,
                        trade_date
                    FROM df_deleted_sp
                ) TO '{del_spot_path}' (FORMAT PARQUET, PARTITION_BY (trade_date), COMPRESSION zstd, PARQUET_VERSION 'V2')
            """)

        if not df_deleted_ft.empty:
            df_deleted_ft['trade_date'] = df_deleted_ft['timestamp'].dt.strftime('%Y-%m-%d')
            
            # Melt futures similar to main warehouse structure
            df_near_del = df_deleted_ft[common_cols + near_cols + ['deleted_reason', 'trade_date']].copy()
            df_near_del.rename(columns=lambda x: x.replace('near_month_', ''), inplace=True)
            df_near_del['contract_type'] = 'near'

            df_mid_del = df_deleted_ft[common_cols + mid_cols + ['deleted_reason', 'trade_date']].copy()
            df_mid_del.rename(columns=lambda x: x.replace('mid_month_', ''), inplace=True)
            df_mid_del['contract_type'] = 'mid'

            df_fut_del = pd.concat([df_near_del, df_mid_del], ignore_index=True)
            df_fut_del = df_fut_del.sort_values(['timestamp'])
            
            con.register('df_fut_del', df_fut_del)
            del_fut_path = os.path.join(DELETED_WAREHOUSE_DIR, "futures").replace("\\", "/")
            con.execute(f"""
                COPY (
                    SELECT
                        timestamp,
                        deleted_reason,
                        anomaly_code,
                        CAST(expiry AS DATE)  AS expiry,
                        CAST(open  AS FLOAT)  AS open,
                        CAST(high  AS FLOAT)  AS high,
                        CAST(low   AS FLOAT)  AS low,
                        CAST(close AS FLOAT)  AS close,
                        CAST(volume AS UINT32) AS volume,
                        CAST(contract_type AS ENUM('near', 'mid')) AS contract_type,
                        trade_date
                    FROM df_fut_del
                ) TO '{del_fut_path}' (FORMAT PARQUET, PARTITION_BY (contract_type, trade_date), COMPRESSION zstd, PARQUET_VERSION 'V2')
            """)

        # --- METADATA ---
        manifest_df = pd.DataFrame(manifest_records)
        con.register('manifest_df', manifest_df)
        manifest_path = os.path.join(WAREHOUSE_DIR, "metadata", "ingestion_manifest.parquet").replace("\\", "/")
        con.execute(f"COPY manifest_df TO '{manifest_path}' (FORMAT PARQUET, COMPRESSION zstd, PARQUET_VERSION 'V2')")

        # --- DUCKDB LOGICAL VIEWS ---
        print("Creating DuckDB views...")
        # Use absolute path for parquet files so views work from any CWD
        abs_warehouse = os.path.abspath(WAREHOUSE_DIR).replace("\\", "/")
        
        con.execute(f"CREATE OR REPLACE VIEW v_spot AS SELECT * FROM read_parquet('{abs_warehouse}/spot/*/*.parquet', hive_partitioning=true);")
        con.execute(f"CREATE OR REPLACE VIEW v_futures AS SELECT * FROM read_parquet('{abs_warehouse}/futures/*/*/*.parquet', hive_partitioning=true);")
        con.execute(f"CREATE OR REPLACE VIEW v_options AS SELECT * FROM read_parquet('{abs_warehouse}/options/*/*/*.parquet', hive_partitioning=true);")
        con.execute(f"CREATE OR REPLACE VIEW v_vix AS SELECT * FROM read_parquet('{abs_warehouse}/vix/*/*.parquet', hive_partitioning=true);")
        con.execute(f"CREATE OR REPLACE VIEW v_fii_dii AS SELECT * FROM read_parquet('{abs_warehouse}/fii_dii/*.parquet');")
        con.close()

        elapsed = time.time() - start_time
        mlflow.log_metric("ingestion_time_seconds", elapsed)
        print(f"Pipeline completed in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    run_ingestion()
