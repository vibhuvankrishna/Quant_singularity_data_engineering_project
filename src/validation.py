import pandas as pd
import numpy as np

# Bitmask Error Dictionary
# Using powers of 2 (1, 2, 4, 8, 16...) allows us to combine multiple errors cleanly.
# Example: If a row has a duplicate timestamp (2) and an OHLC violation (8), its anomaly_code = 10.
ERROR_DICTIONARY = {
    1: "Backward time jump detected",
    2: "Duplicate timestamp detected",
    4: "Missing expected 1-minute candle (Row inserted to fill gap)",
    8: "OHLC integrity violation (e.g., Low > High)",
    16: "Price spike > 1% in a single candle (Imputed with 3-period MA)",
    32: "Abnormal futures spread (>5% between near and mid month)",
    64: "Stale price (No price change for 5+ mins with volume > 0)",
    # 128: "Invalid Option Strike or Side",
    256: "Volatility Triangulation: Isolated Spot Anomaly",
    512: "Volatility Triangulation: Isolated Futures Anomaly",
    1024: "Volatility Triangulation: Isolated Options Anomaly",
    2048: "Delta Triangulation: Isolated Spot Spike Anomaly",
    4096: "Delta Triangulation: Isolated Futures Spike Anomaly",
    8192: "Delta Triangulation: Isolated Options Spike Anomaly",
    16384: "Isolated peak/dip price spike (Misplaced value deleted and forward filled)"
}

DELETED_SPOT = []
DELETED_FUTURES = []

def get_deleted_data():
    global DELETED_SPOT, DELETED_FUTURES
    df_sp = pd.concat(DELETED_SPOT, ignore_index=True) if DELETED_SPOT else pd.DataFrame()
    df_ft = pd.concat(DELETED_FUTURES, ignore_index=True) if DELETED_FUTURES else pd.DataFrame()
    # Reset globals for the next run
    DELETED_SPOT = []
    DELETED_FUTURES = []
    return df_sp, df_ft

def decode_anomalies(anomaly_code):
    """
    Helper function to decode an integer anomaly_code back into a list of human-readable errors.
    Example: decode_anomalies(10) -> ['Duplicate timestamp detected', 'OHLC integrity violation']
    """
    if pd.isna(anomaly_code) or anomaly_code == 0:
        return []
    
    errors = []
    for bit_value, description in ERROR_DICTIONARY.items():
        # Bitwise AND to check if the specific bit is set
        if anomaly_code & bit_value:
            errors.append(description)
    return errors


def validate_spot(df):
    """
    Validates Spot data and returns dataframe with 'anomaly_code' integer column.
    """
    df = df.copy()
    
    # Initialize the anomaly code column to 0 (no errors)
    # We use uint32 to allow for up to 32 different error flags
    df['anomaly_code'] = np.uint32(0)
    
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
    # Check: Backward Time Jump (Bit 1)
    time_diff = df['timestamp'].diff()
    backward_jumps = time_diff < pd.Timedelta(0)
    df.loc[backward_jumps, 'anomaly_code'] |= 1
    
    # Check: Duplicate Timestamps (Bit 2)
    duplicates = df['timestamp'].duplicated(keep=False)
    df.loc[duplicates, 'anomaly_code'] |= 2
    
    # Check: Missing expected 1-minute candle (Bit 4)
    # Group by date to only check within the same trading session
    df['date'] = df['timestamp'].dt.date
    time_diff_gap = df.groupby('date')['timestamp'].diff()
    missing_gaps = time_diff_gap > pd.Timedelta(minutes=1)
    df.loc[missing_gaps, 'anomaly_code'] |= 4
    df.drop(columns=['date'], inplace=True)
    
    # Check: OHLC Integrity (Bit 8)
    ohlc_violation = (df['low'] > df['high']) | \
                     (df['close'] > df['high']) | \
                     (df['close'] < df['low']) | \
                     (df['open'] > df['high']) | \
                     (df['open'] < df['low'])
    df.loc[ohlc_violation, 'anomaly_code'] |= 8
    
    # Check: Misplaced Value (Bit 16384)
    # Price jumps >= 1% and immediately drops/reverts >= 1% in 1 minute
    r1 = df['close'].pct_change()
    r2 = (df['close'].shift(-1) - df['close']) / df['close']
    misplaced_mask = ((r1 > 0.01) & (r2 < -0.01)) | ((r1 < -0.01) & (r2 > 0.01))
    
    if misplaced_mask.any():
        df_deleted = df[misplaced_mask].copy()
        df_deleted['deleted_reason'] = "Misplaced Value (Bit 16384)"
        # Keep track of original values before we impute them
        DELETED_SPOT.append(df_deleted)
        df.loc[misplaced_mask, 'anomaly_code'] |= 16384
        
        # Forward fill spiked rows with previous row's value (shift(1))
        for col in ['open', 'high', 'low', 'close']:
            df.loc[misplaced_mask, col] = df[col].shift(1)[misplaced_mask]

    # Check: Price Spike > 1% (Bit 16)
    # Avoid division by zero by replacing 0s temporarily if any exist
    low_safe = df['low'].replace(0, np.nan)
    intrabar_spike = ((df['high'] - df['low']) / low_safe) > 0.01
    return_spike = df['close'].pct_change().abs() > 0.01
    spike_mask = intrabar_spike | return_spike
    
    if spike_mask.any():
        df_deleted_spike = df[spike_mask].copy()
        df_deleted_spike['deleted_reason'] = "Price Spike 3-period MA (Bit 16)"
        DELETED_SPOT.append(df_deleted_spike)
        df.loc[spike_mask, 'anomaly_code'] |= 16
        
        # Impute spiked values with 3-period moving average (current + last 2 rows)
        for col in ['open', 'high', 'low', 'close']:
            rolling_mean = df[col].rolling(window=3, min_periods=1).mean()
            df.loc[spike_mask, col] = rolling_mean[spike_mask]
    
    return df


def validate_futures(df):
    """
    Validates Futures data and returns dataframe with 'anomaly_code' integer column.
    """
    df = df.copy()
    df['anomaly_code'] = np.uint32(0)
    
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
    # Backward Time Jump (Bit 1)
    backward_jumps = df['timestamp'].diff() < pd.Timedelta(0)
    df.loc[backward_jumps, 'anomaly_code'] |= 1
    
    # Duplicate Timestamps (Bit 2)
    duplicates = df['timestamp'].duplicated(keep=False)
    df.loc[duplicates, 'anomaly_code'] |= 2
    
    # Missing expected 1-minute candle (Bit 4)
    df['date'] = df['timestamp'].dt.date
    time_diff_gap = df.groupby('date')['timestamp'].diff()
    missing_gaps = time_diff_gap > pd.Timedelta(minutes=1)
    df.loc[missing_gaps, 'anomaly_code'] |= 4
    df.drop(columns=['date'], inplace=True)
    
    # OHLC Integrity (Bit 8)
    ohlc_near = (df['near_month_low'] > df['near_month_high']) | \
                (df['near_month_close'] > df['near_month_high']) | \
                (df['near_month_close'] < df['near_month_low'])
    ohlc_mid = (df['mid_month_low'] > df['mid_month_high']) | \
               (df['mid_month_close'] > df['mid_month_high']) | \
               (df['mid_month_close'] < df['mid_month_low'])
    df.loc[ohlc_near | ohlc_mid, 'anomaly_code'] |= 8
    
    # Spread Anomaly > 5% (Bit 32)
    spread_pct = (df['near_month_close'] - df['mid_month_close']).abs() / df['near_month_close'].replace(0, np.nan)
    abnormal_spread = spread_pct > 0.05
    df.loc[abnormal_spread, 'anomaly_code'] |= 32
    
    # Stale Price (Bit 64)
    df['date'] = df['timestamp'].dt.date
    price_change = df.groupby('date')['near_month_close'].diff()
    df['is_stale'] = (price_change == 0) & (df['near_month_volume'] > 0)
    stale_runs = df.groupby('date')['is_stale'].rolling(window=5).sum().reset_index(0, drop=True)
    df.loc[stale_runs >= 5, 'anomaly_code'] |= 64
    df.drop(columns=['date', 'is_stale'], inplace=True)
    
    # Check: Misplaced Value (Bit 16384)
    r1 = df['near_month_close'].pct_change()
    r2 = (df['near_month_close'].shift(-1) - df['near_month_close']) / df['near_month_close']
    misplaced_mask = ((r1 > 0.01) & (r2 < -0.01)) | ((r1 < -0.01) & (r2 > 0.01))
    
    cols_to_impute = ['near_month_open', 'near_month_high', 'near_month_low', 'near_month_close',
                      'mid_month_open', 'mid_month_high', 'mid_month_low', 'mid_month_close']
                      
    if misplaced_mask.any():
        df_deleted = df[misplaced_mask].copy()
        df_deleted['deleted_reason'] = "Misplaced Value (Bit 16384)"
        DELETED_FUTURES.append(df_deleted)
        df.loc[misplaced_mask, 'anomaly_code'] |= 16384
        
        # Forward fill spiked rows with previous row's value (shift(1))
        for col in cols_to_impute:
            df.loc[misplaced_mask, col] = df[col].shift(1)[misplaced_mask]

    # Check: Price Spike > 1% (Bit 16)
    low_safe = df['near_month_low'].replace(0, np.nan)
    intrabar_spike = ((df['near_month_high'] - df['near_month_low']) / low_safe) > 0.01
    return_spike = df['near_month_close'].pct_change().abs() > 0.01
    spike_mask = intrabar_spike | return_spike
    
    if spike_mask.any():
        df_deleted_spike = df[spike_mask].copy()
        df_deleted_spike['deleted_reason'] = "Price Spike 3-period MA (Bit 16)"
        DELETED_FUTURES.append(df_deleted_spike)
        df.loc[spike_mask, 'anomaly_code'] |= 16
        
        # Impute spiked values with 3-period moving average (current + last 2 rows)
        for col in cols_to_impute:
            rolling_mean = df[col].rolling(window=3, min_periods=1).mean()
            df.loc[spike_mask, col] = rolling_mean[spike_mask]
    
    return df


def validate_options(df):
    """
    Validates Options data and returns dataframe with 'anomaly_code' integer column.
    """
    df = df.copy()
    df['anomaly_code'] = np.uint32(0)
    
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
    # Note: Duplicates NOT checked here since an option chain inherently has multiple rows (strikes/sides) per timestamp
    
    # Backward Time Jump (Bit 1)
    # We must group by strike and side to check for backward time jumps accurately
    df = df.sort_values(by=['strike', 'side', 'timestamp'])
    time_diff = df.groupby(['strike', 'side'])['timestamp'].diff()
    backward_jumps = time_diff < pd.Timedelta(0)
    df.loc[backward_jumps, 'anomaly_code'] |= 1
    
    # OHLC Integrity (Bit 8)
    ohlc_violation = (df['low'] > df['high']) | \
                     (df['close'] > df['high']) | \
                     (df['close'] < df['low'])
    df.loc[ohlc_violation, 'anomaly_code'] |= 8
    
    # Invalid Strike or Side (Bit 128)
    # invalid_strike = df['strike'] <= 0
    # invalid_side = ~df['side'].isin(['CE', 'PE'])
    # df.loc[invalid_strike | invalid_side, 'anomaly_code'] |= 128
    
    # Incomplete CE/PE pairs (Bit 128)
    # chain_counts = df.groupby(['timestamp', 'strike'])['side'].transform('size')
    # incomplete_chains = chain_counts != 2
    # df.loc[incomplete_chains, 'anomaly_code'] |= 128
    
    # Sort back by original timestamp to maintain natural order
    df = df.sort_values(by=['timestamp', 'strike', 'side']).reset_index(drop=True)
    
    return df


def validate_triangulation(df_spot, df_fut, df_opt, df_vix):
    """
    Performs Volatility-Aware Triangular Validation across Spot, Futures, and Options.
    This identifies cross-market pricing anomalies.
    Returns updated df_spot, df_fut, df_opt with triangulation anomaly codes.
    """
    df_spot = df_spot.copy()
    df_fut = df_fut.copy()
    df_opt = df_opt.copy()

    if 'anomaly_code' not in df_spot.columns: df_spot['anomaly_code'] = np.uint32(0)
    if 'anomaly_code' not in df_fut.columns: df_fut['anomaly_code'] = np.uint32(0)
    if 'anomaly_code' not in df_opt.columns: df_opt['anomaly_code'] = np.uint32(0)

    # Prepare Unison Data (Merging Spot, Fut, Opt, and VIX)
    df_ce = df_opt[df_opt['side'] == 'CE'][['timestamp', 'strike', 'close']]
    df_pe = df_opt[df_opt['side'] == 'PE'][['timestamp', 'strike', 'close']]
    df_options_pairs = pd.merge(df_ce, df_pe, on=['timestamp', 'strike'], suffixes=('_ce', '_pe'))

    df_unison = pd.merge(df_spot[['timestamp', 'close']], 
                         df_fut[['timestamp', 'near_month_expiry', 'near_month_close']], 
                         on='timestamp')
    df_unison = pd.merge(df_unison, df_options_pairs, on='timestamp')
    df_unison = pd.merge(df_unison, df_vix[['timestamp', 'vix_close']], on='timestamp')

    if df_unison.empty:
        return df_spot, df_fut, df_opt

    # --- MODEL 1: VOLATILITY-AWARE TRIANGULATION (Cost-of-Carry / Put-Call Parity) ---
    # Calculate Time to expiry (t) in years
    df_unison['expiry_dt'] = pd.to_datetime(df_unison['near_month_expiry']) + pd.Timedelta(hours=15, minutes=30)
    df_unison['t'] = (df_unison['expiry_dt'] - df_unison['timestamp']).dt.total_seconds() / (365 * 24 * 3600)

    RISK_FREE_RATE, DIVIDEND_YIELD = 0.065, 0.012

    # Error Vectors
    df_unison['E_SF'] = df_unison['near_month_close'] - df_unison['close'] * np.exp((RISK_FREE_RATE - DIVIDEND_YIELD) * df_unison['t'])
    df_unison['E_FO'] = (df_unison['close_ce'] - df_unison['close_pe']) - (df_unison['near_month_close'] - df_unison['strike']) * np.exp(-RISK_FREE_RATE * df_unison['t'])
    df_unison['E_SO'] = (df_unison['close_ce'] - df_unison['close_pe']) - (df_unison['close'] * np.exp((RISK_FREE_RATE - DIVIDEND_YIELD) * df_unison['t']) - df_unison['strike']) * np.exp(-RISK_FREE_RATE * df_unison['t'])

    # VOLATILITY-AWARE DYNAMIC THRESHOLDS
    BASE_THRESH_SF = 20
    BASE_THRESH_OPT = 50
    df_unison['T_SF'] = BASE_THRESH_SF * (df_unison['vix_close'] / 15.0)
    df_unison['T_OPT'] = BASE_THRESH_OPT * (df_unison['vix_close'] / 15.0)

    # Volatility Triangulation Logic
    spot_bad_vol = (df_unison['E_FO'].abs() <= df_unison['T_OPT']) & (df_unison['E_SF'].abs() > df_unison['T_SF']) & (df_unison['E_SO'].abs() > df_unison['T_OPT'])
    fut_bad_vol = (df_unison['E_SO'].abs() <= df_unison['T_OPT']) & (df_unison['E_SF'].abs() > df_unison['T_SF']) & (df_unison['E_FO'].abs() > df_unison['T_OPT'])

    bad_spot_ts_vol = df_unison[spot_bad_vol]['timestamp'].unique()
    bad_fut_ts_vol = df_unison[fut_bad_vol]['timestamp'].unique()

    df_spot.loc[df_spot['timestamp'].isin(bad_spot_ts_vol), 'anomaly_code'] |= 256
    df_fut.loc[df_fut['timestamp'].isin(bad_fut_ts_vol), 'anomaly_code'] |= 512

    # --- MODEL 2: DELTA-BASED TRIANGULATION (Spike Validation) ---
    df_spot_diff = df_spot[['timestamp', 'close']].copy().sort_values('timestamp')
    df_spot_diff['delta_S'] = df_spot_diff['close'].diff()

    df_fut_diff = df_fut[['timestamp', 'near_month_close']].copy().sort_values('timestamp')
    df_fut_diff['delta_F'] = df_fut_diff['near_month_close'].diff()

    df_opt_pairs_delta = df_options_pairs.copy().sort_values(['strike', 'timestamp'])
    df_opt_pairs_delta['syn_fut'] = df_opt_pairs_delta['close_ce'] - df_opt_pairs_delta['close_pe']
    df_opt_pairs_delta['delta_O_strike'] = df_opt_pairs_delta.groupby('strike')['syn_fut'].diff()
    df_opt_diff = df_opt_pairs_delta.groupby('timestamp')['delta_O_strike'].median().reset_index()
    df_opt_diff.rename(columns={'delta_O_strike': 'delta_O'}, inplace=True)

    df_tri = pd.merge(df_spot_diff, df_fut_diff, on='timestamp')
    df_tri = pd.merge(df_tri, df_opt_diff, on='timestamp')

    SPIKE_THRESHOLD = 50
    TOLERANCE = 15

    # Exactly matching the notebook: Only check when Spot delta > SPIKE_THRESHOLD
    # Check 2: Futures and Options agree with each other, but disagree with Spot
    spot_bad_delta = (df_tri['delta_S'].abs() > SPIKE_THRESHOLD) & \
                     (abs(df_tri['delta_F'] - df_tri['delta_O']) <= TOLERANCE) & \
                     (abs(df_tri['delta_S'] - df_tri['delta_F']) > TOLERANCE) & \
                     (abs(df_tri['delta_S'] - df_tri['delta_O']) > TOLERANCE)

    bad_spot_ts_delta = df_tri[spot_bad_delta]['timestamp'].unique()

    df_spot.loc[df_spot['timestamp'].isin(bad_spot_ts_delta), 'anomaly_code'] |= 2048

    return df_spot, df_fut, df_opt
