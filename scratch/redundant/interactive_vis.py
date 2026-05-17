import pandas as pd
import glob
import os
from lightweight_charts import Chart

def visualize_interactive_market():
    # 1. Paths
    DATA_DIR = "../Data Engine Intern Project/intern_data_db"
    SPOT_DIR = os.path.join(DATA_DIR, "nifty_spot")
    FUTURES_DIR = os.path.join(DATA_DIR, "nifty_futures")

    # 2. Load Spot Data
    spot_files = sorted(glob.glob(os.path.join(SPOT_DIR, "*.csv")))
    df_spot = pd.concat([pd.read_csv(f) for f in spot_files])
    df_spot['time'] = pd.to_datetime(df_spot['timestamp'])
    df_spot = df_spot.sort_values('time').reset_index(drop=True)

    # 3. Load Futures Data
    fut_files = sorted(glob.glob(os.path.join(FUTURES_DIR, "*.csv")))
    df_fut = pd.concat([pd.read_csv(f) for f in fut_files])
    df_fut['time'] = pd.to_datetime(df_fut['timestamp'])
    df_fut = df_fut.sort_values('time').reset_index(drop=True)

    # 4. Prepare for Lightweight Charts (requires 'time' and 'value' columns)
    # Using 'close' prices for both
    spot_series = df_spot[['time', 'close']].rename(columns={'close': 'value'})
    fut_series = df_fut[['time', 'near_month_close']].rename(columns={'near_month_close': 'value'})

    # 5. Initialize Interactive Chart
    chart = Chart(toolbox=True, width=1000, height=600)
    
    # Configure Chart Appearance
    chart.layout(background_color='#0f172a', text_color='#94a3b8', font_size=12)
    chart.grid(vert_color='#1e293b', horz_color='#1e293b')

    # Add Spot Series (Emerald Green)
    line_spot = chart.create_line(name='NIFTY SPOT', color='#10b981', width=2)
    line_spot.set(spot_series)

    # Add Futures Series (Indigo Blue)
    line_fut = chart.create_line(name='NEAR-MONTH FUTURE', color='#6366f1', width=2)
    line_fut.set(fut_series)

    # Add Title/Legend
    chart.legend(visible=True, font_family='Arial', font_size=14)
    
    # Display the chart
    chart.show(block=True)

if __name__ == "__main__":
    visualize_interactive_market()
