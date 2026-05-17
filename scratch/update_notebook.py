import json
import os

notebook_path = 'EDA_notebook.ipynb'
with open(notebook_path, 'r', encoding='utf-8') as f:
    notebook = json.load(f)

for cell in notebook.get('cells', []):
    if cell['cell_type'] == 'code':
        source = cell['source']
        for i, line in enumerate(source):
            if '# 1. Check Monotonicity (Applies to all)' in line:
                source[i] = line.replace('# 1. Check Monotonicity (Applies to all)', '# 1. Check Monotonicity (Applies to all) -> Maps to Error Code 1')
            elif '# 2. Check Duplicates (Skipped for OPTIONS)' in line:
                source[i] = line.replace('# 2. Check Duplicates (Skipped for OPTIONS)', '# 2. Check Duplicates (Skipped for OPTIONS) -> Maps to Error Code 2')
            elif 'missing_spot = expected_ts[~expected_ts.isin(df_spot[\'timestamp\'])]' in line:
                source.insert(i, '    # Missing Candles Check -> Maps to Error Code 4\n')
            elif '# Price Abnormalities' in line:
                source[i] = line.replace('# Price Abnormalities', '# Price Abnormalities (OHLC Violations) -> Maps to Error Code 8')
            elif '# Sudden Spikes (> 1% in 1 minute)' in line:
                source[i] = line.replace('# Sudden Spikes (> 1% in 1 minute)', '# Sudden Spikes (> 1% in 1 minute) -> Maps to Error Code 16')
            elif '# 3. Spread Anomaly' in line:
                source[i] = line.replace('# 3. Spread Anomaly', '# 3. Spread Anomaly -> Maps to Error Code 32')
            elif '# 4. Stale Prices (No change in 5 mins while volume > 0)' in line:
                source[i] = line.replace('# 4. Stale Prices (No change in 5 mins while volume > 0)', '# 4. Stale Prices (No change in 5 mins while volume > 0) -> Maps to Error Code 64')
            elif 'spot_bad = ' in line and 'df_unison[\'E_FO\']' in line:
                source.insert(i, '    # Maps to Error Code 256\n')
            elif 'fut_bad = ' in line and 'df_unison[\'E_SO\']' in line:
                source.insert(i, '    # Maps to Error Code 512\n')
            elif 'opt_bad = ' in line and 'df_unison[\'E_SF\']' in line:
                source.insert(i, '    # Maps to Error Code 1024\n')

with open(notebook_path, 'w', encoding='utf-8') as f:
    json.dump(notebook, f, indent=1)

print('Notebook updated successfully.')
