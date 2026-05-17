"""
Script to add error-code comments to EDA_notebook.ipynb cells,
mapping each check to its corresponding validation.py error codes.

ERROR_DICTIONARY from validation.py:
    1:    "Backward time jump detected"
    2:    "Duplicate timestamp detected"
    4:    "Missing expected 1-minute candle (Row inserted to fill gap)"
    8:    "OHLC integrity violation (e.g., Low > High)"
    16:   "Price spike > 1% in a single candle"
    32:   "Abnormal futures spread (>5% between near and mid month)"
    64:   "Stale price (No price change for 5+ mins with volume > 0)"
    128:  "Invalid Option Strike or Side"
    256:  "Triangulation: Isolated Spot Anomaly"
    512:  "Triangulation: Isolated Futures Anomaly"
    1024: "Triangulation: Isolated Options Anomaly"
"""

import json
import sys
import os

NOTEBOOK_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "EDA_notebook.ipynb")

# Read the notebook
with open(NOTEBOOK_PATH, "r", encoding="utf-8") as f:
    nb = json.load(f)

cells = nb["cells"]

# --- Helper: prepend comment lines to a code cell's source ---
def prepend_comment(cell, comment_lines):
    """Prepend comment lines to a cell's source (list of strings)."""
    # Build the comment block
    block = []
    for line in comment_lines:
        block.append(f"# {line}\n")
    block.append("#\n")
    # Ensure original source starts fresh after the comment block
    cell["source"] = block + cell["source"]


# Track how many cells we've annotated
annotated = 0

for cell in cells:
    if cell["cell_type"] != "code":
        continue

    src = "".join(cell["source"])

    # ------------------------------------------------------------------
    # Cell: #check 1  –  Raw Sequencing & Alignment Check
    # Located in section "0. Raw Sequencing & Alignment Check"
    # Checks: backward time jumps, duplicate timestamps
    # ------------------------------------------------------------------
    if src.lstrip().startswith("#check 1") and "check_raw_alignment" in src:
        prepend_comment(cell, [
            "=== VALIDATION ERROR CODE MAPPING (validation.py) ===",
            "This cell checks raw data alignment BEFORE sorting.",
            "  • Backward time jumps  → Error Code 1  (Bit 1):  'Backward time jump detected'",
            "  • Duplicate timestamps → Error Code 2  (Bit 2):  'Duplicate timestamp detected'",
            "=======================================================",
        ])
        annotated += 1

    # ------------------------------------------------------------------
    # Cell: #check 2  –  Spot: Missing Candles, OHLC, Price Spikes
    # ------------------------------------------------------------------
    elif src.lstrip().startswith("#check 2") and "missing_spot" in src:
        prepend_comment(cell, [
            "=== VALIDATION ERROR CODE MAPPING (validation.py) ===",
            "This cell checks Spot data quality.",
            "  • Missing candles (gap detection)  → Error Code 4  (Bit 4):  'Missing expected 1-minute candle'",
            "  • OHLC violations (Low > High etc) → Error Code 8  (Bit 8):  'OHLC integrity violation'",
            "  • Price spikes > 1% per minute     → Error Code 16 (Bit 16): 'Price spike > 1% in a single candle'",
            "=======================================================",
        ])
        annotated += 1

    # ------------------------------------------------------------------
    # Cell: #check 3  –  Return matrix / multi-lookback volatility analysis
    # ------------------------------------------------------------------
    elif src.lstrip().startswith("#check 3") and "create_return_matrix" in src:
        prepend_comment(cell, [
            "=== VALIDATION ERROR CODE MAPPING (validation.py) ===",
            "This cell builds a multi-lookback return matrix for spike analysis.",
            "  • Identifies candles with abnormal returns across lookback windows.",
            "  • Related to → Error Code 16 (Bit 16): 'Price spike > 1% in a single candle'",
            "=======================================================",
        ])
        annotated += 1

    # ------------------------------------------------------------------
    # Cell: # check 3  –  1-minute absolute spikes (>0.5%)
    # ------------------------------------------------------------------
    elif src.lstrip().startswith("# check 3") and "spikes_1min" in src:
        prepend_comment(cell, [
            "=== VALIDATION ERROR CODE MAPPING (validation.py) ===",
            "This cell filters 1-minute absolute return spikes above a threshold.",
            "  • Related to → Error Code 16 (Bit 16): 'Price spike > 1% in a single candle'",
            "  (Here threshold is 0.5%, but validation.py uses 1%.)",
            "=======================================================",
        ])
        annotated += 1

    # ------------------------------------------------------------------
    # Cell: # futures check 1  –  Futures: Missing candles, Spread, Stale prices
    # ------------------------------------------------------------------
    elif src.lstrip().startswith("# futures check 1") and "missing_fut" in src:
        prepend_comment(cell, [
            "=== VALIDATION ERROR CODE MAPPING (validation.py) ===",
            "This cell checks Futures data quality.",
            "  • Missing candles (gap detection)         → Error Code 4  (Bit 4):   'Missing expected 1-minute candle'",
            "  • Abnormal spread > 5% (near vs mid)      → Error Code 32 (Bit 32):  'Abnormal futures spread (>5%)'",
            "  • Stale prices (no change 5+ mins, vol>0) → Error Code 64 (Bit 64):  'Stale price (No price change for 5+ mins with volume > 0)'",
            "=======================================================",
        ])
        annotated += 1

    # ------------------------------------------------------------------
    # Cell: # options check 1  –  Options: Snapshot alignment, CE/PE completeness, IV
    # ------------------------------------------------------------------
    elif src.lstrip().startswith("# options check 1") and "non_5min_snapshots" in src:
        prepend_comment(cell, [
            "=== VALIDATION ERROR CODE MAPPING (validation.py) ===",
            "This cell checks Options chain data quality.",
            "  • Snapshot alignment (5-min boundary)     → General data quality (no direct bitmask code)",
            "  • Incomplete CE/PE pairs                  → Error Code 128 (Bit 128): 'Invalid Option Strike or Side'",
            "  • Zero / extreme IV records               → General data quality (no direct bitmask code)",
            "  • OHLC integrity on options               → Error Code 8   (Bit 8):   'OHLC integrity violation'",
            "=======================================================",
        ])
        annotated += 1

    # ------------------------------------------------------------------
    # Cell: # overall check  –  Load all datasets for cross-validation
    # ------------------------------------------------------------------
    elif src.lstrip().startswith("# overall check") and "Data Loaded for all days" in src:
        prepend_comment(cell, [
            "=== VALIDATION ERROR CODE MAPPING (validation.py) ===",
            "This cell loads ALL Spot, Futures, and Options data.",
            "  • Data loading step — no direct error code.",
            "  • Prepares data for cross-market consistency checks below.",
            "=======================================================",
        ])
        annotated += 1

    # ------------------------------------------------------------------
    # Cell: # check  –  VALIDATION 1-5 (Timestamp sync, Basis, Arbitrage, Frozen, IV)
    # ------------------------------------------------------------------
    elif src.lstrip().startswith("# check") and "VALIDATION 1" in src and "VALIDATION 5" in src:
        prepend_comment(cell, [
            "=== VALIDATION ERROR CODE MAPPING (validation.py) ===",
            "This cell runs 5 cross-data consistency checks:",
            "  VALIDATION 1 – Timestamp Synchronization",
            "    • Related to → Error Code 2  (Bit 2):  'Duplicate timestamp detected'",
            "  VALIDATION 2 – Basis Stability (Spot vs Futures)",
            "    • Related to → Error Code 32 (Bit 32): 'Abnormal futures spread (>5%)'",
            "  VALIDATION 3 – Options Arbitrage Violations (Intrinsic Value)",
            "    • Related to → Error Code 8  (Bit 8):  'OHLC integrity violation'",
            "  VALIDATION 4 – Frozen Data (O=H=L=C bars)",
            "    • Related to → Error Code 64 (Bit 64): 'Stale price (No price change for 5+ mins)'",
            "  VALIDATION 5 – IV Sanity (zero or extreme IV)",
            "    • General data quality (no direct bitmask code)",
            "=======================================================",
        ])
        annotated += 1

    # ------------------------------------------------------------------
    # Cell: #check  –  Delta-based triangulation (spike validation)
    # ------------------------------------------------------------------
    elif src.lstrip().startswith("#check") and "DELTA-BASED TRIANGULATION" in src:
        prepend_comment(cell, [
            "=== VALIDATION ERROR CODE MAPPING (validation.py) ===",
            "This cell performs DELTA-BASED TRIANGULATION to classify spot spikes.",
            "  • REAL MARKET SPIKE   — All 3 markets agree: no error flagged.",
            "  • BAD DATA (SPOT)     → Error Code 256  (Bit 256):  'Triangulation: Isolated Spot Anomaly'",
            "  • BAD DATA (FUTURES)  → Error Code 512  (Bit 512):  'Triangulation: Isolated Futures Anomaly'",
            "  • UNCERTAIN / MIXED   — Inconclusive; may warrant manual review.",
            "  (Options anomaly)     → Error Code 1024 (Bit 1024): 'Triangulation: Isolated Options Anomaly'",
            "=======================================================",
        ])
        annotated += 1

    # ------------------------------------------------------------------
    # Cell: #check  –  Volatility-aware triangular validation
    # ------------------------------------------------------------------
    elif src.lstrip().startswith("#check") and "VOLATILITY-AWARE TRIANGULAR VALIDATION" in src:
        prepend_comment(cell, [
            "=== VALIDATION ERROR CODE MAPPING (validation.py) ===",
            "This cell performs VOLATILITY-AWARE TRIANGULAR VALIDATION",
            "using VIX-scaled dynamic thresholds on E_SF, E_FO, E_SO error vectors.",
            "  • Isolated Spot anomaly    → Error Code 256  (Bit 256):  'Triangulation: Isolated Spot Anomaly'",
            "  • Isolated Futures anomaly → Error Code 512  (Bit 512):  'Triangulation: Isolated Futures Anomaly'",
            "  • Isolated Options anomaly → Error Code 1024 (Bit 1024): 'Triangulation: Isolated Options Anomaly'",
            "=======================================================",
        ])
        annotated += 1

# Write the modified notebook back
with open(NOTEBOOK_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1)

print(f"Done! Annotated {annotated} cells with validation error code comments.")
