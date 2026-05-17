import pyarrow.parquet as pq
import glob

# Find an options parquet file
files = glob.glob("warehouse/options/*/*/*.parquet")
if files:
    file = files[0]
    meta = pq.read_metadata(file)
    print(f"File: {file}")
    print(f"Num Row Groups: {meta.num_row_groups}")
    print(f"Num Rows: {meta.num_rows}")
    for i in range(min(3, meta.num_row_groups)):
        rg = meta.row_group(i)
        print(f"Row Group {i}: {rg.num_rows} rows, {rg.total_byte_size} bytes")
        # Check stats for timestamp column (usually column 0)
        col_meta = rg.column(0)
        print(f"  Col 0 (Timestamp) Stats: {col_meta.statistics}")
else:
    print("No options files found.")
