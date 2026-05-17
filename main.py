import sys
import os

# Add current directory to path
sys.path.append(os.path.abspath('.'))

from run_pipeline import run_ingestion
from benchmark import run_benchmarks

if __name__ == "__main__":
    print("=== QUANT SINGULARITY DATA ENGINE PIPELINE ===")
    try:
        run_ingestion()
        print("\nSuccess: Warehouse is ready and views are created.")
        print("You can now use src/api.py or run API_Test.ipynb to access the data.")
        run_benchmarks()
        print("\nSuccess: Benchmarks completed and logged to MLflow.")

    except Exception as e:
        print(f"\nPipeline failed: {e}")
        sys.exit(1)
