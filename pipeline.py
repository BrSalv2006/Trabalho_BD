import time
import os
import sys

# --- Import Processors ---
try:
    from processor_mpcorb.processor import AsteroidProcessor as MPCProcessor
    from processor_mpcorb.config import INPUT_FILE as MPC_INPUT, OUTPUT_DIR as MPC_OUTPUT
    from processor_neo.processor import AsteroidProcessor as NEOProcessor
    from processor_neo.config import INPUT_FILE as NEO_INPUT, OUTPUT_DIR as NEO_OUTPUT
    from merger.merger import DataMerger
    from importer.importer import DBImporter

except ImportError as e:
    print(f"CRITICAL ERROR: Could not import necessary modules. Check your file structure.")
    print(f"Details: {e}")
    sys.exit(1)

def run_pipeline():
    start_global = time.time()

    print("="*60)
    print("ASTEROID DATA PIPELINE AUTOMATION")
    print("="*60)

    # ---------------------------------------------------------
    # STEP 1: Process MPCORB Dataset
    # ---------------------------------------------------------
    print("\n" + "-"*40)
    print("[Step 1/4] Processing MPCORB Dataset...")
    print("-"*40)

    if os.path.exists(MPC_INPUT):
        try:
            mpc_processor = MPCProcessor(MPC_INPUT, MPC_OUTPUT)
            mpc_processor.process()
        except Exception as e:
            print(f"[ERROR] MPCORB Processing failed: {e}")
            # We continue? Usually merger fails without this, but let's try to proceed.
    else:
        print(f"[SKIP] MPCORB Input file not found: {MPC_INPUT}")

    # ---------------------------------------------------------
    # STEP 2: Process NEO Dataset
    # ---------------------------------------------------------
    print("\n" + "-"*40)
    print("[Step 2/4] Processing NEO Dataset...")
    print("-"*40)

    if os.path.exists(NEO_INPUT):
        try:
            neo_processor = NEOProcessor(NEO_INPUT, NEO_OUTPUT)
            neo_processor.process()
        except Exception as e:
            print(f"[ERROR] NEO Processing failed: {e}")
    else:
        print(f"[SKIP] NEO Input file not found: {NEO_INPUT}")

    # ---------------------------------------------------------
    # STEP 3: Merge Datasets
    # ---------------------------------------------------------
    print("\n" + "-"*40)
    print("[Step 3/4] Merging Processed Tables...")
    print("-"*40)

    try:
        merger = DataMerger()
        merger.run()
    except Exception as e:
        print(f"[ERROR] Merging failed: {e}")
        print("Stopping pipeline due to merge failure.")
        sys.exit(1)

    # ---------------------------------------------------------
    # STEP 4: Import to Database
    # ---------------------------------------------------------
    print("\n" + "-"*40)
    print("[Step 4/4] Importing to Database...")
    print("-"*40)

    try:
        importer = DBImporter()
        importer.run()
    except Exception as e:
        print(f"[ERROR] Database Import failed: {e}")

    # ---------------------------------------------------------
    # Final Summary
    # ---------------------------------------------------------
    end_global = time.time()
    duration = end_global - start_global

    print("\n" + "="*60)
    print(f"PIPELINE COMPLETE in {duration:.2f} seconds.")
    print("="*60)

if __name__ == "__main__":
    run_pipeline()