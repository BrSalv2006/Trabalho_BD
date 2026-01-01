import time
import os
import sys

# --- Import Processors ---
try:
    from processor_mpcorb import config as MPCProcessorConfig, processor as MPCProcessor
    from processor_neo import config as NEOProcessorConfig, processor as NEOProcessor
    from merger import merger as DataMerger
    #from importer import importer as DBImporter

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

    if os.path.exists(MPCProcessorConfig.INPUT_FILE):
        try:
            mpc_processor = MPCProcessor.AsteroidProcessor(MPCProcessorConfig.INPUT_FILE, MPCProcessorConfig.OUTPUT_DIR)
            mpc_processor.process()
        except Exception as e:
            print(f"[ERROR] MPCORB Processing failed: {e}")
            # We continue? Usually merger fails without this, but let's try to proceed.
    else:
        print(f"[SKIP] MPCORB Input file not found: {MPCProcessorConfig.INPUT_FILE}")

    # ---------------------------------------------------------
    # STEP 2: Process NEO Dataset
    # ---------------------------------------------------------
    print("\n" + "-"*40)
    print("[Step 2/4] Processing NEO Dataset...")
    print("-"*40)

    if os.path.exists(NEOProcessorConfig.INPUT_FILE):
        try:
            neo_processor = NEOProcessor.AsteroidProcessor(NEOProcessorConfig.INPUT_FILE, NEOProcessorConfig.OUTPUT_DIR)
            neo_processor.process()
        except Exception as e:
            print(f"[ERROR] NEO Processing failed: {e}")
    else:
        print(f"[SKIP] NEO Input file not found: {NEOProcessorConfig.INPUT_FILE}")

    # ---------------------------------------------------------
    # STEP 3: Merge Datasets
    # ---------------------------------------------------------
    print("\n" + "-"*40)
    print("[Step 3/4] Merging Processed Tables...")
    print("-"*40)

    try:
        merger = DataMerger.DataMerger()
        merger.run()
    except Exception as e:
        print(f"[ERROR] Merging failed: {e}")
        print("Stopping pipeline due to merge failure.")
        sys.exit(1)

    # ---------------------------------------------------------
    # STEP 4: Import to Database
    # ---------------------------------------------------------
    #print("\n" + "-"*40)
    #print("[Step 4/4] Importing to Database...")
    #print("-"*40)
#
    #try:
    #    importer = DBImporter()
    #    importer.run()
    #except Exception as e:
    #    print(f"[ERROR] Database Import failed: {e}")

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