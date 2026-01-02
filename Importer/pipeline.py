import shutil
import time
import os
import sys

# Ensure we can import modules from the script's directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
	sys.path.insert(0, BASE_DIR)

# --- Import Processors ---
try:
	from processor_mpcorb import config as MPCProcessorConfig, processor as MPCProcessor
	from processor_neo import config as NEOProcessorConfig, processor as NEOProcessor
	from merger import config as MergerConfig, merger as DataMerger
	from importer import importer as DBImporter
except ImportError as e:
	print(f"CRITICAL ERROR: Could not import necessary modules. {e}")
	sys.exit(1)

def clean_directories():
	"""Removes old output directories."""
	dirs_to_clean = [
		MPCProcessorConfig.OUTPUT_DIR,
		NEOProcessorConfig.OUTPUT_DIR,
		MergerConfig.OUTPUT_DIR
	]

	for d in dirs_to_clean:
		if os.path.exists(d):
			try:
				shutil.rmtree(d)
				print(f"  [CLEAN] Removed: {d}")
			except OSError as e:
				print(f"  [WARN] Failed to remove {d}: {e}")

def run_step(name: str, step_func, *args):
	print(f"\n{'-'*40}\n[{name}]\n{'-'*40}")
	try:
		step_func(*args)
	except Exception as e:
		print(f"[ERROR] {name} failed: {e}")
		# Depending on severity, we might want to sys.exit(1) here
		# For now, we print and continue only if it's not critical, but mostly these are critical.
		if "Processing" in name or "Merging" in name:
			print("Stopping pipeline due to critical failure.")
			sys.exit(1)

def run_pipeline():
	start_global = time.time()
	print("="*60)
	print("ASTEROID DATA PIPELINE AUTOMATION")
	print("="*60)

	clean_directories()

	# Step 1: MPCORB
	if os.path.exists(MPCProcessorConfig.INPUT_FILE):
		run_step("Step 1/4: MPCORB Processing",
				 MPCProcessor.AsteroidProcessor(MPCProcessorConfig.INPUT_FILE, MPCProcessorConfig.OUTPUT_DIR).process)
	else:
		print(f"[SKIP] MPCORB Input not found: {MPCProcessorConfig.INPUT_FILE}")

	# Step 2: NEO
	if os.path.exists(NEOProcessorConfig.INPUT_FILE):
		run_step("Step 2/4: NEO Processing",
				 NEOProcessor.AsteroidProcessor(NEOProcessorConfig.INPUT_FILE, NEOProcessorConfig.OUTPUT_DIR).process)
	else:
		print(f"[SKIP] NEO Input not found: {NEOProcessorConfig.INPUT_FILE}")

	# Step 3: Merge
	run_step("Step 3/4: Merging Datasets", DataMerger.DataMerger().run)

	# Step 4: Import
	run_step("Step 4/4: Database Import", DBImporter.DBImporter().run)

	duration = time.time() - start_global
	print("\n" + "="*60)
	print(f"PIPELINE COMPLETE in {duration:.2f} seconds.")
	print("="*60)

if __name__ == "__main__":
	run_pipeline()