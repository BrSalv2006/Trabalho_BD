import shutil
import time
import os
import sys

# Add current directory to path so modules can find each other
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
	from processor_mpcorb import config as MPCConfig, processor as MPC
	from processor_neo import config as NEOConfig, processor as NEO
	from merger import config as MergeConfig, merger as Merge
	from importer import importer as Import
except ImportError as e:
	print(f"Import Error: {e}")
	print("Ensure you are running from the project root: 'python pipeline.py'")
	sys.exit(1)

def clean_dirs():
	dirs = [MPCConfig.OUTPUT_DIR, NEOConfig.OUTPUT_DIR, MergeConfig.OUTPUT_DIR]
	for d in dirs:
		if os.path.exists(d):
			shutil.rmtree(d)
			print(f"Cleaned: {d}")

def run():
	start_global = time.time()
	clean_dirs()

	print("\n=== 1. MPCORB Processing ===")
	MPC.AsteroidProcessor(MPCConfig.INPUT_FILE, MPCConfig.OUTPUT_DIR).process()

	print("\n=== 2. NEO Processing ===")
	NEO.AsteroidProcessor(NEOConfig.INPUT_FILE, NEOConfig.OUTPUT_DIR).process()

	print("\n=== 3. Merging ===")
	Merge.DataMerger().run()

	print("\n=== 4. Importing ===")
	Import.DBImporter().run()

	print(f"\nPipeline Complete in {time.time() - start_global:.2f}s")

if __name__ == "__main__":
	run()