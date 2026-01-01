import os
import time
from .merger import DataMerger
from .config import OUTPUT_DIR

def main():
	print("=== Data Merge Pipeline ===")
	print(f"Target Output: {OUTPUT_DIR}/")

	start = time.time()

	merger = DataMerger()
	merger.run()

	end = time.time()
	print(f"Total Merge Time: {end - start:.2f} seconds")

if __name__ == "__main__":
	main()