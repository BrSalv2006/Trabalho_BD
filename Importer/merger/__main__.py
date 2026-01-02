import time
from merger.merger import DataMerger
from merger.config import OUTPUT_DIR

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