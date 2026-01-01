import os
import sys
from processor_mpcorb.config import INPUT_FILE, OUTPUT_DIR
from processor import AsteroidProcessor

def main():
    print("=== MPCORB Data Processor ===")

    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file '{INPUT_FILE}' not found.")
        print("Please ensure 'mpcorb.csv' is in the 'DATASETS' folder.")
        sys.exit(1)

    # Display core count
    cores = os.cpu_count() or 1
    print(f"Input: {INPUT_FILE}")
    print(f"Output: {OUTPUT_DIR}/")
    print(f"Detected {cores} CPU cores for parallel processing.")

    # Instantiating the standardized class
    processor = AsteroidProcessor(INPUT_FILE, OUTPUT_DIR)
    processor.process()

if __name__ == "__main__":
    main()