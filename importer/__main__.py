import sys
import os
from importer import DBImporter
from importer.config import INPUT_DIR

def main():
    print("=== Database Bulk Importer ===")

    # Pre-flight check
    if not os.path.exists(INPUT_DIR):
        print(f"Error: Input directory '{INPUT_DIR}' not found.")
        print("Please run the 'merge_pipeline' module first to generate the dataset.")
        sys.exit(1)

    try:
        importer = DBImporter()
        importer.run()
    except Exception as e:
        print(f"\nFatal Error during initialization: {e}")
        print("Check your connection string in importer/config.py")
        sys.exit(1)

if __name__ == "__main__":
    main()