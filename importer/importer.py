import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from config import (
    DB_CONNECTION_STRING, INPUT_DIR, BATCH_SIZE,
    TABLE_MAPPINGS, IMPORT_ORDER, IDENTITY_TABLES, STRING_LIMITS
)

class DBImporter:
    def __init__(self):
        print(f"Initializing Database connection...")
        # fast_executemany=True is highly recommended for MSSQL+pyodbc performance
        self.engine = create_engine(DB_CONNECTION_STRING, fast_executemany=True)

    def import_file(self, filename, table_name):
        filepath = os.path.join(INPUT_DIR, filename)

        if not os.path.exists(filepath):
            print(f"Skipping {filename}: File not found in {INPUT_DIR}.")
            return

        print(f"\n--- Importing {filename} -> {table_name} ---")

        start_time = time.time()
        total_rows = 0

        # We read as string (dtype=str) to preserve formatting (e.g. "001" vs "1")
        chunk_iterator = pd.read_csv(
            filepath,
            chunksize=BATCH_SIZE,
            dtype=str,
            keep_default_na=False
        )

        try:
            # Use a single connection for the file to manage IDENTITY_INSERT
            with self.engine.connect() as conn:

                # Check if we need to enable IDENTITY_INSERT
                is_identity_table = table_name in IDENTITY_TABLES

                for chunk in chunk_iterator:
                    if chunk.empty:
                        continue

                    # 1. Clean Data: Convert empty strings "" to None (SQL NULL)
                    chunk = chunk.replace({"": None, "nan": None})

                    # 2. Enforce String Limits (Truncation)
                    if table_name in STRING_LIMITS:
                        for col, limit in STRING_LIMITS[table_name].items():
                            if col in chunk.columns:
                                # Slice strings to the limit
                                chunk[col] = chunk[col].astype(str).str.slice(0, limit)
                                # Restore None for "None" strings resulting from astype conversion of nulls
                                chunk.loc[chunk[col] == 'None', col] = None

                    # 3. Insert to Database using Transaction
                    # We wrap the insert in a transaction to handle IDENTITY_INSERT safely
                    with conn.begin():
                        if is_identity_table:
                            conn.execute(text(f"SET IDENTITY_INSERT [{table_name}] ON"))

                        chunk.to_sql(
                            table_name,
                            conn,
                            if_exists='append',
                            index=False,
                            method=None,
                            chunksize=BATCH_SIZE
                        )

                        if is_identity_table:
                            conn.execute(text(f"SET IDENTITY_INSERT [{table_name}] OFF"))

                    total_rows += len(chunk)
                    print(f"Inserted {total_rows} rows...", end='\r')

        except Exception as e:
            print(f"\n[ERROR] Failed to import {filename}: {e}")
            return

        duration = time.time() - start_time
        print(f"\nCompleted {table_name}: {total_rows} rows in {duration:.2f} seconds.")

    def run(self):
        # Iterate through the defined order to respect Foreign Keys
        for filename in IMPORT_ORDER:
            if filename in TABLE_MAPPINGS:
                table_name = TABLE_MAPPINGS[filename]
                self.import_file(filename, table_name)
            else:
                print(f"Warning: No table mapping defined for {filename}")

        print("\nAll imports finished.")