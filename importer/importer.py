import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from importer.config import (
    DB_CONNECTION_STRING, INPUT_DIR, BATCH_SIZE,
    TABLE_MAPPINGS, IMPORT_ORDER, IDENTITY_TABLES, STRING_LIMITS
)

class DBImporter:
    def __init__(self):
        print(f"Initializing Database connection...")
        self.engine = create_engine(DB_CONNECTION_STRING, fast_executemany=True)

    def _create_default_observation_center(self):
        """
        TEMPORARY: Creates a default observation center (ID 1) for testing.
        This prevents FK violations when inserting Astronomers with IDCentro=1.
        """
        print("\n--- Creating Default Observation Center (Temporary) ---")
        try:
            with self.engine.connect() as conn:
                with conn.begin():
                    check_sql = text("SELECT COUNT(*) FROM Centro_de_observacao WHERE IDCentro = 1")
                    result = conn.execute(check_sql).scalar()

                    if result == 0:
                        try:
                            conn.execute(text("SET IDENTITY_INSERT Centro_de_observacao ON"))
                            insert_sql = text(
                                "INSERT INTO Centro_de_observacao (IDCentro, Nome, Localizacao) "
                                "VALUES (1, 'Default Test Center', 'Earth')"
                            )
                            conn.execute(insert_sql)
                        finally:
                            conn.execute(text("SET IDENTITY_INSERT Centro_de_observacao OFF"))
                        print("Created default center: ID 1 - Default Test Center")
                    else:
                        print("Default center ID 1 already exists.")
        except Exception as e:
            print(f"[ERROR] Could not create default center: {e}")
            raise e

    def import_file(self, filename, table_name):
        filepath = os.path.join(INPUT_DIR, filename)

        if not os.path.exists(filepath):
            print(f"Skipping {filename}: File not found in {INPUT_DIR}.")
            return

        print(f"\n--- Importing {filename} -> {table_name} ---")

        start_time = time.time()
        total_rows = 0

        # Read everything as String/Object to preserve exact CSV formatting
        # This prevents Python float conversion artifacts.
        chunk_iterator = pd.read_csv(
            filepath,
            chunksize=BATCH_SIZE,
            dtype=str,
            keep_default_na=False
        )

        with self.engine.connect() as conn:
            is_identity_table = table_name in IDENTITY_TABLES

            try:
                for chunk in chunk_iterator:
                    if chunk.empty:
                        continue

                    # 1. Clean Data: Convert empty strings to SQL NULL
                    chunk = chunk.replace({"": None, "nan": None})

                    # --- TEMPORARY FIX: Assign IDCentro=1 for Astronomers ---
                    if table_name == 'Astronomo':
                        if 'IDCentro' in chunk.columns:
                            chunk['IDCentro'] = chunk['IDCentro'].fillna('1')
                        else:
                            chunk['IDCentro'] = '1'
                    # -------------------------------------------------------

                    # 2. Enforce String Limits (Truncation)
                    # This only affects defined VARCHAR columns in STRING_LIMITS (pdes, spkid, etc.)
                    # It does NOT touch numeric columns like H, G, e, a.
                    if table_name in STRING_LIMITS:
                        for col, limit in STRING_LIMITS[table_name].items():
                            if col in chunk.columns:
                                # Safe truncation for string columns
                                chunk[col] = chunk[col].astype(str).str.slice(0, limit)
                                # Clean up "None" or "nan" strings created by astype(str) on nulls
                                chunk.loc[chunk[col].isin(['None', 'nan']), col] = None

                    # 3. Insert to Database
                    with conn.begin():
                        try:
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
                        finally:
                            if is_identity_table:
                                conn.execute(text(f"SET IDENTITY_INSERT [{table_name}] OFF"))

                    total_rows += len(chunk)
                    print(f"Inserted {total_rows} rows...", end='\r')

            except Exception as e:
                print(f"\n[ERROR] Failed to import chunk from {filename} (Rows {total_rows} to {total_rows + len(chunk)}).")
                print(f"Details: {e}")

                # --- Diagnostic Block ---
                # Helps identify which column is overflowing if errors persist
                print("\n--- Diagnostic: Analyzing Data in current chunk ---")
                try:
                    for col in chunk.columns:
                        non_null = chunk[col].dropna().astype(str)
                        if non_null.empty:
                            continue

                        max_len = non_null.str.len().max()
                        longest_val = non_null.loc[non_null.str.len().idxmax()]

                        limit_info = ""
                        violation_mark = ""

                        if table_name in STRING_LIMITS and col in STRING_LIMITS[table_name]:
                            limit = STRING_LIMITS[table_name][col]
                            limit_info = f" (Limit: {limit})"
                            if max_len > limit:
                                violation_mark = " <--- TRUNCATION ERROR"

                        if max_len == 36:
                            violation_mark += " <--- SUSPICIOUS LENGTH (UUID?)"

                        print(f"Column '{col}': Max Len = {max_len}{limit_info}. Sample: '{longest_val}'{violation_mark}")
                except Exception:
                    pass
                return

        duration = time.time() - start_time
        print(f"\nCompleted {table_name}: {total_rows} rows in {duration:.2f} seconds.")

    def run(self):
        self._create_default_observation_center()

        for filename in IMPORT_ORDER:
            if filename in TABLE_MAPPINGS:
                table_name = TABLE_MAPPINGS[filename]
                self.import_file(filename, table_name)
            else:
                print(f"Warning: No table mapping defined for {filename}")

        print("\nAll imports finished.")