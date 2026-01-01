import os
import time
import mssql_python
import pandas as pd
import numpy as np
from .config import (
    DB_CONFIG, INPUT_DIR, BATCH_SIZE,
    TABLE_MAPPINGS, IMPORT_ORDER, IDENTITY_TABLES, STRING_LIMITS
)

class DBImporter:
    def __init__(self):
        print(f"Initializing Database connection (mssql_python)...")
        self.conn_params = DB_CONFIG
        # Test connection
        try:
            with mssql_python.connect(**self.conn_params) as conn:
                print(f"Connected successfully to {self.conn_params['server']}")
        except Exception as e:
            print(f"Connection failed: {e}")
            raise e

    def _get_connection(self):
        return mssql_python.connect(**self.conn_params)

    def _create_default_observation_center(self):
        """
        TEMPORARY: Creates a default observation center (ID 1) for testing.
        """
        print("\n--- Creating Default Observation Center (Temporary) ---")
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM Centro_de_observacao WHERE IDCentro = 1")
                    result = cursor.fetchone()[0]

                    if result == 0:
                        cursor.execute("SET IDENTITY_INSERT Centro_de_observacao ON")
                        cursor.execute(
                            "INSERT INTO Centro_de_observacao (IDCentro, Nome, Localizacao) "
                            "VALUES (1, 'Default Test Center', 'Earth')"
                        )
                        cursor.execute("SET IDENTITY_INSERT Centro_de_observacao OFF")
                        conn.commit()
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

        # Read CSV as string/object to preserve formatting
        chunk_iterator = pd.read_csv(
            filepath,
            chunksize=BATCH_SIZE,
            dtype=str,
            keep_default_na=False
        )

        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                is_identity_table = table_name in IDENTITY_TABLES

                for chunk in chunk_iterator:
                    if chunk.empty:
                        continue

                    # 1. Clean Data: Convert empty strings to None (NULL in SQL)
                    # We iterate to replace, or use replace()
                    chunk = chunk.replace({"": None, "nan": None})

                    # --- TEMPORARY FIX: Assign IDCentro=1 for Astronomers ---
                    if table_name == 'Astronomo':
                        if 'IDCentro' in chunk.columns:
                            chunk['IDCentro'] = chunk['IDCentro'].fillna('1')
                        else:
                            chunk['IDCentro'] = '1'
                    # -------------------------------------------------------

                    # 2. Enforce String Limits
                    if table_name in STRING_LIMITS:
                        for col, limit in STRING_LIMITS[table_name].items():
                            if col in chunk.columns:
                                chunk[col] = chunk[col].astype(str).str.slice(0, limit)
                                # Fix None becoming "None" string
                                chunk.loc[chunk[col] == "None", col] = None

                    # 3. Construct SQL Statement
                    columns = list(chunk.columns)
                    quoted_columns = [f"{col}" for col in columns] # Simple quoting
                    cols_str = ", ".join(quoted_columns)
                    placeholders = ", ".join(["%s"] * len(columns))

                    sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"

                    # Convert DataFrame to list of tuples for executemany
                    # We must replace nan/None correctly. replace({np.nan: None}) handles numpy NaNs.
                    chunk_data = chunk.where(pd.notnull(chunk), None).values.tolist()

                    # 4. Execute Batch
                    try:
                        if is_identity_table:
                            cursor.execute(f"SET IDENTITY_INSERT {table_name} ON")

                        cursor.executemany(sql, chunk_data)

                        if is_identity_table:
                            cursor.execute(f"SET IDENTITY_INSERT {table_name} OFF")

                        conn.commit()

                        total_rows += len(chunk)
                        print(f"Inserted {total_rows} rows...", end='\r')

                    except Exception as e:
                        conn.rollback()
                        print(f"\n[ERROR] Failed to import chunk from {filename}.")
                        print(f"Details: {e}")
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