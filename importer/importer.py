import os
import time
import mssql_python
from typing import Optional
from .config import (
    DB_CONNECTION_STRING, INPUT_DIR,
    TABLE_MAPPINGS, IMPORT_ORDER, IDENTITY_TABLES
)

class DBImporter:
    def __init__(self):
        print(f"Initializing Database connection...")
        self.validate_connection()

    def validate_connection(self):
        try:
            with mssql_python.connect(DB_CONNECTION_STRING) as conn:
                print(f"Connected successfully to database.")
        except Exception as e:
            print(f"Connection failed: {e}")
            raise

    def _get_connection(self):
        return mssql_python.connect(DB_CONNECTION_STRING)

    def _create_default_observation_center(self):
        """Creates a default observation center (ID 1) if missing."""
        print("\n--- Checking Default Observation Center ---")
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
                        print("Created default center: ID 1")
                    else:
                        print("Default center exists.")
        except Exception as e:
            print(f"[ERROR] Could not create default center: {e}")
            raise

    def import_file(self, filename: str, table_name: str):
        filepath = os.path.abspath(os.path.join(INPUT_DIR, filename))

        if not os.path.exists(filepath):
            print(f"Skipping {filename}: File not found in {INPUT_DIR}.")
            return

        print(f"\n--- Importing {filename} -> {table_name} ---")
        start_time = time.time()

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Configure BULK INSERT options
                    options = [
                        "FIELDTERMINATOR = ','",
                        "ROWTERMINATOR = '\\n'",
                        "FIRSTROW = 2",
                        "TABLOCK"
                    ]
                    if table_name in IDENTITY_TABLES:
                        options.append("KEEPIDENTITY")

                    sql = f"BULK INSERT {table_name} FROM '{filepath}' WITH ({', '.join(options)})"

                    print(f"Executing BULK INSERT...")
                    cursor.execute(sql)
                    conn.commit()

        except Exception as e:
            print(f"\n[ERROR] Failed to import {filename}.")
            if "Operating system error code 5" in str(e):
                print(f"Permission Denied: SQL Server cannot read '{filepath}'. Check service account permissions.")
            else:
                print(f"Details: {e}")
            return

        print(f"Completed {table_name} in {time.time() - start_time:.2f} seconds.")

    def run(self):
        self._create_default_observation_center()

        for filename in IMPORT_ORDER:
            if filename in TABLE_MAPPINGS:
                self.import_file(filename, TABLE_MAPPINGS[filename])
            else:
                print(f"Warning: No mapping for {filename}")

        print("\nAll imports finished.")