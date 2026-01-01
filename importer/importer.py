import os
import time
import mssql_python
from .config import (
    DB_CONNECTION_STRING, INPUT_DIR,
    TABLE_MAPPINGS, IMPORT_ORDER, IDENTITY_TABLES
)

class DBImporter:
    def __init__(self):
        print(f"Initializing MSSQL Connection...")
        # Validate connection on init
        with self._get_connection():
            print("Connected.")

    def _get_connection(self):
        return mssql_python.connect(DB_CONNECTION_STRING)

    def _create_default_center(self):
        print("Checking default observation center...")
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM Centro_de_observacao WHERE IDCentro = 1")
                    if cursor.fetchone()[0] == 0:
                        print("Creating Default Center (ID 1)...")
                        cursor.execute("SET IDENTITY_INSERT Centro_de_observacao ON")
                        cursor.execute("INSERT INTO Centro_de_observacao (IDCentro, Nome, Localizacao) VALUES (1, 'Default', 'Earth')")
                        cursor.execute("SET IDENTITY_INSERT Centro_de_observacao OFF")
                        conn.commit()
        except Exception as e:
            print(f"Error creating default center: {e}")

    def import_file(self, filename, table_name):
        filepath = os.path.abspath(os.path.join(INPUT_DIR, filename))
        if not os.path.exists(filepath):
            print(f"Skipping {filename} (Not Found)")
            return

        print(f"Importing {filename} -> {table_name}...")
        start = time.time()

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    options = ["FIELDTERMINATOR = ','", "ROWTERMINATOR = '\\n'", "FIRSTROW = 2", "TABLOCK"]
                    if table_name in IDENTITY_TABLES:
                        options.append("KEEPIDENTITY")

                    sql = f"BULK INSERT {table_name} FROM '{filepath}' WITH ({', '.join(options)})"
                    cursor.execute(sql)
                    conn.commit()
                    print(f"  Done in {time.time() - start:.2f}s")
        except Exception as e:
            print(f"  FAILED: {e}")
            if "Operating system error code 5" in str(e):
                print("  Hint: Check SQL Server permissions for the folder.")

    def run(self):
        self._create_default_center()
        for filename in IMPORT_ORDER:
            if filename in TABLE_MAPPINGS:
                self.import_file(filename, TABLE_MAPPINGS[filename])