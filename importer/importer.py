import os
import time
import mssql_python
# Removed pandas/numpy as we are now doing direct BULK INSERT without preprocessing
from .config import (
	DB_CONNECTION_STRING, INPUT_DIR, BATCH_SIZE,
	TABLE_MAPPINGS, IMPORT_ORDER, IDENTITY_TABLES, STRING_LIMITS
)

class DBImporter:
	def __init__(self):
		print(f"Initializing Database connection (mssql_python)...")
		# Test connection
		try:
			with mssql_python.connect(DB_CONNECTION_STRING) as conn:
				print(f"Connected successfully to the database.")
		except Exception as e:
			print(f"Connection failed: {e}")
			raise e

	def _get_connection(self):
		return mssql_python.connect(DB_CONNECTION_STRING)

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
		# BULK INSERT requires an absolute path to the file
		filepath = os.path.abspath(os.path.join(INPUT_DIR, filename))

		if not os.path.exists(filepath):
			print(f"Skipping {filename}: File not found in {INPUT_DIR}.")
			return

		print(f"\n--- Bulk Importing {filename} -> {table_name} ---")

		start_time = time.time()

		try:
			with self._get_connection() as conn:
				with conn.cursor() as cursor:
					is_identity_table = table_name in IDENTITY_TABLES

					# Construct BULK INSERT options
					with_options = [
						"FIELDTERMINATOR = ','",
						"ROWTERMINATOR = '\\n'",
						"FIRSTROW = 2",
						"TABLOCK"
					]

					if is_identity_table:
						with_options.append("KEEPIDENTITY")

					sql = f"""
						BULK INSERT {table_name}
						FROM '{filepath}'
						WITH ({', '.join(with_options)})
					"""

					print(f"Executing BULK INSERT for {table_name}...")
					cursor.execute(sql)
					conn.commit()

		except Exception as e:
			print(f"\n[ERROR] Failed to import {filename}.")
			# Fallback error message if it's a permission issue (Error 5)
			if "Operating system error code 5" in str(e):
				print(f"[ERROR] Permission Denied: SQL Server cannot access the file.")
				print(f"Path: {filepath}")
				print("Ensure the SQL Server service account has read permissions for this folder.")
			else:
				print(f"Details: {e}")
			return

		duration = time.time() - start_time
		print(f"Completed {table_name} in {duration:.2f} seconds.")

	def run(self):
		self._create_default_observation_center()

		for filename in IMPORT_ORDER:
			if filename in TABLE_MAPPINGS:
				table_name = TABLE_MAPPINGS[filename]
				self.import_file(filename, table_name)
			else:
				print(f"Warning: No table mapping defined for {filename}")

		print("\nAll imports finished.")