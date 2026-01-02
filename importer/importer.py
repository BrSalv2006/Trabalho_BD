import os
import time
import csv
import mssql_python
from importer.config import (
	DB_CONNECTION_STRING, INPUT_DIR,
	TABLE_MAPPINGS, IMPORT_ORDER, IDENTITY_TABLES, BATCH_SIZE
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

	def import_file_bulk(self, filename: str, table_name: str):
		filepath = os.path.abspath(os.path.join(INPUT_DIR, filename))

		if not os.path.exists(filepath):
			print(f"Skipping {filename}: File not found in {INPUT_DIR}.")
			return

		print(f"\n--- Importing {filename} -> {table_name} (BULK INSERT) ---")
		start_time = time.time()

		try:
			with self._get_connection() as conn:
				with conn.cursor() as cursor:
					# Configure BULK INSERT options
					options = [
						"FIELDTERMINATOR = ','",
						"ROWTERMINATOR = '\\n'",
						"FIRSTROW = 2",
						"TABLOCK",
						"FIRE_TRIGGERS"
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

	def import_file_standard(self, filename: str, table_name: str):
		filepath = os.path.abspath(os.path.join(INPUT_DIR, filename))

		if not os.path.exists(filepath):
			print(f"Skipping {filename}: File not found in {INPUT_DIR}.")
			return

		print(f"\n--- Importing {filename} -> {table_name} (Standard INSERT) ---")
		start_time = time.time()

		try:
			with open(filepath, 'r', encoding='utf-8') as f:
				reader = csv.reader(f)
				try:
					headers = next(reader)
				except StopIteration:
					print(f"Empty file: {filename}")
					return

				# Prepare SQL
				columns = ", ".join(headers)
				placeholders = ", ".join(["?"] * len(headers))
				sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

				batch = []
				count = 0

				with self._get_connection() as conn:
					with conn.cursor() as cursor:
						if table_name in IDENTITY_TABLES:
							cursor.execute(f"SET IDENTITY_INSERT {table_name} ON")

						for row in reader:
							# Convert empty strings to None to ensure NULLs are inserted correctly
							cleaned_row = [None if cell == '' else cell for cell in row]
							batch.append(cleaned_row)
							if len(batch) >= BATCH_SIZE:
								cursor.executemany(sql, batch)
								conn.commit()
								count += len(batch)
								print(f"  Imported {count} rows...", end='\r')
								batch = []

						if batch:
							cursor.executemany(sql, batch)
							conn.commit()
							count += len(batch)

						if table_name in IDENTITY_TABLES:
							cursor.execute(f"SET IDENTITY_INSERT {table_name} OFF")

		except Exception as e:
			print(f"\n[ERROR] Failed to import {filename}.")
			print(f"Details: {e}")
			return

		print(f"Completed {table_name} ({count} rows) in {time.time() - start_time:.2f} seconds.")

	def run(self, use_bulk=False):
		self._create_default_observation_center()

		for filename in IMPORT_ORDER:
			if filename in TABLE_MAPPINGS:
				if use_bulk:
					self.import_file_bulk(filename, TABLE_MAPPINGS[filename])
				else:
					self.import_file_standard(filename, TABLE_MAPPINGS[filename])
			else:
				print(f"Warning: No mapping for {filename}")

		print("\nAll imports finished.")