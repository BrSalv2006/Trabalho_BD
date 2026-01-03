import os
import re
import sys
from dotenv import load_dotenv
import mssql_python

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv(os.path.join(BASE_DIR, '.env'))

DB_CONNECTION_STRING = os.getenv('SQL_CONNECTION_STRING')

def execute_sql_file(cursor, file_path):
	print(f"Executing {file_path}...")
	try:
		with open(file_path, 'r', encoding='utf-8') as f:
			sql_content = f.read()
	except FileNotFoundError:
		print(f"Error: File {file_path} not found.")
		return

	# Split by GO (case insensitive, on its own line)
	commands = re.split(r'(?i)^\s*GO\s*$', sql_content, flags=re.MULTILINE)

	for cmd in commands:
		cmd = cmd.strip()
		if cmd:
			try:
				cursor.execute(cmd)
			except Exception as e:
				print(f"Error executing command in {file_path}:")
				print(f"Command start: {cmd[:100]}...")
				print(f"Error: {e}")
				# Stop execution on error
				raise e

def run_initialization():
	if not DB_CONNECTION_STRING:
		print("Error: SQL_CONNECTION_STRING not found in .env")
		return

	print(f"Connecting to database...")
	try:
		with mssql_python.connect(DB_CONNECTION_STRING) as conn:
			cursor = conn.cursor()

			files = ['drop_tables.sql', 'tables.sql', 'triggers.sql', 'stored_procedures.sql', 'views.sql']

			# SQL files are in the Importer directory (one level up from scripts)
			SQL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

			for file_name in files:
				# Use absolute path for SQL files
				full_path = os.path.join(SQL_DIR, file_name)
				execute_sql_file(cursor, full_path)
				conn.commit()
				print(f"Successfully executed {file_name}")

			print("\nAll scripts executed successfully.")

	except Exception as e:
		print(f"\nCritical Error: {e}")
		# Re-raise exception so the GUI knows it failed
		raise e

if __name__ == "__main__":
	try:
		run_initialization()
	except Exception:
		sys.exit(1)
