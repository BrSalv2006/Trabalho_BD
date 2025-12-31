import os
from dotenv import load_dotenv

# --- Database Connection ---
# Default connection string for MSSQL using ODBC Driver 17.
# IMPORTANT: Update this with your actual credentials.
# Format: mssql+pyodbc://username:password@host/database?driver=ODBC+Driver+17+for+SQL+Server
load_dotenv()

DB_CONNECTION_STRING = os.getenv(
    'SQL_CONNECTION_STRING_TEST2'
)

# --- Input Configuration ---
# Directory containing the merged CSV files
INPUT_DIR = 'final_dataset'

# Number of rows to insert per transaction
BATCH_SIZE = 10000

# --- Table Mapping ---
# Maps the CSV filename to the destination Table Name in the database.
# Ensure these table names exist in your database schema.
TABLE_MAPPINGS = {
    'software.csv': 'Software',
    'astronomers.csv': 'Astronomo',
    'asteroids.csv': 'Asteroide',
    'observations.csv': 'Obeservacao',
    'orbits.csv': 'Orbita'
}

# --- Import Order ---
# Critical for Foreign Key constraints.
# Reference tables must be loaded before Dependent tables.
IMPORT_ORDER = [
    'software.csv',    # Independent
    'astronomers.csv',   # Independent
    'asteroids.csv',   # Independent (mostly)
    'observations.csv',   # Depends on Asteroid, Software, Astronomer
    'orbits.csv'      # Depends on Asteroid
]