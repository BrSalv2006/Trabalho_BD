import os
from dotenv import load_dotenv

# --- Database Connection ---
load_dotenv()

DB_CONNECTION_STRING = os.getenv('SQL_CONNECTION_STRING')

# --- Input Configuration ---
# Directory containing the merged CSV files
INPUT_DIR = 'final_dataset'

# Number of rows to insert per transaction
BATCH_SIZE = 1000

# --- Table Mapping ---
# Maps the CSV filename to the destination Table Name in the database.
TABLE_MAPPINGS = {
	'classes.csv': 'Classe',
	'software.csv': 'Software',
	'astronomers.csv': 'Astronomo',
	'asteroids.csv': 'Asteroide',
	'observations.csv': 'Observacao',
	'orbits.csv': 'Orbita'
}

# --- Import Order ---
# Critical for Foreign Key constraints.
IMPORT_ORDER = [
	'classes.csv',      # Independent
	'software.csv',     # Independent
	'astronomers.csv',  # Independent
	'asteroids.csv',    # Independent
	'observations.csv', # Depends on Asteroid, Software, Astronomer
	'orbits.csv'        # Depends on Asteroid, Classe
]

# --- Identity Tables ---
# Tables that have an IDENTITY column where we need to insert explicit IDs from the CSV.
# Added 'Orbita' and 'Observacao' to ensure IDs match the CSVs.
IDENTITY_TABLES = {'Classe', 'Software', 'Astronomo', 'Asteroide', 'Orbita', 'Observacao'}