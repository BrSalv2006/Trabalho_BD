import os
from dotenv import load_dotenv

# --- Database Connection ---
# Default connection string for MSSQL using ODBC Driver 17.
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
# Reference tables must be loaded before Dependent tables.
IMPORT_ORDER = [
    'classes.csv',      # Independent
    'software.csv',     # Independent
    'astronomers.csv',  # Independent (references Centro, but we assume those exist or are nullable/handled)
    'asteroids.csv',    # Independent (mostly)
    'observations.csv', # Depends on Asteroid, Software, Astronomer
    'orbits.csv'        # Depends on Asteroid, Classe
]

# --- Identity Tables ---
# Tables that have an IDENTITY column where we need to insert explicit IDs from the CSV.
IDENTITY_TABLES = {'Classe', 'Software', 'Astronomo', 'Asteroide'}

# --- String Limits (Truncation) ---
# Enforce VARCHAR limits to prevent "String data, right truncation" errors.
STRING_LIMITS = {
    'Asteroide': {
        'spkid': 20,
        'pdes': 20,
        'name': 100,
        'prefix': 10
    },
    'Orbita': {
        'uncertainty': 10,
        'Reference': 50,
        'Arc': 20,
        'Coarse_Perts': 20,
        'Precise_Perts': 20,
        'Hex_Flags': 10
    },
    'Software': {
        'Nome': 100,
        'Versao': 20
    },
    'Astronomo': {
        'Nome': 100
    },
    'Classe': {
        'CodClasse': 50,
        'Descricao': 255
    },
    'Observacao': {
        'Modo': 50
    }
}