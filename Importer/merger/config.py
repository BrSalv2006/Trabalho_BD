import sys
import csv
import os

# --- Global Settings ---
csv.field_size_limit(sys.maxsize)

# --- File Paths ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DIR_MPC = os.path.join(BASE_DIR, 'output_tables_mpcorb')
DIR_NEO = os.path.join(BASE_DIR, 'output_tables_neo')
OUTPUT_DIR = os.path.join(BASE_DIR, 'final_dataset')

# --- Table Filenames ---
FILES = {
	'asteroids': 'asteroids.csv',
	'orbits': 'orbits.csv',
	'observations': 'observations.csv',
	'software': 'software.csv',
	'astronomers': 'astronomers.csv',
	'classes': 'classes.csv'
}

# --- Mapping Inputs to Target Names ---
INPUT_MAP_MPC = {
	'asteroids': 'mpcorb_asteroids.csv',
	'orbits': 'mpcorb_orbits.csv',
	'observations': 'mpcorb_observations.csv',
	'software': 'mpcorb_software.csv',
	'astronomers': 'mpcorb_astronomers.csv',
	'classes': 'mpcorb_classes.csv'
}

INPUT_MAP_NEO = {
	'asteroids': 'neo_asteroids.csv',
	'orbits': 'neo_orbits.csv',
	'observations': 'neo_observations.csv',
	'classes': 'neo_classes.csv'
}

# --- Data Types for Fast Loading ---
MERGE_DTYPES = {
	'IDAsteroide': 'string',
	'IDOrbita': 'string',
	'number': 'string',
	'pdes': 'string',
	'name': 'string'
}