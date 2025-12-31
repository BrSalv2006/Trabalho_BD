import os
import sys
import csv

# --- Global Settings ---
csv.field_size_limit(sys.maxsize)

# --- File Paths ---
DIR_MPC = 'output_tables_mpcorb'
DIR_NEO = 'output_tables_neo'
OUTPUT_DIR = 'final_dataset'

# --- Table Filenames ---
FILES = {
    'asteroids': 'asteroids.csv',
    'orbits': 'orbits.csv',
    'observations': 'observations.csv',
    'software': 'software.csv',
    'astronomers': 'astronomers.csv'
}

# --- Mapping Inputs to Target Names ---
INPUT_MAP_MPC = {
    'asteroids': 'mpcorb_asteroids.csv',
    'orbits': 'mpcorb_orbits.csv',
    'observations': 'mpcorb_observations.csv',
    'software': 'mpcorb_software.csv',
    'astronomers': 'mpcorb_astronomers.csv'
}

INPUT_MAP_NEO = {
    'asteroids': 'neo_asteroids.csv',
    'orbits': 'neo_orbits.csv',
    'observations': 'neo_observations.csv'
}

# --- Data Types for Fast Loading ---
MERGE_DTYPES = {
    'IDAsteroide': 'string',
    'number': 'string',
    'pdes': 'string',
    'name': 'string'
}