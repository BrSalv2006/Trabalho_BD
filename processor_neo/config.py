"""
Configuration constants for the NEO Data Importer application.
Specific to the neo.csv dataset structure.
"""
import os
import sys
import csv

# --- Global Settings ---
csv.field_size_limit(sys.maxsize)

# --- File Paths ---
INPUT_FILE = os.path.join('DATASETS', 'neo.csv')
OUTPUT_DIR = 'output_tables_neo'

# --- Processing Config ---
CHUNK_SIZE = 100000
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# --- Pandas Optimization: Dtypes for NEO ---
# Using string instead of float32 to avoid precision loss.
NEO_DTYPES = {
    'id': 'string',
    'spkid': 'string',
    'full_name': 'string',
    'pdes': 'string',
    'name': 'string',
    'prefix': 'string',
    'neo': 'string',  # Y/N
    'pha': 'string',  # Y/N
    'h': 'string',
    'diameter': 'string',
    'albedo': 'string',
    'diameter_sigma': 'string',
    'orbit_id': 'string',
    'epoch': 'string',
    'epoch_mjd': 'string',
    'epoch_cal': 'string',
    'equinox': 'string',
    'e': 'string',
    'a': 'string',
    'q': 'string',
    'i': 'string',
    'om': 'string',
    'w': 'string',
    'ma': 'string',
    'ad': 'string',
    'n': 'string',
    'tp': 'string',
    'tp_cal': 'string',
    'per': 'string',
    'per_y': 'string',
    'moid': 'string',
    'moid_ld': 'string',
    'sigma_e': 'string',
    'sigma_a': 'string',
    'sigma_q': 'string',
    'sigma_i': 'string',
    'sigma_om': 'string',
    'sigma_w': 'string',
    'sigma_ma': 'string',
    'sigma_ad': 'string',
    'sigma_n': 'string',
    'sigma_tp': 'string',
    'sigma_per': 'string',
    'class': 'string',
    'rms': 'string',
    'class_description': 'string'
}

# --- Table Schemas ---
SCHEMAS = {
    'neo_asteroids.csv': [
        'IDAsteroide', 'number', 'spkid', 'pdes', 'name', 'prefix', 'neo', 'pha', 'H', 'G',
        'diameter', 'diameter_sigma', 'albedo'
    ],
    'neo_observations.csv': [
        'IDObservacao', 'IDAsteroide', 'IDAstronomo', 'IDEquipamento', 'IDSoftware',
        'Data_atualizacao', 'Hora', 'Duracao', 'Modo'
    ],
    'neo_orbits.csv': [
        'IDAsteroide', 'epoch', 'e', 'a', 'i', 'om', 'w', 'ma', 'n', 'tp',
        'moid', 'moid_ld', 'q', 'ad', 'per', 'rms', 'Arc',
        'sigma_e', 'sigma_a', 'sigma_q', 'sigma_i', 'sigma_om', 'sigma_w',
        'sigma_ma', 'sigma_ad', 'sigma_n', 'sigma_tp', 'sigma_per',
        'Hex_Flags', 'Is1kmNEO', 'IsCriticalList',
        'IsOneOppositionEarlier', 'uncertainty', 'Reference', 'Num_Obs',
        'Num_Opp', 'Coarse_Perts', 'Precise_Perts', 'IDClasse'
    ],
    'neo_classes.csv': [
        'IDClasse', 'Descricao', 'CodClasse'
    ]
}