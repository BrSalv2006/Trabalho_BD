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
NEO_DTYPES = {
    'id': 'string',
    'spkid': 'string',
    'full_name': 'string',
    'pdes': 'string',
    'name': 'string',
    'prefix': 'string',
    'neo': 'string',  # Y/N
    'pha': 'string',  # Y/N
    'h': 'float32',
    'diameter': 'float32',
    'albedo': 'float32',
    'diameter_sigma': 'float32',
    'orbit_id': 'string',
    'epoch': 'float64',
    'epoch_mjd': 'float64',
    'epoch_cal': 'string', # 20190427
    'equinox': 'string',
    'e': 'float32',
    'a': 'float32',
    'q': 'float32',
    'i': 'float32',
    'om': 'float32',
    'w': 'float32',
    'ma': 'float32',
    'ad': 'float32',
    'n': 'float32',
    'tp': 'float64',
    'tp_cal': 'string', # 20170705.6734485
    'per': 'float32',
    'per_y': 'float32',
    'moid': 'float32',
    'moid_ld': 'float32',
    'sigma_e': 'float32',
    'sigma_a': 'float32',
    'sigma_q': 'float32',
    'sigma_i': 'float32',
    'sigma_om': 'float32',
    'sigma_w': 'float32',
    'sigma_ma': 'float32',
    'sigma_ad': 'float32',
    'sigma_n': 'float32',
    'sigma_tp': 'float32',
    'sigma_per': 'float32',
    'class': 'string',
    'rms': 'float32',
    'class_description': 'string'
}

# --- Table Schemas ---
SCHEMAS = {
    'neo_asteroids.csv': [
        'IDAsteroide', 'number', 'spkid', 'pdes', 'name', 'prefix', 'H', 'G',
        'diameter', 'diameter_sigma', 'albedo', 'neo', 'pha'
    ],
    'neo_observations.csv': [
        'IDAsteroide', 'IDAstronomo', 'IDSoftware', 'Data_atualizacao',
        'IDEquipamento', 'Hora', 'Duracao', 'Modo'
    ],
    'neo_orbits.csv': [
        'IDAsteroide', 'epoch', 'e', 'a', 'i', 'om', 'w', 'ma', 'n', 'tp',
        'moid', 'moid_ld', 'q', 'ad', 'per', 'rms', 'Arc',
        'sigma_e', 'sigma_a', 'sigma_q', 'sigma_i', 'sigma_om', 'sigma_w',
        'sigma_ma', 'sigma_ad', 'sigma_n', 'sigma_tp', 'sigma_per',
        'Hex_Flags', 'OrbitType', 'Is1kmNEO', 'IsCriticalList',
        'IsOneOppositionEarlier', 'uncertainty', 'Reference', 'Num_Obs',
        'Num_Opp', 'Coarse_Perts', 'Precise_Perts', 'IDClasse'
    ],
    'neo_classes.csv': [
        'IDClasse', 'Descricao', 'CodClasse'
    ]
}