"""
Configuration constants for the MPCORB Data Importer application.
Specific to the mpcorb.csv dataset structure.
"""
import os
import sys
import csv

# --- Global Settings ---
csv.field_size_limit(sys.maxsize)

# --- File Paths ---
INPUT_FILE = os.path.join('DATASETS', 'mpcorb.csv')
OUTPUT_DIR = 'output_tables_mpcorb'

# --- Processing Configuration ---
CHUNK_SIZE = 100000
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# --- Pandas Optimization: Dtypes for MPCORB ---
# Using string instead of float32 to avoid precision loss.
MPCORB_DTYPES = {
	'designation': 'string',
	'epoch': 'string',
	'hex_flags': 'string',
	'abs_mag': 'string',
	'slope_param': 'string',
	'mean_anomaly': 'string',
	'arg_perihelion': 'string',
	'long_asc_node': 'string',
	'inclination': 'string',
	'eccentricity': 'string',
	'mean_motion': 'string',
	'semi_major_axis': 'string',
	'uncertainty': 'string',
	'reference': 'string',
	'num_observations': 'string',
	'num_oppositions': 'string',
	'computer': 'string',
	'designation_full': 'string',
	'last_obs_date': 'string',
	'orbit_type': 'string',
	'is_neo': 'string'
}

# --- MPCORB Parsing Constants ---

# Heuristic constants to differentiate Software from Astronomers in the 'computer' column
# Logic: If name starts with "MPC" or is "orbfit", it is Software.
SOFTWARE_PREFIXES = ('MPC',)
SOFTWARE_SPECIFIC_NAMES = {'orbfit'}

# Hex Flags Masks
MASK_ORBIT_TYPE = 0x3F
MASK_NEO = 0x0800
MASK_1KM_NEO = 0x1000
MASK_1_OPPOSITION = 0x2000
MASK_CRITICAL_LIST = 0x4000
MASK_PHA = 0x8000

# Orbit Types Mapping
ORBIT_TYPES = {
	1: 'Atira',
	2: 'Aten',
	3: 'Apollo',
	4: 'Amor',
	5: 'Object with q < 1.665 AU',
	6: 'Hungaria',
	7: 'Phocaea',
	8: 'Hilda',
	9: 'Jupiter Trojan',
	10: 'Distant object'
}

# Date Unpacking Constants
CENTURY_MAP = {'I': 1800, 'J': 1900, 'K': 2000}
MONTH_MAP = {'A': 10, 'B': 11, 'C': 12}
DAY_MAP_OFFSET = 10

# Designation Unpacking Constants
PLANET_MAP = {'J': 'Jupiter', 'S': 'Saturn', 'U': 'Uranus', 'N': 'Neptune'}
CENTURY_PREFIX_MAP = {'I': '18', 'J': '19', 'K': '20'}

# --- Table Schemas ---
SCHEMAS = {
	'mpcorb_asteroids.csv': [
		'IDAsteroide', 'number', 'spkid', 'pdes', 'name', 'prefix', 'neo', 'pha', 'H', 'G',
		'diameter', 'diameter_sigma', 'albedo'
	],
	'mpcorb_astronomers.csv': [
		'IDAstronomo', 'Nome', 'IDCentro'
	],
	'mpcorb_observations.csv': [
		'IDObservacao', 'IDAsteroide', 'IDAstronomo', 'IDSoftware', 'IDEquipamento',
		'Data_atualizacao', 'Hora', 'Duracao', 'Modo'
	],
	'mpcorb_orbits.csv': [
		'IDOrbita', 'IDAsteroide', 'epoch', 'e', 'sigma_e', 'a', 'sigma_a', 'q', 'sigma_q',
		'i', 'sigma_i', 'om', 'sigma_om','w', 'sigma_w', 'ma', 'sigma_ma','ad','sigma_ad',
		'n', 'sigma_n', 'tp', 'sigma_tp', 'per', 'sigma_per', 'moid', 'moid_ld','rms', 'uncertainty',
		'Reference', 'Num_Obs', 'Num_Opp', 'Arc', 'Coarse_Perts', 'Precise_Perts',
		'Hex_Flags', 'Is1kmNEO', 'IsCriticalList', 'IsOneOppositionEarlier', 'IDClasse'
	],
	'mpcorb_software.csv': [
		'IDSoftware', 'Nome'
	],
	'mpcorb_classes.csv': [
		'IDClasse', 'Descricao', 'CodClasse'
	]
}