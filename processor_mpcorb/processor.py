import os
import time
import pandas as pd
import numpy as np
from typing import Dict, Set
from concurrent.futures import ProcessPoolExecutor

# Local Imports
from .config import (
	SCHEMAS, CHUNK_SIZE, MPCORB_DTYPES,
	SOFTWARE_PREFIXES, SOFTWARE_SPECIFIC_NAMES,
	MASK_ORBIT_TYPE, ORBIT_TYPES, MASK_NEO, MASK_PHA,
	MASK_1KM_NEO, MASK_CRITICAL_LIST, MASK_1_OPPOSITION
)
# Shared Imports
from common.utils import (
	ensure_directory, expand_scientific_notation,
	format_float_array, clean_dataframe_text
)
# Module Specific Utils
from .utils import (
	unpack_designation, unpack_packed_date, calculate_tp
)

def process_chunk_worker(chunk: pd.DataFrame) -> pd.DataFrame:
	"""
	Worker function for processing a chunk of MPCORB data.
	"""
	# 1. Basic Cleaning
	chunk = chunk.dropna(subset=['designation']).copy()
	if chunk.empty:
		return chunk

	# 2. Unpack Designation (Hybrid Optimization)
	is_numeric = chunk['designation'].str.isdigit()

	chunk['obj_id'] = chunk['designation'] # Default
	if (~is_numeric).any():
		complex_desigs = chunk.loc[~is_numeric, 'designation']
		chunk.loc[~is_numeric, 'obj_id'] = [unpack_designation(x) for x in complex_desigs]

	# 3. Vectorized Math (Numpy)
	e_float = pd.to_numeric(chunk['eccentricity'], errors='coerce').fillna(0).values
	a_float = pd.to_numeric(chunk['semi_major_axis'], errors='coerce').fillna(0).values
	n_float = pd.to_numeric(chunk['mean_motion'], errors='coerce').fillna(0).values
	ma_float = pd.to_numeric(chunk['mean_anomaly'], errors='coerce').fillna(0).values

	# Derived Calculations
	q_float = a_float * (1.0 - e_float)
	ad_float = a_float * (1.0 + e_float)
	# Avoid divide by zero
	per_float = np.divide(360.0, n_float, out=np.zeros_like(n_float), where=n_float!=0)

	# Optimized Formatting
	chunk['q'] = format_float_array(q_float, 10)
	chunk['ad'] = format_float_array(ad_float, 10)
	chunk['per'] = format_float_array(per_float, 5)

	# 4. Hex Flag Decoding
	hex_list = [int(x, 16) if isinstance(x, str) and len(x) == 4 else 0 for x in chunk['hex_flags']]
	flags_int = np.array(hex_list, dtype=np.int32)

	chunk['OrbitType'] = pd.Series(flags_int & MASK_ORBIT_TYPE).map(ORBIT_TYPES).values
	chunk['is_neo_flag'] = ((flags_int & MASK_NEO) != 0).astype(int)
	chunk['is_pha_flag'] = ((flags_int & MASK_PHA) != 0).astype(int)
	chunk['is_1km_neo'] = ((flags_int & MASK_1KM_NEO) != 0).astype(int)
	chunk['is_critical'] = ((flags_int & MASK_CRITICAL_LIST) != 0).astype(int)
	chunk['is_opp_earlier'] = ((flags_int & MASK_1_OPPOSITION) != 0).astype(int)

	# 5. Date Parsing
	chunk['epoch_iso'] = [unpack_packed_date(x) for x in chunk['epoch']]

	# 6. Tp Calculation
	epochs_dt = pd.to_datetime(chunk['epoch_iso'], errors='coerce')
	n_safe = np.where(n_float != 0, n_float, np.nan)
	offset_days = ma_float / n_safe
	MAX_DAYS = 106000 # Sanity check limit

	valid_mask = (np.abs(offset_days) <= MAX_DAYS) & pd.notna(epochs_dt) & pd.notna(offset_days)
	chunk['tp'] = ""

	if valid_mask.any():
		valid_offsets = pd.to_timedelta(offset_days[valid_mask], unit='D')
		chunk.loc[valid_mask, 'tp'] = (epochs_dt[valid_mask] - valid_offsets).dt.strftime('%Y-%m-%d %H:%M:%S.%f').fillna("")

	# Fallback for complex cases
	fallback_mask = (~valid_mask) & (chunk['epoch_iso'] != "") & (n_float != 0)
	if fallback_mask.any():
		subset = chunk.loc[fallback_mask]
		chunk.loc[fallback_mask, 'tp'] = [
			calculate_tp(ep, ma, n_val)
			for ep, ma, n_val in zip(subset['epoch_iso'], ma_float[fallback_mask], n_float[fallback_mask])
		]

	# 7. Designation Parsing
	# FIX: Initialize with object dtype to prevent "incompatible dtype" error
	chunk['number_str'] = pd.Series([np.nan] * len(chunk), dtype='object')

	# User Heuristic:
	# Packed Designation Length == 7 -> Unnumbered (Provisional).
	# Packed Designation Length != 7 -> Numbered.
	desig_len = chunk['designation'].astype(str).str.len()
	is_numbered = (desig_len != 7)

	if is_numbered.any():
		# For numbered, the unpacked obj_id IS the number (e.g. '1', '99942')
		# UPDATED: Trim leading zeros from the number string
		chunk.loc[is_numbered, 'number_str'] = chunk.loc[is_numbered, 'obj_id'].astype(str).str.lstrip('0')

	chunk['name_parsed'] = ""
	chunk['pdes_parsed'] = ""

	# Logic for Name and PDES
	# If unnumbered: obj_id is the PDES.
	chunk.loc[~is_numbered, 'pdes_parsed'] = chunk.loc[~is_numbered, 'obj_id']

	# If numbered: Name is usually in designation_full "(123) Name"
	if is_numbered.any():
		split = chunk.loc[is_numbered, 'designation_full'].str.split(')', n=1, expand=True)
		if split.shape[1] > 1:
			chunk.loc[is_numbered, 'name_parsed'] = split[1].str.strip()

	return chunk.drop(columns=['designation_full', 'designation'], errors='ignore')

class AsteroidProcessor:
	def __init__(self, input_path: str, output_dir: str):
		self.input_path = input_path
		self.output_dir = output_dir
		self.seen_ids: Set[str] = set()

		# ID Maps
		self.software_map: Dict[str, int] = {}
		self.next_software_id = 1
		self.astronomer_map: Dict[str, int] = {}
		self.next_astronomer_id = 1
		self.class_map: Dict[str, int] = {}
		self.next_class_id = 1

		# Counters
		self.next_observation_id = 1
		self.next_orbit_id = 1
		self.next_asteroid_id = 1
		self.file_handles = {}

	def _map_computers_and_astronomers(self, chunk: pd.DataFrame) -> None:
		chunk['id_soft'] = ""
		chunk['id_astro'] = ""

		entities = chunk['computer'].dropna().unique()
		for name in entities:
			name_str = str(name).strip()
			if not name_str: continue

			if name_str.startswith(SOFTWARE_PREFIXES) or name_str in SOFTWARE_SPECIFIC_NAMES:
				if name_str not in self.software_map:
					self.software_map[name_str] = self.next_software_id
					self.next_software_id += 1
			else:
				if name_str not in self.astronomer_map:
					self.astronomer_map[name_str] = self.next_astronomer_id
					self.next_astronomer_id += 1

		# Vectorized mapping
		computer_clean = chunk['computer'].str.strip()
		chunk['id_soft'] = computer_clean.map(self.software_map).fillna("").astype(str).str.replace(r'\.0$', '', regex=True)
		chunk['id_astro'] = computer_clean.map(self.astronomer_map).fillna("").astype(str).str.replace(r'\.0$', '', regex=True)

	def _map_classes(self, chunk: pd.DataFrame) -> None:
		chunk['id_class'] = ""
		classes = chunk['OrbitType'].dropna().unique()

		for name in classes:
			if not name: continue

			# Filter out 'Unclassified'
			if str(name).strip().lower() == 'unclassified':
				continue

			if name not in self.class_map:
				self.class_map[name] = self.next_class_id
				self.next_class_id += 1

		chunk['id_class'] = chunk['OrbitType'].map(self.class_map).fillna("").astype(str).str.replace(r'\.0$', '', regex=True)

	def process(self):
		print(f"Reading {self.input_path}...")
		if not os.path.exists(self.input_path):
			print(f"Error: {self.input_path} not found.")
			return

		ensure_directory(self.output_dir)

		try:
			# Init Files
			for filename, headers in SCHEMAS.items():
				self.file_handles[filename] = open(os.path.join(self.output_dir, filename), 'w', encoding='utf-8', newline='')
				pd.DataFrame(columns=headers).to_csv(self.file_handles[filename], index=False)

			start_time = time.time()
			total_records = 0
			max_workers = max(1, (os.cpu_count() or 2) - 1)
			print(f"Processing with {max_workers} workers...")

			with pd.read_csv(self.input_path, chunksize=CHUNK_SIZE, dtype=MPCORB_DTYPES, on_bad_lines='skip', low_memory=False) as reader:
				with ProcessPoolExecutor(max_workers=max_workers) as executor:
					futures = []

					def handle_future(future):
						nonlocal total_records
						try:
							chunk = future.result()
							if chunk is None or chunk.empty: return

							# Filter duplicates locally per chunk relative to global seen
							is_new = ~chunk['obj_id'].isin(self.seen_ids)
							chunk = chunk[is_new].copy().reset_index(drop=True)
							if chunk.empty: return

							self.seen_ids.update(chunk['obj_id'])

							# Assign IDs
							chunk_len = len(chunk)
							chunk['IDAsteroide'] = range(self.next_asteroid_id, self.next_asteroid_id + chunk_len)
							chunk['IDOrbita'] = range(self.next_orbit_id, self.next_orbit_id + chunk_len)
							self.next_asteroid_id += chunk_len
							self.next_orbit_id += chunk_len

							self._map_computers_and_astronomers(chunk)
							self._map_classes(chunk)
							self._write_tables(chunk)

							total_records += chunk_len
							print(f"Processed {total_records} records...", end='\r')
						except Exception as e:
							print(f"Worker Error: {e}")

					for raw_chunk in reader:
						futures.append(executor.submit(process_chunk_worker, raw_chunk))
						if len(futures) >= max_workers * 2:
							handle_future(futures.pop(0))

					for f in futures: handle_future(f)

			self._write_reference_tables()
			print(f"\nDone! Processed {total_records} records in {time.time() - start_time:.2f}s.")

		finally:
			for f in self.file_handles.values(): f.close()

	def _write_reference_tables(self):
		# Software
		if self.software_map and 'mpcorb_software.csv' in self.file_handles:
			df = pd.DataFrame(list(self.software_map.items()), columns=['Nome', 'IDSoftware'])
			df['Versao'] = ""
			df = clean_dataframe_text(df, ['Nome'])
			df[['IDSoftware', 'Nome', 'Versao']].to_csv(self.file_handles['mpcorb_software.csv'], mode='a', header=False, index=False)

		# Astronomers
		if self.astronomer_map and 'mpcorb_astronomers.csv' in self.file_handles:
			df = pd.DataFrame(list(self.astronomer_map.items()), columns=['Nome', 'IDAstronomo'])
			df['IDCentro'] = ""
			df = clean_dataframe_text(df, ['Nome'])
			df[['IDAstronomo', 'Nome', 'IDCentro']].to_csv(self.file_handles['mpcorb_astronomers.csv'], mode='a', header=False, index=False)

		# Classes
		if self.class_map and 'mpcorb_classes.csv' in self.file_handles:
			df = pd.DataFrame(list(self.class_map.items()), columns=['Descricao', 'IDClasse'])
			df['CodClasse'] = df['Descricao']
			df = clean_dataframe_text(df, ['Descricao'])
			df[['IDClasse', 'Descricao', 'CodClasse']].to_csv(self.file_handles['mpcorb_classes.csv'], mode='a', header=False, index=False)

	def _write_tables(self, chunk):
		# Optimized write

		# Asteroids
		df_ast = pd.DataFrame({
			'IDAsteroide': chunk['IDAsteroide'],
			'number': chunk['number_str'],
			'spkid': "",
			'pdes': chunk['pdes_parsed'],
			'name': chunk['name_parsed'],
			'prefix': "",
			'neo': chunk['is_neo_flag'],
			'pha': chunk['is_pha_flag'],
			'H': [expand_scientific_notation(x) for x in chunk['abs_mag']],
			'G': [expand_scientific_notation(x) for x in chunk['slope_param']],
			'diameter': "", 'diameter_sigma': "", 'albedo': ""
		})
		clean_dataframe_text(df_ast, ['name'])
		df_ast.to_csv(self.file_handles['mpcorb_asteroids.csv'], mode='a', header=False, index=False)

		# Observations
		df_obs = pd.DataFrame({
			'IDObservacao': range(self.next_observation_id, self.next_observation_id + len(chunk)),
			'IDAsteroide': chunk['IDAsteroide'],
			'IDAstronomo': chunk['id_astro'],
			'IDSoftware': chunk['id_soft'],
			'IDEquipamento': "",
			'Data_atualizacao': chunk['epoch_iso'],
			'Hora': "", 'Duracao': "", 'Modo': "Orbit Computation"
		})
		self.next_observation_id += len(chunk)
		df_obs = df_obs[SCHEMAS['mpcorb_observations.csv']] # Enforce order
		df_obs.to_csv(self.file_handles['mpcorb_observations.csv'], mode='a', header=False, index=False)

		# Orbits
		# Prepare columns that need cleaning/formatting
		cols_to_expand = {
			'e': 'eccentricity', 'a': 'semi_major_axis', 'i': 'inclination',
			'om': 'long_asc_node', 'w': 'arg_perihelion', 'ma': 'mean_anomaly',
			'n': 'mean_motion', 'rms': 'rms_residual'
		}

		orbit_data = {
			'IDOrbita': chunk['IDOrbita'],
			'IDAsteroide': chunk['IDAsteroide'],
			'epoch': chunk['epoch_iso'],
			'tp': chunk['tp'],
			'q': chunk['q'], 'ad': chunk['ad'], 'per': chunk['per'], # Pre-calculated
			'Hex_Flags': chunk['hex_flags'],
			'Is1kmNEO': chunk['is_1km_neo'], 'IsCriticalList': chunk['is_critical'],
			'IsOneOppositionEarlier': chunk['is_opp_earlier'],
			'uncertainty': chunk.get('uncertainty', ""),
			'Reference': chunk.get('reference', ""),
			'Coarse_Perts': chunk.get('coarse_perturbers', ""),
			'Precise_Perts': chunk.get('precise_perturbers', ""),
			'IDClasse': chunk['id_class'],
			'Arc': chunk.get('first_obs', "").astype(str) + "-" + chunk.get('last_obs', "").astype(str),
			'moid': "", 'moid_ld': ""
		}

		# Fill standard floats
		for dest, src in cols_to_expand.items():
			orbit_data[dest] = [expand_scientific_notation(x) for x in chunk[src]]

		# Fill empty sigmas
		for sigma in ['sigma_e', 'sigma_a', 'sigma_q', 'sigma_i', 'sigma_om', 'sigma_w', 'sigma_ma', 'sigma_ad', 'sigma_n', 'sigma_tp', 'sigma_per']:
			orbit_data[sigma] = ""

		df_orb = pd.DataFrame(orbit_data)

		# Num Obs/Opp handling
		df_orb['Num_Obs'] = pd.to_numeric(chunk['num_observations'], errors='coerce').fillna(-1).astype(int).astype(str).replace('-1', '')
		df_orb['Num_Opp'] = pd.to_numeric(chunk['num_oppositions'], errors='coerce').fillna(-1).astype(int).astype(str).replace('-1', '')

		clean_dataframe_text(df_orb, ['Reference'])
		df_orb = df_orb[SCHEMAS['mpcorb_orbits.csv']]
		df_orb.to_csv(self.file_handles['mpcorb_orbits.csv'], mode='a', header=False, index=False)