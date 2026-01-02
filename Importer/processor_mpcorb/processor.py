import os
import time
import pandas as pd
import numpy as np
from typing import Dict, Set, Any
from concurrent.futures import ProcessPoolExecutor

from processor_mpcorb.config import (
	SCHEMAS, CHUNK_SIZE, MPCORB_DTYPES,
	SOFTWARE_PREFIXES, SOFTWARE_SPECIFIC_NAMES,
	MASK_ORBIT_TYPE, ORBIT_TYPES, MASK_NEO, MASK_PHA
)
from processor_mpcorb.utils import ensure_directory, unpack_designation, unpack_packed_date, calculate_tp, expand_scientific_notation

# --- Worker Function ---

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

	# Initialize obj_id with existing values
	chunk['obj_id'] = chunk['designation'].astype(object)

	# Only apply unpack logic to non-numeric designations
	if (~is_numeric).any():
		complex_desigs = chunk.loc[~is_numeric, 'designation']
		chunk.loc[~is_numeric, 'obj_id'] = [unpack_designation(x) for x in complex_desigs]

	# 3. Vectorized Math (Numpy)
	# Convert to numeric, forcing errors to NaN, then filling with 0 for safety
	e_float = pd.to_numeric(chunk['eccentricity'], errors='coerce').fillna(0.0).values
	a_float = pd.to_numeric(chunk['semi_major_axis'], errors='coerce').fillna(0.0).values
	n_float = pd.to_numeric(chunk['mean_motion'], errors='coerce').fillna(0.0).values
	ma_float = pd.to_numeric(chunk['mean_anomaly'], errors='coerce').fillna(0.0).values

	# Derived Calculations
	q_float = a_float * (1.0 - e_float)
	ad_float = a_float * (1.0 + e_float)

	# Safe division for period
	with np.errstate(divide='ignore', invalid='ignore'):
		per_float = np.where(n_float != 0, 360.0 / n_float, 0.0)

	# Format derived values efficiently
	chunk['q'] = [expand_scientific_notation(x) for x in np.round(q_float, 8)]
	chunk['ad'] = [expand_scientific_notation(x) for x in np.round(ad_float, 8)]
	chunk['per'] = [expand_scientific_notation(x) for x in np.round(per_float, 2)]

	# 4. Hex Flag Decoding
	# Convert hex strings to int efficiently
	hex_series = chunk['hex_flags'].fillna('0000').astype(str)
	# Ensure 4 chars just in case
	hex_series = hex_series.where(hex_series.str.len() == 4, '0000')

	# Int conversion (map is often faster than list comp for Series operations)
	flags_int = hex_series.apply(lambda x: int(x, 16)).values.astype(np.int32)

	chunk['OrbitType'] = pd.Series(flags_int & MASK_ORBIT_TYPE).map(ORBIT_TYPES).fillna("").values
	chunk['is_neo_flag'] = ((flags_int & MASK_NEO) != 0).astype(int)
	chunk['is_pha_flag'] = ((flags_int & MASK_PHA) != 0).astype(int)

	# 5. Date Parsing
	chunk['epoch_iso'] = chunk['epoch'].map(unpack_packed_date)

	# 6. Tp Calculation
	epochs_dt = pd.to_datetime(chunk['epoch_iso'], errors='coerce')

	# Calculate offset days where possible
	offset_days = np.full_like(ma_float, np.nan)
	valid_n = n_float != 0
	offset_days[valid_n] = ma_float[valid_n] / n_float[valid_n]

	MAX_DAYS = 106000
	valid_mask = (np.abs(offset_days) <= MAX_DAYS) & pd.notna(epochs_dt) & pd.notna(offset_days)

	chunk['tp'] = ""

	# Fast path: Vectorized calculation
	if valid_mask.any():
		valid_offsets = pd.to_timedelta(offset_days[valid_mask], unit='D')
		valid_epochs = epochs_dt[valid_mask]
		chunk.loc[valid_mask, 'tp'] = (valid_epochs - valid_offsets).dt.strftime('%Y-%m-%d %H:%M:%S.%f').fillna("")

	# Fallback path: Manual calculation for edge cases
	has_data = (chunk['epoch_iso'] != "") & pd.notna(chunk['mean_anomaly']) & valid_n
	fallback_mask = (~valid_mask) & has_data

	if fallback_mask.any():
		chunk.loc[fallback_mask, 'tp'] = [
			calculate_tp(ep, ma, n_val)
			for ep, ma, n_val in zip(
				chunk.loc[fallback_mask, 'epoch_iso'],
				ma_float[fallback_mask],
				n_float[fallback_mask]
			)
		]

	# 7. Designation Parsing
	chunk['number_str'] = ""
	chunk['remainder'] = chunk['designation_full']

	# Extract number from parentheses
	is_numbered = chunk['designation_full'].str.startswith('(', na=False)
	if is_numbered.any():
		# Split on first ')'
		split = chunk.loc[is_numbered, 'designation_full'].str.split(')', n=1, expand=True)
		if not split.empty and split.shape[1] > 0:
			chunk.loc[is_numbered, 'number_str'] = split[0].str.replace('(', '', regex=False)
			if split.shape[1] > 1:
				chunk.loc[is_numbered, 'remainder'] = split[1].str.strip()

	# Determine Name vs Pdes based on digits
	has_digits = chunk['remainder'].str.contains(r'\d', regex=True).fillna(False)
	chunk['name_parsed'] = np.where(~has_digits, chunk['remainder'], "")
	chunk['pdes_parsed'] = np.where(has_digits, chunk['remainder'], "")

	# Cleanup temporary columns
	chunk.drop(columns=['remainder', 'designation_full', 'designation'], errors='ignore', inplace=True)

	return chunk


# --- Main Processor Class ---

class AsteroidProcessor:
	def __init__(self, input_path: str, output_dir: str):
		self.input_path = input_path
		self.output_dir = output_dir

		self.seen_ids: Set[str] = set()
		self.file_handles: Dict[str, Any] = {}

		# ID Counters
		self.next_software_id = 1
		self.next_astronomer_id = 1
		self.next_class_id = 1
		self.next_observation_id = 1
		self.next_orbit_id = 1
		self.next_asteroid_id = 1

		# Maps
		self.software_map: Dict[str, int] = {}
		self.astronomer_map: Dict[str, int] = {}
		self.class_map: Dict[str, int] = {}

	def _map_computers_and_astronomers(self, chunk: pd.DataFrame) -> None:
		chunk['id_soft'] = ""
		chunk['id_astro'] = ""

		entities = chunk['computer'].dropna().unique()

		for name in entities:
			name_str = str(name).strip()
			if not name_str:
				continue

			# Heuristic Check
			is_software = name_str.startswith(SOFTWARE_PREFIXES) or name_str in SOFTWARE_SPECIFIC_NAMES

			if is_software:
				if name_str not in self.software_map:
					self.software_map[name_str] = self.next_software_id
					self.next_software_id += 1
			else:
				if name_str not in self.astronomer_map:
					self.astronomer_map[name_str] = self.next_astronomer_id
					self.next_astronomer_id += 1

		# Map IDs
		chunk['id_soft'] = chunk['computer'].str.strip().map(self.software_map).fillna("").astype(str).str.replace(r'\.0$', '', regex=True)
		chunk['id_astro'] = chunk['computer'].str.strip().map(self.astronomer_map).fillna("").astype(str).str.replace(r'\.0$', '', regex=True)

	def _map_classes(self, chunk: pd.DataFrame) -> None:
		chunk['id_class'] = ""

		classes = chunk['OrbitType'].dropna().unique()
		for name in classes:
			name_str = str(name)
			if name_str and name_str not in self.class_map:
				self.class_map[name_str] = self.next_class_id
				self.next_class_id += 1

		chunk['id_class'] = chunk['OrbitType'].map(self.class_map).fillna("").astype(str).str.replace(r'\.0$', '', regex=True)

	def process(self):
		print(f"Reading {self.input_path}...")

		if not os.path.exists(self.input_path):
			print(f"Error: Input file '{self.input_path}' not found.")
			return

		ensure_directory(self.output_dir)

		try:
			# Initialize Output Files
			for filename, headers in SCHEMAS.items():
				path = os.path.join(self.output_dir, filename)
				self.file_handles[filename] = open(path, 'w', encoding='utf-8', newline='')
				pd.DataFrame(columns=headers).to_csv(self.file_handles[filename], index=False)

			start_time = time.time()
			total_records = 0
			max_workers = max(1, (os.cpu_count() or 2) - 1)

			print(f"Spinning up {max_workers} worker processes...")

			with pd.read_csv(self.input_path, chunksize=CHUNK_SIZE, dtype=MPCORB_DTYPES, on_bad_lines='skip', low_memory=False) as reader:
				with ProcessPoolExecutor(max_workers=max_workers) as executor:
					futures = []

					def handle_result(future):
						nonlocal total_records
						try:
							chunk = future.result()
							if chunk is None or chunk.empty:
								return

							# Filter duplicates based on 'obj_id'
							is_new = ~chunk['obj_id'].isin(self.seen_ids)
							chunk = chunk[is_new].copy()

							if chunk.empty:
								return

							self.seen_ids.update(chunk['obj_id'])

							# Reset index for alignment
							chunk.reset_index(drop=True, inplace=True)

							chunk_len = len(chunk)
							chunk['IDAsteroide'] = np.arange(self.next_asteroid_id, self.next_asteroid_id + chunk_len)
							chunk['IDOrbita'] = np.arange(self.next_orbit_id, self.next_orbit_id + chunk_len)

							self.next_asteroid_id += chunk_len
							self.next_orbit_id += chunk_len

							self._map_computers_and_astronomers(chunk)
							self._map_classes(chunk)
							self._write_tables(chunk)

							total_records += chunk_len
							print(f"Processed {total_records} records...", end='\r')

						except Exception as e:
							print(f"Error processing chunk: {e}")

					for raw_chunk in reader:
						future = executor.submit(process_chunk_worker, raw_chunk)
						futures.append(future)

						if len(futures) >= max_workers * 2:
							handle_result(futures.pop(0))

					for future in futures:
						handle_result(future)

			# Write Reference Tables
			self._write_reference_tables()

			end_time = time.time()
			print(f"\nDone! Processed {total_records} records in {end_time - start_time:.2f} seconds.")

		finally:
			for f in self.file_handles.values():
				f.close()
			self.file_handles.clear()

	def _write_reference_tables(self):
		# Software
		if self.software_map and 'mpcorb_software.csv' in self.file_handles:
			df = pd.DataFrame(list(self.software_map.items()), columns=['Nome', 'IDSoftware'])
			df[['IDSoftware', 'Nome']].to_csv(self.file_handles['mpcorb_software.csv'], mode='a', header=False, index=False)

		# Astronomers
		if self.astronomer_map and 'mpcorb_astronomers.csv' in self.file_handles:
			df = pd.DataFrame(list(self.astronomer_map.items()), columns=['Nome', 'IDAstronomo'])
			df['IDCentro'] = ""
			df[['IDAstronomo', 'Nome', 'IDCentro']].to_csv(self.file_handles['mpcorb_astronomers.csv'], mode='a', header=False, index=False)

		# Classes
		if self.class_map and 'mpcorb_classes.csv' in self.file_handles:
			df = pd.DataFrame(list(self.class_map.items()), columns=['Descricao', 'IDClasse'])
			df['CodClasse'] = df['Descricao']
			df[['IDClasse', 'Descricao', 'CodClasse']].to_csv(self.file_handles['mpcorb_classes.csv'], mode='a', header=False, index=False)

	def _write_tables(self, chunk):
		# Helper lambda to batch apply scientific notation expansion
		# Using map is cleaner than list comp for df assignment
		def expand_col(col_name):
			return chunk[col_name].map(expand_scientific_notation)

		# Asteroids
		df_ast = pd.DataFrame()
		df_ast['IDAsteroide'] = chunk['IDAsteroide']
		df_ast['number'] = chunk['number_str'].fillna("")
		df_ast['spkid'] = ""
		df_ast['pdes'] = chunk['pdes_parsed']
		df_ast['name'] = chunk['name_parsed']
		df_ast['prefix'] = ""
		df_ast['neo'] = chunk['is_neo_flag']
		df_ast['pha'] = chunk['is_pha_flag']
		df_ast['H'] = expand_col('abs_mag')
		df_ast['G'] = expand_col('slope_param')
		df_ast['diameter'] = ""
		df_ast['diameter_sigma'] = ""
		df_ast['albedo'] = ""
		df_ast.to_csv(self.file_handles['mpcorb_asteroids.csv'], mode='a', header=False, index=False)

		# Observations
		n_obs = len(chunk)
		df_obs = pd.DataFrame()
		df_obs['IDObservacao'] = np.arange(self.next_observation_id, self.next_observation_id + n_obs)
		self.next_observation_id += n_obs
		df_obs['IDAsteroide'] = chunk['IDAsteroide']
		df_obs['IDAstronomo'] = chunk['id_astro']
		df_obs['IDSoftware'] = chunk['id_soft']
		df_obs['IDEquipamento'] = ""
		df_obs['Data_atualizacao'] = chunk['epoch_iso']
		df_obs['Hora'] = ""
		df_obs['Duracao'] = ""
		df_obs['Modo'] = np.where(df_obs['IDSoftware'] != "", "Orbit Computation", np.where(df_obs['IDAstronomo'] != "", "Orbit Sighting", ""))
		df_obs = df_obs[SCHEMAS['mpcorb_observations.csv']]
		df_obs.to_csv(self.file_handles['mpcorb_observations.csv'], mode='a', header=False, index=False)

		# Orbits
		df_orb = pd.DataFrame()
		df_orb['IDOrbita'] = chunk['IDOrbita']
		df_orb['IDAsteroide'] = chunk['IDAsteroide']
		df_orb['epoch'] = chunk['epoch_iso']

		cols_to_expand = {
			'e': 'eccentricity', 'a': 'semi_major_axis', 'i': 'inclination',
			'om': 'long_asc_node', 'w': 'arg_perihelion', 'ma': 'mean_anomaly',
			'n': 'mean_motion', 'rms': 'rms_residual'
		}
		for target, source in cols_to_expand.items():
			df_orb[target] = expand_col(source)

		df_orb['tp'] = chunk['tp']
		df_orb['moid'] = ""
		df_orb['moid_ld'] = ""
		df_orb['q'] = chunk['q']
		df_orb['ad'] = chunk['ad']
		df_orb['per'] = chunk['per']

		df_orb['Arc'] = chunk['first_obs'].astype(str) + "-" + chunk['last_obs'].astype(str)

		empty_cols = ['sigma_e', 'sigma_a', 'sigma_q', 'sigma_i', 'sigma_om', 'sigma_w',
					  'sigma_ma', 'sigma_ad', 'sigma_n', 'sigma_tp', 'sigma_per']
		for col in empty_cols:
			df_orb[col] = ""

		df_orb['uncertainty'] = chunk['uncertainty']
		df_orb['Reference'] = chunk['reference']

		# Handle numerical columns safely
		df_orb['Num_Obs'] = pd.to_numeric(chunk['num_observations'], errors='coerce').fillna(-1).astype(int).astype(str).replace('-1', '')
		df_orb['Num_Opp'] = pd.to_numeric(chunk['num_oppositions'], errors='coerce').fillna(-1).astype(int).astype(str).replace('-1', '')

		df_orb['Coarse_Perts'] = chunk['coarse_perturbers']
		df_orb['Precise_Perts'] = chunk['precise_perturbers']
		df_orb['IDClasse'] = chunk['id_class']

		df_orb = df_orb[SCHEMAS['mpcorb_orbits.csv']]
		df_orb.to_csv(self.file_handles['mpcorb_orbits.csv'], mode='a', header=False, index=False)