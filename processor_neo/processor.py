import os
import time
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, Any

from .config import SCHEMAS, CHUNK_SIZE, NEO_DTYPES
from .utils import ensure_directory, expand_scientific_notation

# --- Worker Function ---

def process_chunk_worker(chunk: pd.DataFrame) -> pd.DataFrame:
	"""
	Worker function for processing a chunk of NEO data.
	"""
	# 1. Basic Cleaning
	chunk = chunk.dropna(subset=['id']).copy()
	if chunk.empty:
		return chunk

	# 2. Date Parsing (Vectorized)
	date_configs = [
		('epoch_cal', 'epoch_iso', '%Y-%m-%d'),             # Date only for Epoch
		('tp_cal', 'tp_iso', '%Y-%m-%d %H:%M:%S.%f')        # Full precision for Tp
	]

	for col_src, col_dest, date_format in date_configs:
		s = chunk[col_src].astype(str).str.strip()

		# Mask valid entries to avoid useless processing
		mask = (s != 'nan') & (s != '') & (s != '<NA>')

		chunk[col_dest] = ""
		if not mask.any():
			continue

		subset = s[mask]
		split = subset.str.split('.', n=1, expand=True)
		base = split[0]

		# Convert base date
		dt_series = pd.to_datetime(base, format='%Y%m%d', errors='coerce')

		# Handle fractional days if present
		if split.shape[1] > 1:
			frac = split[1]
			has_frac = frac.notna() & (frac != '')
			if has_frac.any():
				# We need float math here, but it's isolated to specific rows
				frac_vals = ("0." + frac[has_frac]).astype(float)
				dt_series.loc[has_frac] += pd.to_timedelta(frac_vals, unit='D')

		chunk.loc[mask, col_dest] = dt_series.dt.strftime(date_format).fillna("")

	# 3. Boolean Flags (Vectorized)
	chunk['neo_flag'] = np.where(chunk['neo'].fillna('N') == 'Y', '1', '0')
	chunk['pha_flag'] = np.where(chunk['pha'].fillna('N') == 'Y', '1', '0')

	# 4. String Cleaning
	chunk['prefix_clean'] = chunk['prefix'].fillna("").astype(str).str.strip()
	chunk['spkid_clean'] = chunk['spkid'].fillna("").astype(str).str.strip()

	# Clean Class Code
	chunk['class_clean'] = chunk['class'].astype(str).str.strip()
	# Vectorized replace of invalid values with NaN
	chunk['class_clean'] = chunk['class_clean'].replace(['nan', '', '<NA>'], np.nan)

	# Clean Class Description
	chunk['class_desc_clean'] = chunk['class_description'].astype(str).str.strip()
	chunk['class_desc_clean'] = (
		chunk['class_desc_clean']
		.str.replace(',', ';', regex=False)
		.str.replace('–', '-', regex=False)
		.str.replace('—', '-', regex=False)
		.replace(['nan', '', '<NA>'], np.nan)
	)

	# 5. Advanced Designation Parsing
	chunk['full_name_clean'] = chunk['full_name'].fillna("").astype(str).str.strip()
	chunk['id_str'] = chunk['id'].fillna("").astype(str).str.strip()
	chunk['name_clean'] = chunk['name'].fillna("").astype(str).str.strip() if 'name' in chunk.columns else ""

	# Identify Types
	is_numbered = chunk['id_str'].str.startswith('a', na=False)
	is_unnumbered = chunk['id_str'].str.startswith('b', na=False)

	chunk['number_clean'] = ""
	chunk['pdes_clean'] = ""

	# --- Numbered Asteroids ---
	if is_numbered.any():
		chunk.loc[is_numbered, 'number_clean'] = chunk.loc[is_numbered, 'id_str'].str[1:].str.lstrip('0')

		# Pdes: Extract from Full Name
		pattern_pdes = r'\s+\((?P<pdes>[^\)]+)\)$'
		# Apply extraction only to rows that are numbered
		extracted = chunk.loc[is_numbered, 'full_name_clean'].str.extract(pattern_pdes)

		# mask_found indices match the indices of 'extracted', which are the indices where is_numbered is True
		mask_found = extracted['pdes'].notna()

		if mask_found.any():
			# We get the specific indices where a pdes was found
			valid_indices = mask_found[mask_found].index
			# Assign directly using these indices.
			# extracted.loc[mask_found, 'pdes'] aligns perfectly with valid_indices
			chunk.loc[valid_indices, 'pdes_clean'] = extracted.loc[mask_found, 'pdes'].str.strip()

	# --- Unnumbered Asteroids ---
	if is_unnumbered.any():
		if 'pdes' in chunk.columns:
			chunk.loc[is_unnumbered, 'pdes_clean'] = chunk.loc[is_unnumbered, 'pdes'].fillna("").astype(str).str.strip()
		else:
			chunk.loc[is_unnumbered, 'pdes_clean'] = chunk.loc[is_unnumbered, 'full_name_clean']

	moid_ld_numeric = pd.to_numeric(chunk['moid_ld'], errors='coerce').fillna(0)
	moid_str = chunk['moid'].fillna("").astype(str).str.strip()
	moid_is_empty = (moid_str == "") | (moid_str.str.lower() == "nan") | (moid_str.str.lower() == "<na>")
	mask_invalid_moid = (moid_ld_numeric == 0) & moid_is_empty

	if mask_invalid_moid.any():
		chunk.loc[mask_invalid_moid, 'moid_ld'] = ""

	return chunk

# --- Main Processor Class ---

class AsteroidProcessor:
	def __init__(self, input_path: str, output_dir: str):
		self.input_path = input_path
		self.output_dir = output_dir

		self.next_asteroid_id = 1
		self.next_orbit_id = 1
		self.next_observation_id = 1
		self.next_class_id = 1

		self.class_map: Dict[str, int] = {}
		self.class_desc_map: Dict[str, str] = {}
		self.file_handles: Dict[str, Any] = {}

	def _map_classes(self, chunk: pd.DataFrame) -> None:
		chunk['id_class'] = ""

		# Efficient Unique Check
		unique_pairs = chunk[['class_clean', 'class_desc_clean']].drop_duplicates('class_clean')

		for _, row in unique_pairs.iterrows():
			code = row['class_clean']
			desc = row['class_desc_clean']

			if pd.isna(code):
				continue

			if code not in self.class_map:
				self.class_map[code] = self.next_class_id
				self.class_desc_map[code] = desc if not pd.isna(desc) else code
				self.next_class_id += 1

		chunk['id_class'] = chunk['class_clean'].map(self.class_map).fillna("").astype(str).str.replace(r'\.0$', '', regex=True)

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

			with pd.read_csv(self.input_path, sep=';', chunksize=CHUNK_SIZE,
							 dtype=NEO_DTYPES, on_bad_lines='skip', low_memory=False) as reader:

				with ProcessPoolExecutor(max_workers=max_workers) as executor:
					futures = []

					def handle_result(future):
						nonlocal total_records
						try:
							chunk = future.result()
							if chunk is None or chunk.empty: return

							chunk.reset_index(drop=True, inplace=True)

							chunk_len = len(chunk)
							chunk['IDAsteroide'] = np.arange(self.next_asteroid_id, self.next_asteroid_id + chunk_len)
							chunk['IDOrbita'] = np.arange(self.next_orbit_id, self.next_orbit_id + chunk_len)

							self.next_asteroid_id += chunk_len
							self.next_orbit_id += chunk_len

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

			# Write Classes Table
			if self.class_map and 'neo_classes.csv' in self.file_handles:
				class_data = [
					{'IDClasse': v, 'Descricao': self.class_desc_map.get(k, k), 'CodClasse': k}
					for k, v in self.class_map.items()
				]
				df_class = pd.DataFrame(class_data)
				df_class[['IDClasse', 'Descricao', 'CodClasse']].to_csv(
					self.file_handles['neo_classes.csv'], mode='a', header=False, index=False
				)

			end_time = time.time()
			print(f"\nDone! Processed {total_records} records in {end_time - start_time:.2f} seconds.")

		finally:
			for f in self.file_handles.values():
				f.close()
			self.file_handles.clear()

	def _write_tables(self, chunk):
		# Helper for map
		def expand_col(col_name):
			return chunk[col_name].map(expand_scientific_notation)

		# Asteroids
		df_ast = pd.DataFrame()
		df_ast['IDAsteroide'] = chunk['IDAsteroide']
		df_ast['number'] = chunk['number_clean']
		df_ast['spkid'] = chunk['spkid_clean']
		df_ast['pdes'] = chunk['pdes_clean']
		df_ast['name'] = chunk['name_clean']
		df_ast['prefix'] = chunk['prefix_clean']
		df_ast['neo'] = chunk['neo_flag']
		df_ast['pha'] = chunk['pha_flag']

		df_ast['H'] = expand_col('h')
		df_ast['G'] = ""
		df_ast['diameter'] = expand_col('diameter')
		df_ast['diameter_sigma'] = expand_col('diameter_sigma')
		df_ast['albedo'] = expand_col('albedo')

		df_ast.to_csv(self.file_handles['neo_asteroids.csv'], mode='a', header=False, index=False)

		# Orbits
		df_orb = pd.DataFrame()
		df_orb['IDOrbita'] = chunk['IDOrbita']
		df_orb['IDAsteroide'] = chunk['IDAsteroide']
		df_orb['epoch'] = chunk['epoch_iso']

		pass_through_cols = {
			'e': 'e', 'a': 'a', 'i': 'i', 'om': 'om', 'w': 'w',
			'ma': 'ma', 'n': 'n', 'q': 'q', 'ad': 'ad',
			'per': 'per', 'rms': 'rms', 'moid': 'moid', 'moid_ld': 'moid_ld'
		}
		for target, source in pass_through_cols.items():
			df_orb[target] = expand_col(source)

		df_orb['tp'] = chunk['tp_iso']
		df_orb['Arc'] = ""

		sigma_cols = [
			'sigma_e', 'sigma_a', 'sigma_q', 'sigma_i', 'sigma_om', 'sigma_w',
			'sigma_ma', 'sigma_ad', 'sigma_n', 'sigma_tp', 'sigma_per'
		]
		for col in sigma_cols:
			df_orb[col] = expand_col(col)

		# Empty/Default columns
		df_orb['Hex_Flags'] = ""
		df_orb['Is1kmNEO'] = ""
		df_orb['IsCriticalList'] = ""
		df_orb['IsOneOppositionEarlier'] = ""
		df_orb['uncertainty'] = ""
		df_orb['Reference'] = ""
		df_orb['Num_Obs'] = ""
		df_orb['Num_Opp'] = ""
		df_orb['Coarse_Perts'] = ""
		df_orb['Precise_Perts'] = ""

		df_orb['IDClasse'] = chunk['id_class']

		df_orb = df_orb[SCHEMAS['neo_orbits.csv']]
		df_orb.to_csv(self.file_handles['neo_orbits.csv'], mode='a', header=False, index=False)

		# Observations
		n_obs = len(chunk)
		df_obs = pd.DataFrame()
		df_obs['IDObservacao'] = np.arange(self.next_observation_id, self.next_observation_id + n_obs)
		self.next_observation_id += n_obs
		df_obs['IDAsteroide'] = chunk['IDAsteroide']
		df_obs['IDAstronomo'] = ""
		df_obs['IDSoftware'] = ""
		df_obs['IDEquipamento'] = ""
		df_obs['Data_atualizacao'] = chunk['epoch_iso']
		df_obs['Hora'] = ""
		df_obs['Duracao'] = ""
		df_obs['Modo'] = ""

		df_obs = df_obs[SCHEMAS['neo_observations.csv']]
		df_obs.to_csv(self.file_handles['neo_observations.csv'], mode='a', header=False, index=False)