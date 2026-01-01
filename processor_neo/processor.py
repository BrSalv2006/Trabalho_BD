import os
import time
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from typing import Dict

from .config import SCHEMAS, INPUT_FILE, CHUNK_SIZE, NEO_DTYPES
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
		mask = (s != 'nan') & (s != '') & (s != '<NA>')

		chunk[col_dest] = ""

		if not mask.any():
			continue

		subset = s[mask]
		split = subset.str.split('.', n=1, expand=True)
		base = split[0]

		dt_series = pd.to_datetime(base, format='%Y%m%d', errors='coerce')

		if split.shape[1] > 1:
			frac = split[1]
			has_frac = frac.notna() & (frac != '')
			if has_frac.any():
				# We do need float math here for date calculation, but it's isolated
				frac_vals = ("0." + frac[has_frac]).astype(float)
				dt_series.loc[has_frac] += pd.to_timedelta(frac_vals, unit='D')

		chunk.loc[mask, col_dest] = dt_series.dt.strftime(date_format).fillna("")

	# 3. Boolean Flags (Y/N -> 1/0)
	chunk['neo_flag'] = np.where(chunk['neo'].fillna('N') == 'Y', '1', '0')
	chunk['pha_flag'] = np.where(chunk['pha'].fillna('N') == 'Y', '1', '0')

	# 4. String Cleaning
	chunk['prefix_clean'] = chunk['prefix'].fillna("").astype(str).str.strip()
	chunk['spkid_clean'] = chunk['spkid'].fillna("").astype(str).str.strip()

	# Clean Class Code and Class Description
	chunk['class_clean'] = chunk['class'].astype(str).str.strip()
	chunk.loc[chunk['class_clean'].isin(['nan', '', '<NA>']), 'class_clean'] = np.nan

	chunk['class_desc_clean'] = chunk['class_description'].astype(str).str.strip()
	chunk['class_desc_clean'] = chunk['class_desc_clean'].str.replace(',', ';', regex=False)
	chunk['class_desc_clean'] = chunk['class_desc_clean'].str.replace('–', '-', regex=False).str.replace('—', '-', regex=False)
	chunk.loc[chunk['class_desc_clean'].isin(['nan', '', '<NA>']), 'class_desc_clean'] = np.nan

	# 5. Advanced Designation Parsing

	# Prepare Source Columns
	chunk['full_name_clean'] = chunk['full_name'].fillna("").astype(str).str.strip()
	chunk['id_str'] = chunk['id'].fillna("").astype(str).str.strip()

	# Name: Always use the 'name' column as the primary source for the Name field
	if 'name' in chunk.columns:
		chunk['name_clean'] = chunk['name'].fillna("").astype(str).str.strip()
	else:
		chunk['name_clean'] = ""

	# Identify Types
	is_numbered = chunk['id_str'].str.startswith('a', na=False)
	is_unnumbered = chunk['id_str'].str.startswith('b', na=False)

	chunk['number_clean'] = ""
	chunk['pdes_clean'] = ""

	# --- Logic for Numbered Asteroids (ID starts with 'a') ---
	if is_numbered.any():
		# 1. Number: Parse from ID (remove 'a' and leading zeros)
		chunk.loc[is_numbered, 'number_clean'] = chunk.loc[is_numbered, 'id_str'].str[1:].str.lstrip('0')

		# 2. Pdes: Extract from Full Name if present in parentheses at the end
		# Example: "28394 Mittag-Leffler (1999 RY36)" -> Pdes is "1999 RY36"
		pattern_pdes = r'\s+\((?P<pdes>[^\)]+)\)$'

		extracted = chunk.loc[is_numbered, 'full_name_clean'].str.extract(pattern_pdes)
		mask_found = extracted['pdes'].notna()

		if mask_found.any():
			chunk.loc[mask_found.index[mask_found], 'pdes_clean'] = extracted.loc[mask_found, 'pdes'].str.strip()

			# Fallback for Numbered: If no parens found, check if remainder has digits (like MPC logic)
			# This handles edge cases where full_name might lack parens but still have pdes info.
			# However, since we prioritize extraction, we leave pdes_clean empty if not found,
			# unless we want to try using the 'pdes' column as a fallback?
			# For now, adhering to strict extraction from full_name as primary for Numbered.

	# --- Logic for Unnumbered Asteroids (ID starts with 'b') ---
	if is_unnumbered.any():
		# Number is empty

		# Pdes: Use the 'pdes' column directly as requested
		if 'pdes' in chunk.columns:
			chunk.loc[is_unnumbered, 'pdes_clean'] = chunk.loc[is_unnumbered, 'pdes'].fillna("").astype(str).str.strip()
		else:
			# Fallback if pdes column is missing (defensive)
			chunk.loc[is_unnumbered, 'pdes_clean'] = chunk.loc[is_unnumbered, 'full_name_clean']

	return chunk

# --- Main Processor Class ---

class AsteroidProcessor:
	"""
	Processor for the NEO dataset.
	"""
	def __init__(self, input_path: str, output_dir: str):
		self.input_path = input_path
		self.output_dir = output_dir
		self.next_asteroid_id = 1
		self.next_orbit_id = 1

		# Observation ID counter
		self.next_observation_id = 1

		# Class Map (Code -> ID)
		self.class_map: Dict[str, int] = {}
		# Description Map (Code -> Description)
		self.class_desc_map: Dict[str, str] = {}
		self.next_class_id = 1

		self.file_handles = {}

	def _map_classes(self, chunk: pd.DataFrame) -> None:
		"""
		Maps Class string to Class IDs and captures descriptions.
		"""
		chunk['id_class'] = ""

		# 1. Update Maps
		unique_pairs = chunk[['class_clean', 'class_desc_clean']].drop_duplicates('class_clean')

		for _, row in unique_pairs.iterrows():
			code = row['class_clean']
			desc = row['class_desc_clean']

			if pd.isna(code):
				continue

			if code not in self.class_map:
				self.class_map[code] = self.next_class_id
				final_desc = desc if not pd.isna(desc) else code
				self.class_desc_map[code] = final_desc
				self.next_class_id += 1

		# 2. Vectorized Assignment
		class_series = chunk['class_clean'].map(self.class_map)
		mask_class = class_series.notna()
		chunk.loc[mask_class, 'id_class'] = class_series[mask_class].astype(int).astype(str)

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
				f = open(path, 'w', encoding='utf-8', newline='')
				self.file_handles[filename] = f
				pd.DataFrame(columns=headers).to_csv(f, index=False)

			start_time = time.time()
			total_records = 0

			# Use max cores
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

							# --- FIX: Reset index to ensure alignment with output dataframes ---
							chunk.reset_index(drop=True, inplace=True)
							# -----------------------------------------------------------------

							chunk_len = len(chunk)
							chunk['IDAsteroide'] = range(self.next_asteroid_id, self.next_asteroid_id + chunk_len)
							chunk['IDOrbita'] = range(self.next_orbit_id, self.next_orbit_id + chunk_len)

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
							done_future = futures.pop(0)
							handle_result(done_future)

					for future in futures:
						handle_result(future)

			# Write Classes Table
			if self.class_map and 'neo_classes.csv' in self.file_handles:
				class_data = []
				for code, id_val in self.class_map.items():
					desc = self.class_desc_map.get(code, code)
					class_data.append({
						'IDClasse': id_val,
						'Descricao': desc,
						'CodClasse': code
					})

				df_class = pd.DataFrame(class_data)
				df_class[['IDClasse', 'Descricao', 'CodClasse']].to_csv(
					self.file_handles['neo_classes.csv'],
					mode='a', header=False, index=False
				)

			end_time = time.time()
			print(f"\nDone! Processed {total_records} records in {end_time - start_time:.2f} seconds.")

		finally:
			for f in self.file_handles.values():
				f.close()
			self.file_handles.clear()

	def _write_tables(self, chunk):
		"""Maps processed chunk data to the specific output CSVs."""

		# neo_asteroids.csv
		df_ast = pd.DataFrame()
		df_ast['IDAsteroide'] = chunk['IDAsteroide']
		df_ast['number'] = chunk['number_clean']
		df_ast['spkid'] = chunk['spkid_clean']
		df_ast['pdes'] = chunk['pdes_clean']
		df_ast['name'] = chunk['name_clean']
		df_ast['prefix'] = chunk['prefix_clean']
		df_ast['neo'] = chunk['neo_flag']
		df_ast['pha'] = chunk['pha_flag']

		# Expand Scientific Notation for Float Fields
		df_ast['H'] = [expand_scientific_notation(x) for x in chunk['h']]
		df_ast['G'] = ""
		df_ast['diameter'] = [expand_scientific_notation(x) for x in chunk['diameter']]
		df_ast['diameter_sigma'] = [expand_scientific_notation(x) for x in chunk['diameter_sigma']]
		df_ast['albedo'] = [expand_scientific_notation(x) for x in chunk['albedo']]

		df_ast.to_csv(self.file_handles['neo_asteroids.csv'], mode='a', header=False, index=False)

		# neo_orbits.csv
		df_orb = pd.DataFrame()
		df_orb['IDOrbita'] = chunk['IDOrbita']
		df_orb['IDAsteroide'] = chunk['IDAsteroide']
		df_orb['epoch'] = chunk['epoch_iso']

		# Map simple columns (Use expand_scientific_notation for all floats)
		pass_through_cols = {
			'e': 'e', 'a': 'a', 'i': 'i', 'om': 'om', 'w': 'w',
			'ma': 'ma', 'n': 'n', 'q': 'q', 'ad': 'ad',
			'per': 'per', 'rms': 'rms', 'moid': 'moid', 'moid_ld': 'moid_ld'
		}
		for target, source in pass_through_cols.items():
			df_orb[target] = [expand_scientific_notation(x) for x in chunk[source]]

		df_orb['tp'] = chunk['tp_iso']
		df_orb['Arc'] = ""

		# Map Sigma columns (Present in NEO)
		sigma_cols = {
			'sigma_e': 'sigma_e', 'sigma_a': 'sigma_a', 'sigma_q': 'sigma_q',
			'sigma_i': 'sigma_i', 'sigma_om': 'sigma_om', 'sigma_w': 'sigma_w',
			'sigma_ma': 'sigma_ma', 'sigma_ad': 'sigma_ad', 'sigma_n': 'sigma_n',
			'sigma_tp': 'sigma_tp', 'sigma_per': 'sigma_per'
		}
		for target, source in sigma_cols.items():
			df_orb[target] = [expand_scientific_notation(x) for x in chunk[source]]

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

		# Link to Class Table
		df_orb['IDClasse'] = chunk['id_class']

		# Enforce Correct Column Order
		df_orb = df_orb[SCHEMAS['neo_orbits.csv']]

		df_orb.to_csv(self.file_handles['neo_orbits.csv'], mode='a', header=False, index=False)

		# neo_observations.csv
		df_obs = pd.DataFrame()
		n_obs = len(chunk)
		df_obs['IDObservacao'] = range(self.next_observation_id, self.next_observation_id + n_obs)
		self.next_observation_id += n_obs
		df_obs['IDAsteroide'] = chunk['IDAsteroide']
		df_obs['IDAstronomo'] = ""
		df_obs['IDSoftware'] = ""
		df_obs['IDEquipamento'] = ""
		df_obs['Data_atualizacao'] = chunk['epoch_iso']
		df_obs['Hora'] = ""
		df_obs['Duracao'] = ""
		df_obs['Modo'] = ""

		# Ensure correct order
		df_obs = df_obs[SCHEMAS['neo_observations.csv']]

		df_obs.to_csv(self.file_handles['neo_observations.csv'], mode='a', header=False, index=False)