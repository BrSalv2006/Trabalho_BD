import os
import pandas as pd
import numpy as np
from typing import Optional
from merger.config import DIR_MPC, DIR_NEO, OUTPUT_DIR, FILES, INPUT_MAP_MPC, INPUT_MAP_NEO

class DataMerger:
	def __init__(self):
		self.mpc_id_map: Optional[pd.Series] = None
		self.neo_id_map: Optional[pd.Series] = None
		self.mpc_class_map: Optional[pd.Series] = None
		self.neo_class_map: Optional[pd.Series] = None

	def _create_match_key(self, df: pd.DataFrame) -> np.ndarray:
		"""
		Creates a single normalized column for matching duplicates using vectorization.
		Priority: Number > Pdes (Provisional Desig) > Name
		"""
		number = df['number'].fillna("").astype(str).str.strip().str.lstrip('0')
		pdes = df['pdes'].fillna("").astype(str).str.strip().str.upper()
		name = df['name'].fillna("").astype(str).str.strip().str.upper()

		has_number = number != ""
		has_pdes = pdes != ""

		conditions = [has_number, has_pdes]
		choices = ["NUM_" + number, "DES_" + pdes]
		default = "NAM_" + name

		return np.select(conditions, choices, default=default)

	def _read_csv_safe(self, path: str, dtype: str = 'string') -> pd.DataFrame:
		if os.path.exists(path):
			return pd.read_csv(path, dtype=dtype, low_memory=False)
		return pd.DataFrame()

	def merge_classes(self):
		print("Merging Classes tables...")

		df_mpc = self._read_csv_safe(os.path.join(DIR_MPC, INPUT_MAP_MPC['classes']))
		df_neo = self._read_csv_safe(os.path.join(DIR_NEO, INPUT_MAP_NEO['classes']))

		# Concatenate and Dedup
		all_classes = pd.concat([df_mpc, df_neo], ignore_index=True)
		all_classes = all_classes[all_classes['CodClasse'].notna() & (all_classes['CodClasse'] != "")].copy()

		# Deduplicate keeping first occurrence
		unique_classes = all_classes.drop_duplicates(subset=['CodClasse']).reset_index(drop=True)
		unique_classes['New_ID'] = np.arange(1, len(unique_classes) + 1).astype(str)

		# Build ID Maps
		if not df_mpc.empty:
			merged = df_mpc.merge(unique_classes[['CodClasse', 'New_ID']], on='CodClasse', how='left')
			self.mpc_class_map = merged.set_index('IDClasse')['New_ID']

		if not df_neo.empty:
			merged = df_neo.merge(unique_classes[['CodClasse', 'New_ID']], on='CodClasse', how='left')
			self.neo_class_map = merged.set_index('IDClasse')['New_ID']

		out_df = unique_classes[['New_ID', 'Descricao', 'CodClasse']].rename(columns={'New_ID': 'IDClasse'})
		self._save(out_df, FILES['classes'])
		print(f"Merged Classes saved: {len(out_df)}")

	def _normalize_identifiers(self, df1: pd.DataFrame, df2: pd.DataFrame):
		"""
		Fills missing 'number' values in both dataframes by matching 'pdes'.
		"""
		# Create temporary clean columns for matching
		pdes_col = '_temp_pdes'
		num_col = '_temp_number'

		for df in [df1, df2]:
			df[pdes_col] = df['pdes'].fillna("").astype(str).str.strip().str.upper()
			df[num_col] = df['number'].fillna("").astype(str).str.strip().str.lstrip('0')

		# Build mapping from both, prioritizing df1 (MPC)
		combined = pd.concat([df1[[pdes_col, num_col]], df2[[pdes_col, num_col]]])

		# Valid: pdes not empty, number not empty
		valid = combined[(combined[pdes_col] != "") & (combined[num_col] != "")]

		# pdes -> number map
		pdes_map = valid.drop_duplicates(subset=[pdes_col]).set_index(pdes_col)[num_col].to_dict()

		# Apply map
		def apply_map(df):
			# Mask: number is empty AND pdes is in map
			mask = (df[num_col] == "") & (df[pdes_col].isin(pdes_map))
			if mask.any():
				df.loc[mask, 'number'] = df.loc[mask, pdes_col].map(pdes_map)

			return df.drop(columns=[pdes_col, num_col])

		df1 = apply_map(df1)
		df2 = apply_map(df2)

		return df1, df2

	def merge_asteroids(self):
		print("Merging Asteroids tables...")

		path_mpc = os.path.join(DIR_MPC, INPUT_MAP_MPC['asteroids'])
		if not os.path.exists(path_mpc):
			raise FileNotFoundError(f"Missing primary dataset: {path_mpc}")

		df_mpc = pd.read_csv(path_mpc, dtype=str, low_memory=False)
		df_neo = self._read_csv_safe(os.path.join(DIR_NEO, INPUT_MAP_NEO['asteroids']))

		# Normalize columns if missing in NEO
		if df_neo.empty:
			df_neo = pd.DataFrame(columns=df_mpc.columns)

		df_mpc, df_neo = self._normalize_identifiers(df_mpc, df_neo)

		# Vectorized Key Generation
		df_mpc['match_key'] = self._create_match_key(df_mpc)
		df_neo['match_key'] = self._create_match_key(df_neo)

		# Filter out orphans with no identifiers
		df_mpc = df_mpc[df_mpc['match_key'] != "NAM_"]
		df_neo = df_neo[df_neo['match_key'] != "NAM_"]

		df_mpc = df_mpc.set_index('match_key')
		df_neo = df_neo.set_index('match_key')

		print(f"MPC Unique: {len(df_mpc)}, NEO Unique: {len(df_neo)}")

		# combine_first is powerful: MPC is primary, fills holes with NEO
		df_merged = df_mpc.combine_first(df_neo).reset_index()
		df_merged['New_IDAsteroide'] = np.arange(1, len(df_merged) + 1).astype(str)

		# Build Maps
		key_to_new_id = df_merged.set_index('match_key')['New_IDAsteroide']

		self.mpc_id_map = df_mpc.reset_index()[['IDAsteroide', 'match_key']].copy()
		self.mpc_id_map['New_ID'] = self.mpc_id_map['match_key'].map(key_to_new_id)
		self.mpc_id_map = self.mpc_id_map.set_index('IDAsteroide')['New_ID']

		self.neo_id_map = df_neo.reset_index()[['IDAsteroide', 'match_key']].copy()
		self.neo_id_map['New_ID'] = self.neo_id_map['match_key'].map(key_to_new_id)
		self.neo_id_map = self.neo_id_map.set_index('IDAsteroide')['New_ID']

		df_merged['IDAsteroide'] = df_merged['New_IDAsteroide']
		df_merged.drop(columns=['match_key', 'New_IDAsteroide'], inplace=True)

		self._save(df_merged, FILES['asteroids'])
		print(f"Merged Asteroids saved: {len(df_merged)}")

	def _update_ids(self, df: pd.DataFrame, id_map: pd.Series, col: str = 'IDAsteroide') -> pd.DataFrame:
		if df.empty or id_map is None or col not in df.columns:
			return df
		# fillna with original values to preserve data consistency even if map fails
		df[col] = df[col].map(id_map).fillna(df[col])
		return df

	def merge_orbits(self):
		print("Merging Orbits tables...")

		df_mpc = self._read_csv_safe(os.path.join(DIR_MPC, INPUT_MAP_MPC['orbits']))
		df_mpc = self._update_ids(df_mpc, self.mpc_id_map)

		if self.mpc_class_map is not None and 'IDClasse' in df_mpc.columns:
			df_mpc['IDClasse'] = df_mpc['IDClasse'].map(self.mpc_class_map).fillna(df_mpc['IDClasse'])

		df_neo = self._read_csv_safe(os.path.join(DIR_NEO, INPUT_MAP_NEO['orbits']))
		if not df_neo.empty:
			df_neo = self._update_ids(df_neo, self.neo_id_map)
			if self.neo_class_map is not None and 'IDClasse' in df_neo.columns:
				df_neo['IDClasse'] = df_neo['IDClasse'].map(self.neo_class_map).fillna(df_neo['IDClasse'])

		# Drop old IDs before concat
		df_merged = pd.concat([
			df_mpc.drop(columns=['IDOrbita'], errors='ignore'),
			df_neo.drop(columns=['IDOrbita'], errors='ignore')
		], ignore_index=True)

		# Deduplicate based on Asteroid ID and Epoch
		if not df_merged.empty:
			df_merged = df_merged.groupby(['IDAsteroide', 'epoch'], as_index=False).first()
			df_merged['IDOrbita'] = np.arange(1, len(df_merged) + 1).astype(str)

			# Reorder
			cols = ['IDOrbita'] + [c for c in df_merged.columns if c != 'IDOrbita']
			df_merged = df_merged[cols]

		self._save(df_merged, FILES['orbits'])
		print(f"Merged Orbits saved: {len(df_merged)}")

	def merge_observations(self):
		print("Merging Observations tables...")

		df_mpc = self._read_csv_safe(os.path.join(DIR_MPC, INPUT_MAP_MPC['observations']))
		df_mpc = self._update_ids(df_mpc, self.mpc_id_map)

		df_neo = self._read_csv_safe(os.path.join(DIR_NEO, INPUT_MAP_NEO['observations']))
		df_neo = self._update_ids(df_neo, self.neo_id_map)

		df_merged = pd.concat([df_mpc, df_neo], ignore_index=True)
		if not df_merged.empty:
			df_merged['IDObservacao'] = np.arange(1, len(df_merged) + 1).astype(str)

		self._save(df_merged, FILES['observations'])
		print(f"Merged Observations saved: {len(df_merged)}")

	def copy_references(self):
		print("Copying Reference tables...")
		for key in ['software', 'astronomers']:
			src = os.path.join(DIR_MPC, INPUT_MAP_MPC[key])
			df = self._read_csv_safe(src)
			if not df.empty:
				if key == 'astronomers':
					df['IDCentro'] = '1'
				self._save(df, FILES[key])

	def _save(self, df: pd.DataFrame, filename: str):
		if not os.path.exists(OUTPUT_DIR):
			os.makedirs(OUTPUT_DIR, exist_ok=True)
		path = os.path.join(OUTPUT_DIR, filename)
		df.to_csv(path, index=False, na_rep='')

	def run(self):
		self.merge_classes()
		self.merge_asteroids()
		self.merge_orbits()
		self.merge_observations()
		self.copy_references()
		print("Merge Pipeline Complete!")