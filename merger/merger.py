import os
import pandas as pd
import numpy as np
from .config import DIR_MPC, DIR_NEO, OUTPUT_DIR, FILES, INPUT_MAP_MPC, INPUT_MAP_NEO

class DataMerger:
	def __init__(self):
		self.mpc_id_map = None
		self.neo_id_map = None
		self.mpc_class_map = None
		self.neo_class_map = None

	def _create_match_key(self, df):
		"""Vectorized key generation for duplication matching."""
		number = df['number'].fillna("").astype(str).str.strip().str.lstrip('0')
		pdes = df['pdes'].fillna("").astype(str).str.strip().str.upper()
		name = df['name'].fillna("").astype(str).str.strip().str.upper()

		conditions = [number != "", pdes != ""]
		choices = ["NUM_" + number, "DES_" + pdes]
		default = "NAM_" + name
		return np.select(conditions, choices, default=default)

	def _save(self, df, filename):
		if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
		df.to_csv(os.path.join(OUTPUT_DIR, filename), index=False, na_rep='')

	def merge_classes(self):
		print("Merging Classes...")
		# Read
		df_mpc = self._read_csv(DIR_MPC, INPUT_MAP_MPC['classes'], ['IDClasse', 'Descricao', 'CodClasse'])
		df_neo = self._read_csv(DIR_NEO, INPUT_MAP_NEO['classes'], ['IDClasse', 'Descricao', 'CodClasse'])

		# Combine
		all_classes = pd.concat([df_mpc, df_neo], ignore_index=True)
		all_classes = all_classes[all_classes['CodClasse'].notna() & (all_classes['CodClasse'] != "")].copy()

		# Deduplicate
		unique_classes = all_classes.drop_duplicates(subset=['CodClasse']).reset_index(drop=True)
		unique_classes['New_ID'] = np.arange(1, len(unique_classes) + 1).astype(str)

		# Build Maps
		if not df_mpc.empty:
			self.mpc_class_map = df_mpc.merge(unique_classes, on='CodClasse', how='left').set_index('IDClasse_x')['New_ID']
		if not df_neo.empty:
			self.neo_class_map = df_neo.merge(unique_classes, on='CodClasse', how='left').set_index('IDClasse_x')['New_ID']

		out_df = unique_classes[['New_ID', 'Descricao', 'CodClasse']].rename(columns={'New_ID': 'IDClasse'})
		self._save(out_df, FILES['classes'])
		print(f"  Saved {len(out_df)} classes.")

	def merge_asteroids(self):
		print("Merging Asteroids...")
		df_mpc = self._read_csv(DIR_MPC, INPUT_MAP_MPC['asteroids'])
		df_neo = self._read_csv(DIR_NEO, INPUT_MAP_NEO['asteroids'])

		# Keys
		df_mpc['match_key'] = self._create_match_key(df_mpc)
		df_neo['match_key'] = self._create_match_key(df_neo)

		# Filter unnamed
		df_mpc = df_mpc[df_mpc['match_key'] != "NAM_"].set_index('match_key')
		df_neo = df_neo[df_neo['match_key'] != "NAM_"].set_index('match_key')

		print(f"  MPC: {len(df_mpc)}, NEO: {len(df_neo)}")

		# Combine First (fills nulls in MPC with values from NEO)
		df_merged = df_mpc.combine_first(df_neo).reset_index()
		df_merged['New_IDAsteroide'] = np.arange(1, len(df_merged) + 1).astype(str)

		# Map creation
		key_map = df_merged.set_index('match_key')['New_IDAsteroide']

		self.mpc_id_map = df_mpc.reset_index()[['IDAsteroide', 'match_key']].copy()
		self.mpc_id_map['New_ID'] = self.mpc_id_map['match_key'].map(key_map)
		self.mpc_id_map = self.mpc_id_map.set_index('IDAsteroide')['New_ID']

		self.neo_id_map = df_neo.reset_index()[['IDAsteroide', 'match_key']].copy()
		self.neo_id_map['New_ID'] = self.neo_id_map['match_key'].map(key_map)
		self.neo_id_map = self.neo_id_map.set_index('IDAsteroide')['New_ID']

		df_merged['IDAsteroide'] = df_merged['New_IDAsteroide']
		self._save(df_merged.drop(columns=['match_key', 'New_IDAsteroide']), FILES['asteroids'])
		print(f"  Saved {len(df_merged)} merged asteroids.")

	def merge_orbits(self):
		print("Merging Orbits...")
		self._merge_dependent_table('orbits', 'IDOrbita', has_class=True)

	def merge_observations(self):
		print("Merging Observations...")
		self._merge_dependent_table('observations', 'IDObservacao', has_class=False)

	def _merge_dependent_table(self, table_key, id_col, has_class=False):
		current_id = 1
		dfs = []

		# Process MPC
		df_mpc = self._read_csv(DIR_MPC, INPUT_MAP_MPC[table_key])
		if not df_mpc.empty:
			df_mpc['IDAsteroide'] = df_mpc['IDAsteroide'].map(self.mpc_id_map).fillna(df_mpc['IDAsteroide'])
			if has_class and self.mpc_class_map is not None:
				df_mpc['IDClasse'] = df_mpc['IDClasse'].map(self.mpc_class_map).fillna(df_mpc['IDClasse'])

			df_mpc[id_col] = np.arange(current_id, current_id + len(df_mpc)).astype(str)
			current_id += len(df_mpc)
			dfs.append(df_mpc)

		# Process NEO
		if table_key in INPUT_MAP_NEO:
			df_neo = self._read_csv(DIR_NEO, INPUT_MAP_NEO[table_key])
			if not df_neo.empty:
				df_neo['IDAsteroide'] = df_neo['IDAsteroide'].map(self.neo_id_map).fillna(df_neo['IDAsteroide'])
				if has_class and self.neo_class_map is not None:
					df_neo['IDClasse'] = df_neo['IDClasse'].map(self.neo_class_map).fillna(df_neo['IDClasse'])

				df_neo[id_col] = np.arange(current_id, current_id + len(df_neo)).astype(str)
				current_id += len(df_neo)
				dfs.append(df_neo)

		if dfs:
			merged = pd.concat(dfs, ignore_index=True)
			self._save(merged, FILES[table_key])
			print(f"  Saved {len(merged)} {table_key}.")

	def copy_references(self):
		print("Copying Reference Tables...")
		for key in ['software', 'astronomers']:
			df = self._read_csv(DIR_MPC, INPUT_MAP_MPC[key])
			if not df.empty:
				df['IDCentro'] = '1' # Default center
				self._save(df, FILES[key])

	def _read_csv(self, folder, filename, cols=None):
		path = os.path.join(folder, filename)
		if os.path.exists(path):
			df = pd.read_csv(path, dtype=str, low_memory=False)
			if cols and df.empty: return pd.DataFrame(columns=cols)
			return df
		return pd.DataFrame(columns=cols) if cols else pd.DataFrame()

	def run(self):
		self.merge_classes()
		self.merge_asteroids()
		self.merge_orbits()
		self.merge_observations()
		self.copy_references()