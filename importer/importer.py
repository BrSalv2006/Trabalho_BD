import os
import pandas as pd
import numpy as np
from config import DIR_MPC, DIR_NEO, OUTPUT_DIR, FILES, INPUT_MAP_MPC, INPUT_MAP_NEO

class DataMerger:
    def __init__(self):
        # We store ID maps as Pandas Series for vectorized lookups
        self.mpc_id_map = None
        self.neo_id_map = None

    def _create_match_key(self, df):
        """
        Creates a single normalized column for matching duplicates using vectorization.
        Priority: Number > Pdes (Provisional Desig) > Name
        """
        # Vectorized String Cleaning
        number = df['number'].fillna("").astype(str).str.strip().str.lstrip('0')
        pdes = df['pdes'].fillna("").astype(str).str.strip().str.upper()
        name = df['name'].fillna("").astype(str).str.strip().str.upper()

        # Boolean masks for vectorized selection
        has_number = number != ""
        has_pdes = pdes != ""

        # numpy.select evaluates conditions in order on the whole array
        conditions = [has_number, has_pdes]
        choices = ["NUM_" + number, "DES_" + pdes]
        default = "NAM_" + name

        return np.select(conditions, choices, default=default)

    def merge_asteroids(self):
        print("Merging Asteroids tables...")

        path_mpc = os.path.join(DIR_MPC, INPUT_MAP_MPC['asteroids'])
        path_neo = os.path.join(DIR_NEO, INPUT_MAP_NEO['asteroids'])

        if not os.path.exists(path_mpc):
            raise FileNotFoundError(f"Missing {path_mpc}")

        # low_memory=False speeds up reading by using larger blocks
        df_mpc = pd.read_csv(path_mpc, dtype=str, low_memory=False)

        if os.path.exists(path_neo):
            df_neo = pd.read_csv(path_neo, dtype=str, low_memory=False)
        else:
            df_neo = pd.DataFrame(columns=df_mpc.columns)

        # Vectorized Key Generation
        df_mpc['match_key'] = self._create_match_key(df_mpc)
        df_neo['match_key'] = self._create_match_key(df_neo)

        # Vectorized Filtering
        df_mpc = df_mpc[df_mpc['match_key'] != "NAM_"]
        df_neo = df_neo[df_neo['match_key'] != "NAM_"]

        # Set Index for fast alignment in combine_first (C-level optimization)
        df_mpc = df_mpc.set_index('match_key')
        df_neo = df_neo.set_index('match_key')

        print(f"MPC Unique Records: {len(df_mpc)}")
        print(f"NEO Unique Records: {len(df_neo)}")

        # combine_first aligns indices and fills gaps without looping
        df_merged = df_mpc.combine_first(df_neo)

        df_merged = df_merged.reset_index()

        # Vectorized Sequential ID Generation
        df_merged['New_IDAsteroide'] = np.arange(1, len(df_merged) + 1).astype(str)

        # --- Build ID Maps (Optimized) ---
        # Create a reference Series: match_key -> New_ID
        key_to_new_id = df_merged.set_index('match_key')['New_IDAsteroide']

        print("Building ID maps...")
        # Map back to MPC Old IDs using vectorized index lookup
        mpc_keys = df_mpc.reset_index()[['IDAsteroide', 'match_key']]
        mpc_keys['New_ID'] = mpc_keys['match_key'].map(key_to_new_id)
        # Store as Series (Index=Old_ID, Value=New_ID) for fast .map() later
        self.mpc_id_map = mpc_keys.set_index('IDAsteroide')['New_ID']

        # Map back to NEO Old IDs
        neo_keys = df_neo.reset_index()[['IDAsteroide', 'match_key']]
        neo_keys['New_ID'] = neo_keys['match_key'].map(key_to_new_id)
        self.neo_id_map = neo_keys.set_index('IDAsteroide')['New_ID']

        # Finalize
        df_merged['IDAsteroide'] = df_merged['New_IDAsteroide']
        df_merged.drop(columns=['match_key', 'New_IDAsteroide'], inplace=True)

        self._save(df_merged, FILES['asteroids'])
        print(f"Merged Asteroids saved. Total count: {len(df_merged)}")

    def _update_ids(self, df, id_map_series):
        """
        Updates 'IDAsteroide' using vectorized mapping.
        """
        if df.empty: return df
        # map() with a Series uses optimized index lookup
        df['IDAsteroide'] = df['IDAsteroide'].map(id_map_series)
        return df.dropna(subset=['IDAsteroide'])

    def merge_orbits(self):
        print("Merging Orbits tables...")

        path_mpc = os.path.join(DIR_MPC, INPUT_MAP_MPC['orbits'])
        df_mpc = pd.read_csv(path_mpc, dtype=str, low_memory=False)
        # Vectorized Update
        df_mpc = self._update_ids(df_mpc, self.mpc_id_map)

        path_neo = os.path.join(DIR_NEO, INPUT_MAP_NEO['orbits'])
        if os.path.exists(path_neo):
            df_neo = pd.read_csv(path_neo, dtype=str, low_memory=False)
            df_neo = self._update_ids(df_neo, self.neo_id_map)
        else:
            df_neo = pd.DataFrame()

        # Concatenate (Vectorized Append)
        df_merged = pd.concat([df_mpc, df_neo], ignore_index=True)
        self._save(df_merged, FILES['orbits'])

    def merge_observations(self):
        print("Merging Observations tables...")

        path_mpc = os.path.join(DIR_MPC, INPUT_MAP_MPC['observations'])
        df_mpc = pd.read_csv(path_mpc, dtype=str, low_memory=False)
        df_mpc = self._update_ids(df_mpc, self.mpc_id_map)

        path_neo = os.path.join(DIR_NEO, INPUT_MAP_NEO['observations'])
        if os.path.exists(path_neo):
            df_neo = pd.read_csv(path_neo, dtype=str, low_memory=False)
            df_neo = self._update_ids(df_neo, self.neo_id_map)
        else:
            df_neo = pd.DataFrame()

        df_merged = pd.concat([df_mpc, df_neo], ignore_index=True)
        self._save(df_merged, FILES['observations'])

    def copy_references(self):
        print("Copying Reference tables...")
        for key in ['software', 'astronomers']:
            src = os.path.join(DIR_MPC, INPUT_MAP_MPC[key])
            if os.path.exists(src):
                df = pd.read_csv(src, dtype=str, low_memory=False)
                self._save(df, FILES[key])

    def _save(self, df, filename):
        path = os.path.join(OUTPUT_DIR, filename)
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
        df.to_csv(path, index=False)

    def run(self):
        self.merge_asteroids()
        self.merge_orbits()
        self.merge_observations()
        self.copy_references()
        print("Merge Pipeline Complete!")