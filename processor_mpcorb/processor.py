import os
import time
import pandas as pd
import numpy as np
from typing import Dict, Set
from concurrent.futures import ProcessPoolExecutor, as_completed

# Import config and utils
from .config import (
    SCHEMAS, INPUT_FILE, CHUNK_SIZE, MPCORB_DTYPES,
    SOFTWARE_PREFIXES, SOFTWARE_SPECIFIC_NAMES,
    MASK_ORBIT_TYPE, ORBIT_TYPES, MASK_NEO, MASK_PHA,
    MASK_1KM_NEO, MASK_CRITICAL_LIST, MASK_1_OPPOSITION
)
from .utils import ensure_directory, unpack_designation, unpack_packed_date, calculate_tp

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

    chunk['obj_id'] = pd.Series(index=chunk.index, dtype='object')
    chunk.loc[is_numeric, 'obj_id'] = chunk.loc[is_numeric, 'designation']

    if (~is_numeric).any():
        complex_desigs = chunk.loc[~is_numeric, 'designation']
        chunk.loc[~is_numeric, 'obj_id'] = [unpack_designation(x) for x in complex_desigs]

    # 3. Vectorized Math (Numpy)
    # Convert to float ONLY for calculation, handle errors
    e_float = pd.to_numeric(chunk['eccentricity'], errors='coerce').fillna(0).values
    a_float = pd.to_numeric(chunk['semi_major_axis'], errors='coerce').fillna(0).values
    n_float = pd.to_numeric(chunk['mean_motion'], errors='coerce').fillna(0).values
    ma_float = pd.to_numeric(chunk['mean_anomaly'], errors='coerce').fillna(0).values

    # Derived Calculations (Must be floats)
    q_float = a_float * (1.0 - e_float)
    ad_float = a_float * (1.0 + e_float)
    per_float = np.divide(360.0, n_float, out=np.zeros_like(n_float), where=n_float!=0)

    # Store derived values as strings (rounded to avoid base2 mess in output)
    chunk['q'] = np.round(q_float, 8).astype(str)
    chunk['ad'] = np.round(ad_float, 8).astype(str)
    chunk['per'] = np.round(per_float, 2).astype(str)

    # 4. Hex Flag Decoding (List Comp + Bitwise)
    hex_list = [
        int(x, 16) if isinstance(x, str) and len(x) == 4 else 0
        for x in chunk['hex_flags']
    ]
    flags_int = np.array(hex_list, dtype=np.int32)

    chunk['OrbitType'] = pd.Series(flags_int & MASK_ORBIT_TYPE).map(ORBIT_TYPES).values

    chunk['is_neo_flag'] = ((flags_int & MASK_NEO) != 0).astype(int)
    chunk['is_pha_flag'] = ((flags_int & MASK_PHA) != 0).astype(int)
    chunk['is_1km_neo'] = ((flags_int & MASK_1KM_NEO) != 0).astype(int)
    chunk['is_critical'] = ((flags_int & MASK_CRITICAL_LIST) != 0).astype(int)
    chunk['is_opp_earlier'] = ((flags_int & MASK_1_OPPOSITION) != 0).astype(int)

    # 5. Date Parsing (List Comp)
    chunk['epoch_iso'] = [unpack_packed_date(x) for x in chunk['epoch']]

    # 6. Tp Calculation (Requires floats)
    epochs_dt = pd.to_datetime(chunk['epoch_iso'], errors='coerce')

    n_safe = np.where(n_float != 0, n_float, np.nan)
    offset_days = ma_float / n_safe

    MAX_DAYS = 106000

    valid_mask = (np.abs(offset_days) <= MAX_DAYS) & pd.notna(epochs_dt) & pd.notna(offset_days)

    chunk['tp'] = ""

    if valid_mask.any():
        valid_offsets = pd.to_timedelta(offset_days[valid_mask], unit='D')
        valid_epochs = epochs_dt[valid_mask]
        chunk.loc[valid_mask, 'tp'] = (valid_epochs - valid_offsets).dt.strftime('%Y-%m-%d %H:%M:%S.%f').fillna("")

    has_data = (chunk['epoch_iso'] != "") & pd.notna(chunk['mean_anomaly']) & (n_float != 0)
    fallback_mask = (~valid_mask) & has_data

    if fallback_mask.any():
        # Fallback uses manual calc
        fallback_subset = chunk.loc[fallback_mask]

        # Zip inputs: epoch string, Mean Anomaly float, Mean Motion float
        ma_subset = ma_float[fallback_mask]
        n_subset = n_float[fallback_mask]

        chunk.loc[fallback_mask, 'tp'] = [
            calculate_tp(ep, ma, n_val)
            for ep, ma, n_val in zip(fallback_subset['epoch_iso'], ma_subset, n_subset)
        ]

    # 7. Designation Parsing
    chunk['number_str'] = ""
    chunk['remainder'] = chunk['designation_full']

    is_numbered = chunk['designation_full'].str.startswith('(', na=False)

    if is_numbered.any():
        split = chunk.loc[is_numbered, 'designation_full'].str.split(')', n=1, expand=True)
        if not split.empty and split.shape[1] > 0:
            chunk.loc[is_numbered, 'number_str'] = split[0].str.replace('(', '', regex=False)
            if split.shape[1] > 1:
                chunk.loc[is_numbered, 'remainder'] = split[1].str.strip()

    has_digits = chunk['remainder'].str.contains(r'\d', regex=True).fillna(False)
    chunk['name_parsed'] = np.where(~has_digits, chunk['remainder'], "")
    chunk['pdes_parsed'] = np.where(has_digits, chunk['remainder'], "")

    cols_to_drop = ['remainder', 'designation_full', 'designation']
    chunk.drop(columns=[c for c in cols_to_drop if c in chunk.columns], inplace=True)

    return chunk


# --- Main Processor Class ---

class AsteroidProcessor:
    """
    Processor for the MPCORB dataset.
    """
    def __init__(self, input_path: str, output_dir: str):
        self.input_path = input_path
        self.output_dir = output_dir

        self.seen_ids: Set[str] = set()

        # Two maps now
        self.software_map: Dict[str, int] = {}
        self.next_software_id = 1

        self.astronomer_map: Dict[str, int] = {}
        self.next_astronomer_id = 1

        # Class Map
        self.class_map: Dict[str, int] = {}
        self.next_class_id = 1

        # Observation ID counter
        self.next_observation_id = 1

        self.next_asteroid_id = 1
        self.file_handles = {}

    def _map_computers_and_astronomers(self, chunk: pd.DataFrame) -> None:
        """
        Differentiates between Software and Astronomers in the 'computer' column.
        """
        # Ensure columns exist
        chunk['id_soft'] = ""
        chunk['id_astro'] = ""

        # 1. Update Maps (Iterate only over unique values)
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

        # 2. Vectorized Assignment
        computer_clean = chunk['computer'].str.strip()

        # Map Software IDs
        soft_map_series = computer_clean.map(self.software_map)
        mask_soft = soft_map_series.notna()
        chunk.loc[mask_soft, 'id_soft'] = soft_map_series[mask_soft].astype(int).astype(str)

        # Map Astronomer IDs
        astro_map_series = computer_clean.map(self.astronomer_map)
        mask_astro = astro_map_series.notna()
        chunk.loc[mask_astro, 'id_astro'] = astro_map_series[mask_astro].astype(int).astype(str)

    def _map_classes(self, chunk: pd.DataFrame) -> None:
        """
        Maps OrbitType to Class IDs.
        """
        chunk['id_class'] = ""

        # 1. Update Maps
        classes = chunk['OrbitType'].dropna().unique()

        for name in classes:
            name_str = str(name)
            if not name_str:
                continue

            if name_str not in self.class_map:
                self.class_map[name_str] = self.next_class_id
                self.next_class_id += 1

        # 2. Vectorized Assignment
        class_series = chunk['OrbitType'].map(self.class_map)
        mask_class = class_series.notna()
        chunk.loc[mask_class, 'id_class'] = class_series[mask_class].astype(int).astype(str)

    def process(self):
        print(f"Reading {self.input_path}...")

        if not os.path.exists(self.input_path):
            print(f"Error: Input file '{self.input_path}' not found.")
            return

        ensure_directory(self.output_dir)

        try:
            for filename, headers in SCHEMAS.items():
                path = os.path.join(self.output_dir, filename)
                f = open(path, 'w', encoding='utf-8', newline='')
                self.file_handles[filename] = f
                pd.DataFrame(columns=headers).to_csv(f, index=False)

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

                            is_new = ~chunk['obj_id'].isin(self.seen_ids)
                            chunk = chunk[is_new].copy()
                            if chunk.empty:
                                return

                            # --- FIX: Reset index to ensure alignment with output dataframes ---
                            # This fixes the issue where subsequent chunks have mismatched indices
                            # causing NaNs in IDAsteroide/IDSoftware/IDAstronomo
                            chunk.reset_index(drop=True, inplace=True)
                            # -----------------------------------------------------------------

                            self.seen_ids.update(chunk['obj_id'])

                            chunk_len = len(chunk)
                            chunk['IDAsteroide'] = range(self.next_asteroid_id, self.next_asteroid_id + chunk_len)
                            self.next_asteroid_id += chunk_len

                            # --- Maps ---
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
                            done_future = futures.pop(0)
                            handle_result(done_future)

                    for future in futures:
                        handle_result(future)

            # Write Final Reference Tables
            # 1. Software
            if self.software_map and 'mpcorb_software.csv' in self.file_handles:
                df_soft = pd.DataFrame(list(self.software_map.items()), columns=['Nome', 'IDSoftware'])
                df_soft['Versao'] = ""
                df_soft[['IDSoftware', 'Nome', 'Versao']].to_csv(
                    self.file_handles['mpcorb_software.csv'],
                    mode='a', header=False, index=False
                )

            # 2. Astronomers
            if self.astronomer_map and 'mpcorb_astronomers.csv' in self.file_handles:
                df_astro = pd.DataFrame(list(self.astronomer_map.items()), columns=['Nome', 'IDAstronomo'])
                df_astro['IDCentro'] = ""
                df_astro[['IDAstronomo', 'Nome', 'IDCentro']].to_csv(
                    self.file_handles['mpcorb_astronomers.csv'],
                    mode='a', header=False, index=False
                )

            # 3. Classes
            if self.class_map and 'mpcorb_classes.csv' in self.file_handles:
                df_class = pd.DataFrame(list(self.class_map.items()), columns=['Descricao', 'IDClasse'])
                df_class['CodClasse'] = df_class['Descricao'] # Map Descricao to CodClasse as fallback
                df_class[['IDClasse', 'Descricao', 'CodClasse']].to_csv(
                    self.file_handles['mpcorb_classes.csv'],
                    mode='a', header=False, index=False
                )

            end_time = time.time()
            print(f"\nDone! Processed {total_records} records in {end_time - start_time:.2f} seconds.")

        finally:
            for f in self.file_handles.values():
                f.close()
            self.file_handles.clear()

    def _write_tables(self, chunk):
        """Writes to persistent file handles directly."""

        # mpcorb_asteroids.csv
        df_ast = pd.DataFrame()
        df_ast['IDAsteroide'] = chunk['IDAsteroide']
        df_ast['number'] = chunk['number_str'].fillna("")
        df_ast['spkid'] = ""
        df_ast['pdes'] = chunk['pdes_parsed']
        df_ast['name'] = chunk['name_parsed']
        df_ast['prefix'] = ""
        df_ast['neo'] = chunk['is_neo_flag']
        df_ast['pha'] = chunk['is_pha_flag']
        df_ast['H'] = chunk['abs_mag'].fillna("")
        df_ast['G'] = chunk['slope_param'].fillna("")
        df_ast['diameter'] = ""
        df_ast['diameter_sigma'] = ""
        df_ast['albedo'] = ""
        df_ast.to_csv(self.file_handles['mpcorb_asteroids.csv'], mode='a', header=False, index=False)

        # mpcorb_observations.csv
        df_obs = pd.DataFrame()
        n_obs = len(chunk)
        df_obs['IDObservacao'] = range(self.next_observation_id, self.next_observation_id + n_obs)
        self.next_observation_id += n_obs
        df_obs['IDAsteroide'] = chunk['IDAsteroide']
        df_obs['IDAstronomo'] = chunk['id_astro']
        df_obs['IDSoftware'] = chunk['id_soft']
        df_obs['IDEquipamento'] = ""
        df_obs['Data_atualizacao'] = chunk['epoch_iso']
        df_obs['Hora'] = ""
        df_obs['Duracao'] = ""
        df_obs['Modo'] = "Orbit Computation"
        df_obs.to_csv(self.file_handles['mpcorb_observations.csv'], mode='a', header=False, index=False)

        # mpcorb_orbits.csv
        df_orb = pd.DataFrame()
        df_orb['IDAsteroide'] = chunk['IDAsteroide']
        df_orb['epoch'] = chunk['epoch_iso']
        # PASS THROUGH ORIGINAL STRINGS
        df_orb['e'] = chunk['eccentricity'].fillna("")
        df_orb['a'] = chunk['semi_major_axis'].fillna("")
        df_orb['i'] = chunk['inclination'].fillna("")
        df_orb['om'] = chunk['long_asc_node'].fillna("")
        df_orb['w'] = chunk['arg_perihelion'].fillna("")
        df_orb['ma'] = chunk['mean_anomaly'].fillna("")
        df_orb['n'] = chunk['mean_motion'].fillna("")

        df_orb['tp'] = chunk['tp']
        df_orb['moid'] = ""
        df_orb['moid_ld'] = ""

        # Use calculated strings for derived columns
        df_orb['q'] = chunk['q']
        df_orb['ad'] = chunk['ad']
        df_orb['per'] = chunk['per']

        df_orb['rms'] = chunk.get('rms_residual', "")
        df_orb['Arc'] = chunk.get('first_obs', "").astype(str) + "-" + chunk.get('last_obs', "").astype(str)

        empty_cols = ['sigma_e', 'sigma_a', 'sigma_q', 'sigma_i', 'sigma_om', 'sigma_w', 'sigma_ma', 'sigma_ad', 'sigma_n', 'sigma_tp', 'sigma_per']
        for col in empty_cols:
            df_orb[col] = ""

        df_orb['Hex_Flags'] = chunk['hex_flags']
        df_orb['Is1kmNEO'] = chunk['is_1km_neo']
        df_orb['IsCriticalList'] = chunk['is_critical']
        df_orb['IsOneOppositionEarlier'] = chunk['is_opp_earlier']
        df_orb['uncertainty'] = chunk.get('uncertainty', "")
        df_orb['Reference'] = chunk.get('reference', "")

        num_obs = pd.to_numeric(chunk.get('num_observations', ""), errors='coerce')
        num_obs_filled = num_obs.fillna(-1).astype(int).astype(str)
        df_orb['Num_Obs'] = np.where(num_obs.notna(), num_obs_filled, "")

        num_opp = pd.to_numeric(chunk.get('num_oppositions', ""), errors='coerce')
        num_opp_filled = num_opp.fillna(-1).astype(int).astype(str)
        df_orb['Num_Opp'] = np.where(num_opp.notna(), num_opp_filled, "")

        df_orb['Coarse_Perts'] = chunk.get('coarse_perturbers', "")
        df_orb['Precise_Perts'] = chunk.get('precise_perturbers', "")

        # Link to Class Table
        df_orb['IDClasse'] = chunk['id_class']

        # Enforce Correct Column Order from Schema
        df_orb = df_orb[SCHEMAS['mpcorb_orbits.csv']]

        df_orb.to_csv(self.file_handles['mpcorb_orbits.csv'], mode='a', header=False, index=False)