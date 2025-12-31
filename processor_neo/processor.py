import os
import time
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from typing import Dict

from config import SCHEMAS, INPUT_FILE, CHUNK_SIZE, NEO_DTYPES
from utils import ensure_directory

# --- Worker Function ---

def process_chunk_worker(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Worker function for processing a chunk of NEO data.
    Standardized name 'process_chunk_worker' to match MPCORB processor.
    """
    # 1. Basic Cleaning
    # Drop rows where 'id' is missing
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
                frac_vals = ("0." + frac[has_frac]).astype(float)
                dt_series.loc[has_frac] += pd.to_timedelta(frac_vals, unit='D')

        chunk.loc[mask, col_dest] = dt_series.dt.strftime(date_format).fillna("")

    # 3. Boolean Flags (Y/N -> 1/0)
    chunk['neo_flag'] = np.where(chunk['neo'].fillna('N') == 'Y', '1', '0')
    chunk['pha_flag'] = np.where(chunk['pha'].fillna('N') == 'Y', '1', '0')

    # 4. String Cleaning
    chunk['name_clean'] = chunk['name'].fillna("").astype(str).str.strip()
    chunk['pdes_clean'] = chunk['pdes'].fillna("").astype(str).str.strip()
    chunk['prefix_clean'] = chunk['prefix'].fillna("").astype(str).str.strip()
    chunk['spkid_clean'] = chunk['spkid'].fillna("").astype(str).str.strip()
    chunk['class_clean'] = chunk['class'].fillna("Unclassified").astype(str).str.strip()

    # 5. Number Parsing
    id_str = chunk['id'].astype(str)
    is_numbered = id_str.str.startswith('a', na=False)

    chunk['number_clean'] = ""
    if is_numbered.any():
        # Strip 'a' then strip leading '0's
        chunk.loc[is_numbered, 'number_clean'] = id_str.loc[is_numbered].str[1:].str.lstrip('0')

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

        # Class Map
        self.class_map: Dict[str, int] = {}
        self.next_class_id = 1

        self.file_handles = {}

    def _map_classes(self, chunk: pd.DataFrame) -> None:
        """
        Maps Class string to Class IDs.
        """
        chunk['id_class'] = ""

        # 1. Update Maps
        classes = chunk['class_clean'].unique()

        for name in classes:
            name_str = str(name)
            if not name_str:
                continue

            if name_str not in self.class_map:
                self.class_map[name_str] = self.next_class_id
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

            # Read CSV with semicolon separator (NEO specific)
            with pd.read_csv(self.input_path, sep=';', chunksize=CHUNK_SIZE,
                             dtype=NEO_DTYPES, on_bad_lines='skip', low_memory=False) as reader:

                with ProcessPoolExecutor(max_workers=max_workers) as executor:
                    futures = []

                    def handle_result(future):
                        nonlocal total_records
                        try:
                            chunk = future.result()
                            if chunk is None or chunk.empty: return

                            # Generate Sequential IDs
                            chunk_len = len(chunk)
                            chunk['IDAsteroide'] = range(self.next_asteroid_id, self.next_asteroid_id + chunk_len)
                            self.next_asteroid_id += chunk_len

                            # Maps
                            self._map_classes(chunk)

                            # Write to disk
                            self._write_tables(chunk)

                            total_records += chunk_len
                            print(f"Processed {total_records} records...", end='\r')
                        except Exception as e:
                            print(f"Error processing chunk: {e}")

                    for raw_chunk in reader:
                        future = executor.submit(process_chunk_worker, raw_chunk)
                        futures.append(future)

                        # Manage memory usage
                        if len(futures) >= max_workers * 2:
                            done_future = futures.pop(0)
                            handle_result(done_future)

                    # Process remaining
                    for future in futures:
                        handle_result(future)

            # Write Classes Table
            if self.class_map and 'neo_classes.csv' in self.file_handles:
                df_class = pd.DataFrame(list(self.class_map.items()), columns=['Descricao', 'IDClasse'])
                df_class['CodClasse'] = df_class['Descricao']
                df_class[['IDClasse', 'Descricao', 'CodClasse']].to_csv(
                    self.file_handles['neo_classes.csv'],
                    mode='a', header=False, index=False
                )

            end_time = time.time()
            print(f"\nDone! Processed {total_records} records in {end_time - start_time:.2f} seconds.")

        finally:
            # Clean up handles
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
        df_ast['H'] = chunk['h'].fillna("")
        df_ast['G'] = ""
        df_ast['diameter'] = chunk['diameter'].fillna("")
        df_ast['diameter_sigma'] = chunk['diameter_sigma'].fillna("")
        df_ast['albedo'] = chunk['albedo'].fillna("")
        df_ast['neo'] = chunk['neo_flag']
        df_ast['pha'] = chunk['pha_flag']

        df_ast.to_csv(self.file_handles['neo_asteroids.csv'], mode='a', header=False, index=False)

        # neo_orbits.csv
        df_orb = pd.DataFrame()
        df_orb['IDAsteroide'] = chunk['IDAsteroide']
        df_orb['epoch'] = chunk['epoch_iso']

        # Map simple float columns
        float_cols = {
            'e': 'e', 'a': 'a', 'i': 'i', 'om': 'om', 'w': 'w',
            'ma': 'ma', 'n': 'n', 'q': 'q', 'ad': 'ad',
            'per': 'per', 'rms': 'rms', 'moid': 'moid', 'moid_ld': 'moid_ld'
        }
        for target, source in float_cols.items():
            df_orb[target] = chunk[source].fillna("")

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
            df_orb[target] = chunk[source].fillna("")

        df_orb['Hex_Flags'] = ""

        # Empty fields not in NEO dataset
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

        # Enforce Correct Column Order from Schema (Fixes tp vs rms issue)
        df_orb = df_orb[SCHEMAS['neo_orbits.csv']]

        df_orb.to_csv(self.file_handles['neo_orbits.csv'], mode='a', header=False, index=False)

        # neo_observations.csv
        df_obs = pd.DataFrame()
        df_obs['IDAsteroide'] = chunk['IDAsteroide']
        df_obs['IDAstronomo'] = ""
        df_obs['IDSoftware'] = ""
        df_obs['Data_atualizacao'] = chunk['epoch_iso']
        df_obs['IDEquipamento'] = ""
        df_obs['Hora'] = ""
        df_obs['Duracao'] = ""
        df_obs['Modo'] = ""

        df_obs.to_csv(self.file_handles['neo_observations.csv'], mode='a', header=False, index=False)