import os
import time
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from typing import Dict

# Local Imports
from .config import SCHEMAS, CHUNK_SIZE, NEO_DTYPES
# Shared Imports
from common.utils import (
    ensure_directory, expand_scientific_notation,
    clean_dataframe_text
)

def process_chunk_worker(chunk: pd.DataFrame) -> pd.DataFrame:
    """Worker for NEO Data."""
    chunk = chunk.dropna(subset=['id']).copy().reset_index(drop=True)
    if chunk.empty: return chunk

    # 1. Date Parsing (Vectorized)
    # epoch_cal: YYYYMMDD -> YYYY-MM-DD
    chunk['epoch_iso'] = pd.to_datetime(chunk['epoch_cal'].astype(str), format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d').fillna("")

    # tp_cal: YYYYMMDD.ddddd -> YYYY-MM-DD HH:MM:SS.fff
    tp_str = chunk['tp_cal'].astype(str)
    # Split integral and fractional parts
    tp_split = tp_str.str.split('.', n=1, expand=True)
    base_dates = pd.to_datetime(tp_split[0], format='%Y%m%d', errors='coerce')

    chunk['tp_iso'] = ""
    valid_tp = base_dates.notna()

    if valid_tp.any():
        if tp_split.shape[1] > 1:
            # Handle fraction of day
            frac = ("0." + tp_split[1].fillna("0")).astype(float)
            full_dates = base_dates + pd.to_timedelta(frac, unit='D')
            chunk.loc[valid_tp, 'tp_iso'] = full_dates[valid_tp].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            chunk.loc[valid_tp, 'tp_iso'] = base_dates[valid_tp].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

    # 2. Boolean Flags
    chunk['neo_flag'] = np.where(chunk['neo'].fillna('N') == 'Y', '1', '0')
    chunk['pha_flag'] = np.where(chunk['pha'].fillna('N') == 'Y', '1', '0')

    # 3. String Cleaning
    # Define mapping explicitly to match expected column names in _map_classes
    col_map = {
        'name': 'name_clean',
        'pdes': 'pdes_clean',
        'prefix': 'prefix_clean',
        'spkid': 'spkid_clean',
        'class': 'class_clean',
        'class_description': 'class_desc_clean'
    }

    for src, dest in col_map.items():
        chunk[dest] = chunk[src].fillna("").astype(str).str.strip()

        # Base invalid values
        mask = chunk[dest].isin(['nan', '', '<NA>'])

        # Special handling: Treat 'Unclassified' class as null/empty
        if dest == 'class_clean':
            mask |= (chunk[dest].str.lower() == 'unclassified')

        chunk.loc[mask, dest] = np.nan

    # 4. Number Parsing
    # FIX: Initialize with object dtype to prevent "incompatible dtype" error when assigning strings later
    chunk['number_clean'] = pd.Series([np.nan] * len(chunk), dtype='object')

    raw_id = chunk['id'].astype(str)

    # Heuristic:
    # Starts with 'a' -> Numbered (strip 'a', remove leading zeros).
    # Starts with 'b' (or anything else) -> Unnumbered (kept as NaN).
    is_numbered = raw_id.str.startswith('a', na=False)

    if is_numbered.any():
        # UPDATED: Trim leading zeros from the number string
        chunk.loc[is_numbered, 'number_clean'] = raw_id.loc[is_numbered].str[1:].str.lstrip('0')

    return chunk

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
        self.file_handles = {}

    def _map_classes(self, chunk: pd.DataFrame) -> None:
        chunk['id_class'] = ""
        # Access class_desc_clean which is now correctly created
        unique_pairs = chunk[['class_clean', 'class_desc_clean']].drop_duplicates('class_clean')

        for _, row in unique_pairs.iterrows():
            code = row['class_clean']
            if pd.isna(code): continue

            if code not in self.class_map:
                self.class_map[code] = self.next_class_id
                self.class_desc_map[code] = row['class_desc_clean'] if pd.notna(row['class_desc_clean']) else code
                self.next_class_id += 1

        chunk['id_class'] = chunk['class_clean'].map(self.class_map).fillna("").astype(str).str.replace(r'\.0$', '', regex=True)

    def process(self):
        print(f"Reading {self.input_path}...")
        if not os.path.exists(self.input_path):
            print(f"Error: {self.input_path} not found.")
            return

        ensure_directory(self.output_dir)

        try:
            for filename, headers in SCHEMAS.items():
                self.file_handles[filename] = open(os.path.join(self.output_dir, filename), 'w', encoding='utf-8', newline='')
                pd.DataFrame(columns=headers).to_csv(self.file_handles[filename], index=False)

            start_time = time.time()
            total_records = 0
            max_workers = max(1, (os.cpu_count() or 2) - 1)

            with pd.read_csv(self.input_path, sep=';', chunksize=CHUNK_SIZE, dtype=NEO_DTYPES, on_bad_lines='skip', low_memory=False) as reader:
                with ProcessPoolExecutor(max_workers=max_workers) as executor:
                    futures = []

                    def handle_future(future):
                        nonlocal total_records
                        try:
                            chunk = future.result()
                            if chunk is None or chunk.empty: return

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
                            print(f"Worker Error: {e}")

                    for raw_chunk in reader:
                        futures.append(executor.submit(process_chunk_worker, raw_chunk))
                        if len(futures) >= max_workers * 2:
                            handle_future(futures.pop(0))
                    for f in futures: handle_future(f)

            # Write Classes
            if self.class_map and 'neo_classes.csv' in self.file_handles:
                df = pd.DataFrame([
                    {'IDClasse': v, 'Descricao': self.class_desc_map.get(k, k), 'CodClasse': k}
                    for k, v in self.class_map.items()
                ])
                clean_dataframe_text(df, ['Descricao'])
                df[['IDClasse', 'Descricao', 'CodClasse']].to_csv(self.file_handles['neo_classes.csv'], mode='a', header=False, index=False)

            print(f"\nDone! Processed {total_records} records in {time.time() - start_time:.2f}s.")

        finally:
            for f in self.file_handles.values(): f.close()

    def _write_tables(self, chunk):
        # Asteroids
        df_ast = pd.DataFrame({
            'IDAsteroide': chunk['IDAsteroide'],
            'number': chunk['number_clean'],
            'spkid': chunk['spkid_clean'],
            'pdes': chunk['pdes_clean'],
            'name': chunk['name_clean'],
            'prefix': chunk['prefix_clean'],
            'neo': chunk['neo_flag'],
            'pha': chunk['pha_flag']
        })
        for col in ['h', 'diameter', 'diameter_sigma', 'albedo']:
            target = col.upper() if col == 'h' else col
            df_ast[target] = [expand_scientific_notation(x) for x in chunk[col]]

        # G is missing in NEO, set empty
        df_ast['G'] = ""
        clean_dataframe_text(df_ast, ['name'])
        df_ast.to_csv(self.file_handles['neo_asteroids.csv'], mode='a', header=False, index=False)

        # Orbits
        df_orb = pd.DataFrame({
            'IDOrbita': chunk['IDOrbita'],
            'IDAsteroide': chunk['IDAsteroide'],
            'epoch': chunk['epoch_iso'],
            'tp': chunk['tp_iso'],
            'IDClasse': chunk['id_class']
        })

        # Columns that just need expansion
        cols = ['e', 'a', 'q', 'i', 'om', 'w', 'ma', 'ad', 'n', 'per', 'rms', 'moid', 'moid_ld',
                'sigma_e', 'sigma_a', 'sigma_q', 'sigma_i', 'sigma_om', 'sigma_w',
                'sigma_ma', 'sigma_ad', 'sigma_n', 'sigma_tp', 'sigma_per']

        for col in cols:
            df_orb[col] = [expand_scientific_notation(x) for x in chunk.get(col, "")]

        # Empty cols
        for col in ['Hex_Flags', 'Is1kmNEO', 'IsCriticalList', 'IsOneOppositionEarlier',
                    'uncertainty', 'Reference', 'Num_Obs', 'Num_Opp', 'Arc', 'Coarse_Perts', 'Precise_Perts']:
            df_orb[col] = ""

        df_orb = df_orb[SCHEMAS['neo_orbits.csv']]
        df_orb.to_csv(self.file_handles['neo_orbits.csv'], mode='a', header=False, index=False)

        # Observations
        df_obs = pd.DataFrame({
            'IDObservacao': range(self.next_observation_id, self.next_observation_id + len(chunk)),
            'IDAsteroide': chunk['IDAsteroide'],
            'Data_atualizacao': chunk['epoch_iso']
        })
        self.next_observation_id += len(chunk)
        for col in ['IDAstronomo', 'IDSoftware', 'IDEquipamento', 'Hora', 'Duracao', 'Modo']:
            df_obs[col] = ""

        df_obs = df_obs[SCHEMAS['neo_observations.csv']]
        df_obs.to_csv(self.file_handles['neo_observations.csv'], mode='a', header=False, index=False)