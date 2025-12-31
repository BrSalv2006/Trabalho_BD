import os
import pandas as pd
import numpy as np
from merger.config import OUTPUT_DIR, FILES

# --- Configuration: SQL Constraints ---
# Define expected columns and constraints based on your SQL Schema
SCHEMA_CONSTRAINTS = {
    'classes': {
        'filename': FILES.get('classes', 'classes.csv'),
        'pk': 'IDClasse',
        'columns': {
            'IDClasse': {'type': 'int', 'not_null': True},
            'Descricao': {'type': 'str', 'max_len': 255},
            'CodClasse': {'type': 'str', 'max_len': 50}
        }
    },
    'asteroids': {
        'filename': FILES['asteroids'],
        'pk': 'IDAsteroide',
        'columns': {
            'IDAsteroide': {'type': 'int', 'not_null': True},
            'number': {'type': 'int', 'nullable': True},
            'spkid': {'type': 'str', 'max_len': 20, 'nullable': True},
            'pdes': {'type': 'str', 'max_len': 20},
            'name': {'type': 'str', 'max_len': 100},
            'prefix': {'type': 'str', 'max_len': 10},
            'neo': {'type': 'bit'},
            'pha': {'type': 'bit'},
            # DECIMAL(10, 5) -> Max 5 digits before decimal
            'H': {'type': 'decimal', 'precision': 10, 'scale': 5},
            'G': {'type': 'decimal', 'precision': 10, 'scale': 5},
            'diameter': {'type': 'decimal', 'precision': 10, 'scale': 5},
            'diameter_sigma': {'type': 'decimal', 'precision': 10, 'scale': 5},
            'albedo': {'type': 'decimal', 'precision': 10, 'scale': 5}
        }
    },
    'software': {
        'filename': FILES['software'],
        'pk': 'IDSoftware',
        'columns': {
            'IDSoftware': {'type': 'int', 'not_null': True},
            'Nome': {'type': 'str', 'max_len': 100},
            'Versao': {'type': 'str', 'max_len': 20}
        }
    },
    'astronomers': {
        'filename': FILES['astronomers'],
        'pk': 'IDAstronomo',
        'columns': {
            'IDAstronomo': {'type': 'int', 'not_null': True},
            'Nome': {'type': 'str', 'max_len': 100},
            'IDCentro': {'type': 'int'}
        }
    },
    'orbits': {
        'filename': FILES['orbits'],
        'fk': [
            {'col': 'IDAsteroide', 'ref_table': 'asteroids', 'ref_col': 'IDAsteroide'},
            {'col': 'IDClasse', 'ref_table': 'classes', 'ref_col': 'IDClasse'}
        ],
        'columns': {
            'IDAsteroide': {'type': 'int', 'not_null': True},
            'IDClasse': {'type': 'int', 'nullable': True},
            'epoch': {'type': 'str'}, # DATE
            # DECIMAL(20, 10)
            'e': {'type': 'decimal', 'precision': 20, 'scale': 10},
            'i': {'type': 'decimal', 'precision': 20, 'scale': 10},
            'om': {'type': 'decimal', 'precision': 20, 'scale': 10},
            'w': {'type': 'decimal', 'precision': 20, 'scale': 10},
            'ma': {'type': 'decimal', 'precision': 20, 'scale': 10},
            'n': {'type': 'decimal', 'precision': 20, 'scale': 10},
            'moid': {'type': 'decimal', 'precision': 20, 'scale': 10},
            'moid_ld': {'type': 'decimal', 'precision': 20, 'scale': 10},
            # DECIMAL(30, 10)
            'a': {'type': 'decimal', 'precision': 30, 'scale': 10},
            'q': {'type': 'decimal', 'precision': 30, 'scale': 10},
            'sigma_e': {'type': 'decimal', 'precision': 30, 'scale': 10},
            'sigma_a': {'type': 'decimal', 'precision': 30, 'scale': 10},
            'sigma_q': {'type': 'decimal', 'precision': 30, 'scale': 10},
            'sigma_i': {'type': 'decimal', 'precision': 30, 'scale': 10},
            'sigma_om': {'type': 'decimal', 'precision': 30, 'scale': 10},
            'sigma_w': {'type': 'decimal', 'precision': 30, 'scale': 10},
            'sigma_ma': {'type': 'decimal', 'precision': 30, 'scale': 10},
            'sigma_ad': {'type': 'decimal', 'precision': 30, 'scale': 10},
            'sigma_n': {'type': 'decimal', 'precision': 30, 'scale': 10},
            'sigma_tp': {'type': 'decimal', 'precision': 30, 'scale': 10},
            'sigma_per': {'type': 'decimal', 'precision': 30, 'scale': 10},
            # Others
            'tp': {'type': 'str'}, # DATETIME2(7) string check
            'rms': {'type': 'decimal', 'precision': 10, 'scale': 5},
            'uncertainty': {'type': 'str', 'max_len': 10},
            'Reference': {'type': 'str', 'max_len': 50},
            'Num_Obs': {'type': 'int'},
            'Num_Opp': {'type': 'int'},
            'Arc': {'type': 'str', 'max_len': 20},
            'Coarse_Perts': {'type': 'str', 'max_len': 20},
            'Precise_Perts': {'type': 'str', 'max_len': 20},
            'Hex_Flags': {'type': 'str', 'max_len': 10},
            'Is1kmNEO': {'type': 'bit'},
            'IsCriticalList': {'type': 'bit'},
            'IsOneOppositionEarlier': {'type': 'bit'}
        }
    },
    'observations': {
        'filename': FILES['observations'],
        'fk': [
            {'col': 'IDAsteroide', 'ref_table': 'asteroids', 'ref_col': 'IDAsteroide'},
            {'col': 'IDAstronomo', 'ref_table': 'astronomers', 'ref_col': 'IDAstronomo'},
            {'col': 'IDSoftware', 'ref_table': 'software', 'ref_col': 'IDSoftware'}
        ],
        'columns': {
            'IDAsteroide': {'type': 'int', 'not_null': True},
            'IDAstronomo': {'type': 'int', 'nullable': True},
            'IDSoftware': {'type': 'int', 'nullable': True},
            'Data_atualizacao': {'type': 'str'}, # DATE
            'Hora': {'type': 'str'}, # TIME
            'Duracao': {'type': 'decimal', 'precision': 10, 'scale': 2},
            'Modo': {'type': 'str', 'max_len': 50}
        }
    }
}

class SchemaValidator:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.dfs = {}

    def load_data(self):
        print(f"Loading data from {self.data_dir}...")
        for table_name, config in SCHEMA_CONSTRAINTS.items():
            path = os.path.join(self.data_dir, config['filename'])
            if os.path.exists(path):
                # Load as string to check lengths and original formats
                self.dfs[table_name] = pd.read_csv(path, dtype=str, low_memory=False)
                print(f"  Loaded {table_name}: {len(self.dfs[table_name])} records")
            else:
                print(f"  WARNING: File not found for {table_name}: {path}")

    def check_constraints(self):
        print("\n--- Starting Validation ---")

        for table_name, config in SCHEMA_CONSTRAINTS.items():
            if table_name not in self.dfs:
                continue

            print(f"\nChecking table: {table_name}")
            df = self.dfs[table_name]

            # 1. Primary Key Check
            if 'pk' in config:
                pk = config['pk']
                if pk in df.columns:
                    # Check Uniqueness
                    if df[pk].is_unique:
                        print(f"  [PASS] PK '{pk}' is unique.")
                    else:
                        dupes = df[pk].duplicated().sum()
                        print(f"  [FAIL] PK '{pk}' has {dupes} duplicates!")

                    # Check Nulls
                    nulls = df[pk].isna().sum()
                    if nulls == 0:
                        print(f"  [PASS] PK '{pk}' has no nulls.")
                    else:
                        print(f"  [FAIL] PK '{pk}' has {nulls} null values!")
                else:
                    print(f"  [FAIL] PK column '{pk}' missing from file!")

            # 2. Column Constraints
            for col, constraints in config['columns'].items():
                if col not in df.columns:
                    print(f"  [FAIL] Missing column: {col}")
                    continue

                # Prepare Series
                series = df[col]

                # Check Not Null
                if constraints.get('not_null'):
                    null_count = series.isna().sum() + (series == "").sum()
                    if null_count > 0:
                        print(f"  [FAIL] Column '{col}' contains {null_count} nulls (Expected NOT NULL).")

                # Check String Length (VARCHAR)
                if constraints.get('type') == 'str' and 'max_len' in constraints:
                    max_len = constraints['max_len']
                    # Calculate lengths of non-null values
                    mask_not_null = series.notna()
                    if mask_not_null.any():
                        lengths = series[mask_not_null].astype(str).str.len()
                        max_found = lengths.max()
                        if max_found > max_len:
                            print(f"  [FAIL] Column '{col}' exceeds max length {max_len}. Found max: {max_found}")
                            # Show examples
                            over_limit = series[lengths > max_len].head(3).tolist()
                            print(f"         Examples: {over_limit}")
                        else:
                            # print(f"  [PASS] Column '{col}' length ok (max {max_found}/{max_len}).")
                            pass

                # Check BIT (0/1)
                if constraints.get('type') == 'bit':
                    mask = series.notna() & (series != "")
                    if mask.any():
                        # Allow 0, 1, '0', '1', True, False
                        valid_bits = series[mask].astype(str).str.lower().isin(['0', '1', 'true', 'false'])
                        invalid_count = (~valid_bits).sum()
                        if invalid_count > 0:
                            print(f"  [FAIL] Column '{col}' has {invalid_count} invalid BIT values (expected 0/1/True/False).")

                # Check Integers
                if constraints.get('type') == 'int':
                    # Try converting non-nulls to int
                    mask = series.notna() & (series != "")
                    if mask.any():
                        try:
                            # 1. Valid Integer Check
                            numerics = pd.to_numeric(series[mask], errors='coerce')
                            # Check for completely non-numeric garbage
                            invalid_count = numerics.isna().sum()
                            if invalid_count > 0:
                                print(f"  [FAIL] Column '{col}' has {invalid_count} non-numeric values.")

                            valid_numerics = numerics.dropna()

                            # Check for non-integer values (e.g. 1.5)
                            non_integers = (valid_numerics % 1 != 0)
                            if non_integers.any():
                                print(f"  [FAIL] Column '{col}' has {non_integers.sum()} values with decimals (e.g. 1.5).")
                                print(f"         Examples: {valid_numerics[non_integers].head(3).tolist()}")

                            # Check for floats formatted as ints (e.g. 1.0) - Warning only
                            # Get original strings for valid numerics
                            original_strings = series.loc[valid_numerics.index].astype(str)
                            has_decimal = original_strings.str.contains(r'\.', regex=True)
                            floats_as_ints = has_decimal.sum()

                            if floats_as_ints > 0:
                                print(f"  [WARN] Column '{col}' has {floats_as_ints} integers formatted as floats (e.g. '10.0').")

                            # 2. Range Check (SQL INT: -2,147,483,648 to 2,147,483,647)
                            min_int, max_int = -2147483648, 2147483647
                            out_of_range = (valid_numerics < min_int) | (valid_numerics > max_int)
                            if out_of_range.any():
                                print(f"  [FAIL] Column '{col}' has {out_of_range.sum()} integers out of SQL INT range.")
                                print(f"         Examples: {valid_numerics[out_of_range].head(3).tolist()}")

                        except Exception as e:
                             print(f"  [FAIL] Column '{col}' integer check failed: {e}")

                # Check Decimals
                if constraints.get('type') == 'decimal':
                    mask = series.notna() & (series != "")
                    if mask.any():
                        try:
                            numerics = pd.to_numeric(series[mask], errors='coerce')
                            invalid_count = numerics.isna().sum()
                            if invalid_count > 0:
                                print(f"  [FAIL] Column '{col}' has {invalid_count} non-numeric values.")

                            # Check Precision (Total Digits) and Scale
                            if 'precision' in constraints and 'scale' in constraints:
                                precision = constraints['precision']
                                scale = constraints['scale']
                                max_int_digits = precision - scale

                                # Check magnitude (integer part length)
                                valid_numerics = numerics.dropna().abs()
                                # Allow slight tolerance for float logic, but strictly check limits
                                # If x >= 10^max_int_digits, it fails
                                limit = 10**max_int_digits
                                over_limit = valid_numerics >= limit

                                if over_limit.any():
                                    print(f"  [FAIL] Column '{col}' exceeds DECIMAL({precision},{scale}) limits.")
                                    print(f"         Max value allowed < {limit}. Found: {valid_numerics[over_limit].max()}")
                                    print(f"         Examples: {valid_numerics[over_limit].head(3).tolist()}")

                        except Exception:
                            print(f"  [FAIL] Column '{col}' decimal check failed.")

    def check_foreign_keys(self):
        print("\n--- Checking Foreign Keys ---")
        for table_name, config in SCHEMA_CONSTRAINTS.items():
            if table_name not in self.dfs or 'fk' not in config:
                continue

            df_child = self.dfs[table_name]

            for fk in config['fk']:
                col = fk['col']
                ref_table = fk['ref_table']
                ref_col = fk['ref_col']

                if ref_table not in self.dfs:
                    print(f"  [SKIP] Cannot check FK '{col}' in '{table_name}': Parent table '{ref_table}' not loaded.")
                    continue

                if col not in df_child.columns:
                    print(f"  [FAIL] FK column '{col}' missing in '{table_name}'.")
                    continue

                df_parent = self.dfs[ref_table]

                # Get unique IDs
                child_ids = df_child[col].dropna().unique()
                child_ids = child_ids[child_ids != ""] # Remove empty strings if any

                parent_ids = set(df_parent[ref_col].dropna().unique())

                # Check existence
                # Convert to strings for consistent comparison
                child_ids_str = set(map(str, child_ids))
                parent_ids_str = set(map(str, parent_ids))

                missing = child_ids_str - parent_ids_str

                if missing:
                    print(f"  [FAIL] FK Integrity '{table_name}.{col}' -> '{ref_table}.{ref_col}'")
                    print(f"         Found {len(missing)} orphaned IDs.")
                    print(f"         Examples: {list(missing)[:5]}")
                else:
                    print(f"  [PASS] FK '{table_name}.{col}' -> '{ref_table}.{ref_col}' is valid.")

    def run(self):
        self.load_data()
        self.check_constraints()
        self.check_foreign_keys()
        print("\nValidation Complete.")

if __name__ == "__main__":
    validator = SchemaValidator(OUTPUT_DIR)
    validator.run()