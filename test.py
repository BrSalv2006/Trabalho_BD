import os
import pandas as pd

# Configuration
INPUT_DIR = 'output_tables_mpcorb'

# Define expected schemas for validation (matching your target DB structure)
EXPECTED_SCHEMAS = {
    'mpcorb_asteroids.csv': [
        'IDAsteroide', 'number', 'spkid', 'pdes', 'name', 'prefix', 'H', 'G',
        'diameter', 'diameter_sigma', 'albedo', 'neo', 'pha'
    ],
    'mpcorb_astronomers.csv': [
        'IDAstronomo', 'Nome', 'IDCentro'
    ],
    'mpcorb_observations.csv': [
        'IDAsteroide', 'IDAstronomo', 'IDSoftware', 'Data_atualizacao',
        'IDEquipamento', 'Hora', 'Duracao', 'Modo'
    ],
    'mpcorb_orbits.csv': [
        'IDAsteroide', 'epoch', 'e', 'a', 'i', 'om', 'w', 'ma', 'n', 'tp',
        'moid', 'moid_ld', 'q', 'ad', 'per', 'rms', 'Arc',
        'sigma_e', 'sigma_a', 'sigma_q', 'sigma_i', 'sigma_om', 'sigma_w',
        'sigma_ma', 'sigma_ad', 'sigma_n', 'sigma_tp', 'sigma_per',
        'Hex_Flags', 'OrbitType', 'Is1kmNEO', 'IsCriticalList',
        'IsOneOppositionEarlier', 'uncertainty', 'Reference', 'Num_Obs',
        'Num_Opp', 'Coarse_Perts', 'Precise_Perts', 'IDClasse'
    ],
    'mpcorb_software.csv': [
        'IDSoftware', 'Nome', 'Versao'
    ]
}

def analyze_csv_lengths(directory):
    print(f"Analyzing CSV files in '{directory}'...\n")

    if not os.path.exists(directory):
        print(f"Error: Directory '{directory}' not found.")
        return

    # List all csv files
    files = [f for f in os.listdir(directory) if f.endswith('.csv')]

    # 1. Verify File Existence & Schema
    print("--- 1. Schema Verification ---")
    all_schemas_valid = True

    for filename, expected_cols in EXPECTED_SCHEMAS.items():
        filepath = os.path.join(directory, filename)
        if not os.path.exists(filepath):
            print(f"[MISSING] {filename} not found.")
            all_schemas_valid = False
            continue

        try:
            # Read just the header
            df_head = pd.read_csv(filepath, nrows=0)
            file_cols = list(df_head.columns)

            if file_cols != expected_cols:
                print(f"[FAIL] {filename} column mismatch!")
                print(f"  Expected: {expected_cols[:5]}... ({len(expected_cols)} cols)")
                print(f"  Found:    {file_cols[:5]}... ({len(file_cols)} cols)")
                # Find mismatch
                missing = set(expected_cols) - set(file_cols)
                extra = set(file_cols) - set(expected_cols)
                if missing: print(f"  Missing cols: {missing}")
                if extra: print(f"  Extra cols: {extra}")
                all_schemas_valid = False
            else:
                print(f"[OK] {filename} schema matches.")
        except Exception as e:
            print(f"[ERROR] Could not read {filename}: {e}")
            all_schemas_valid = False

    print("\n" + "="*50 + "\n")

    # 2. Content Analysis (Lengths & Limits)
    print("--- 2. Content Analysis ---")
    for filename in files:
        filepath = os.path.join(directory, filename)
        print(f"File: {filename}")

        try:
            # Read all as string to check raw character lengths
            df = pd.read_csv(filepath, dtype=str, keep_default_na=False)

            if df.empty:
                print("  [Empty File]")
                continue

            # Identify a Key Column for reporting (Heuristic)
            key_col = df.columns[0]
            possible_keys = ['IDAsteroide', 'IDSoftware', 'IDAstronomo', 'IDObservacao', 'IDOrbita']
            for k in possible_keys:
                if k in df.columns:
                    key_col = k
                    break

            # Header for the table
            print(f"  {'Column Name':<25} | {'Type':<10} | {'Max Len':<7} | {'Sample Max Value'}")
            print(f"  {'-'*25}-+-{'-'*10}-+-{'-'*7}-+-{'-'*30}")

            for col in df.columns:
                # Calculate max length of strings in this column
                lengths = df[col].map(len)
                max_len = lengths.max()

                # Get the value that has this max length (for debugging)
                sample_val = ""
                if max_len > 0:
                    sample_row = df[lengths == max_len].iloc[0]
                    sample_val = str(sample_row[col])
                    if len(sample_val) > 50:
                        sample_val = sample_val[:47] + "..."

                # Check for SQL limits
                warning = ""
                threshold = 0

                if max_len > 255:
                    warning = " (! > 255)"
                    threshold = 255
                elif max_len > 50:
                    warning = " (! > 50)"
                    threshold = 50
                elif max_len > 20:
                    warning = " (! > 20)"
                    threshold = 20

                print(f"  {col:<25} | {str(df[col].dtype):<10} | {str(max_len):<7}{warning} | {sample_val}")

                # DETAILED REPORTING: If warning exists, show the IDs causing it
                if warning and threshold > 0:
                    bad_rows = df[lengths > threshold]
                    count = len(bad_rows)
                    if count > 0:
                        print(f"    [!] Found {count} rows exceeding {threshold} chars.")
                        print(f"        Examples ({key_col}):")
                        for idx, row in bad_rows.head(3).iterrows():
                            val = row[col]
                            if len(val) > 40: val = val[:37] + "..."
                            print(f"        - ID {row[key_col]}: '{val}'")

            print("\n")

        except Exception as e:
            print(f"  Error analyzing file: {e}\n")

if __name__ == "__main__":
    analyze_csv_lengths(INPUT_DIR)