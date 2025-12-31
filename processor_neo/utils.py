import os
import pandas as pd
from typing import Any

# --- Directory Utilities ---

def ensure_directory(path: str):
    """Creates directory if it doesn't exist."""
    if not os.path.exists(path):
        os.makedirs(path)

# --- Date Parsing Utilities ---

def parse_neo_cal_date(date_val: Any) -> str:
    """
    Parses NEO specific date formats to ISO 8601.
    Supported inputs:
      - '20190427' (YYYYMMDD)
      - '20170705.6734485' (YYYYMMDD.ddddd)
    Returns: 'YYYY-MM-DD HH:MM:SS.ffffff' or empty string.
    """
    s = str(date_val).strip()
    if not s or s.lower() == 'nan' or s.lower() == '<na>':
        return ""

    try:
        # Format: YYYYMMDD.ddddd
        if '.' in s:
            parts = s.split('.')
            base = parts[0]
            frac = parts[1]

            if len(base) == 8:
                dt = pd.to_datetime(base, format='%Y%m%d', errors='coerce')
                if pd.isna(dt): return ""

                # Add fraction of day
                fraction_day = float(f"0.{frac}")
                dt += pd.Timedelta(days=fraction_day)
                return dt.strftime('%Y-%m-%d %H:%M:%S.%f')

        # Format: YYYYMMDD
        elif len(s) == 8 and s.isdigit():
            dt = pd.to_datetime(s, format='%Y%m%d', errors='coerce')
            if pd.notna(dt):
                return dt.strftime('%Y-%m-%d %H:%M:%S')

    except Exception:
        pass

    return ""