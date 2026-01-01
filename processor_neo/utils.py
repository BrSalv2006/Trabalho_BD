import os
import pandas as pd
from decimal import Decimal
from typing import Any

# --- Directory Utilities ---

def ensure_directory(path: str):
    """Creates directory if it doesn't exist."""
    if not os.path.exists(path):
        os.makedirs(path)

# --- Format Utilities ---

def clean_text_for_csv(val: Any) -> str:
    """
    Sanitizes text for CSV output to prevent column shifting during BULK INSERT.
    1. Replaces commas ',' with semicolons ';'.
    2. Replaces en-dashes '–' with hyphens '-'.
    3. Strips whitespace.
    """
    if val is None:
        return ""
    s = str(val)
    if not s or s.lower() in ['nan', '<na>', '']:
        return ""

    # Replace dangerous characters
    s = s.replace(',', ';').replace('–', '-')
    return s.strip()

def expand_scientific_notation(val: Any) -> str:
    """
    Converts scientific notation (e.g., 1.23E-4) to a standard decimal string.
    Uses Decimal to avoid floating-point precision loss.
    """
    if val is None:
        return ""
    s = str(val).strip()
    if not s or s.lower() in ['nan', '<na>', '']:
        return ""

    # Optimization: If it doesn't look like scientific notation, return as is.
    # This prevents any casting of standard numbers.
    if 'e' not in s.lower():
        return s

    try:
        # Use Decimal to preserve exact precision during expansion
        d = Decimal(s)
        # Format with high precision to capture small numbers, then strip
        # 30 places covers the DECIMAL(30,10) SQL requirement comfortably
        return "{:.30f}".format(d).rstrip('0').rstrip('.')
    except Exception:
        # Fallback to original string on any error
        return s

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