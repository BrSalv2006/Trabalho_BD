import os
import numpy as np
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
	"""
	if val is None:
		return ""
	s = str(val)
	if not s or s.lower() in ['nan', '<na>', '']:
		return ""

	# Replace dangerous characters
	return s.replace(',', ';').replace('–', '-').strip()

def expand_scientific_notation(val: Any) -> str:
	"""
	Converts scientific notation (e.g., 1.23E-4) strings to standard decimal strings.
	Uses Decimal to preserve precision for source data that is already string.
	"""
	if val is None:
		return ""
	s = str(val).strip()
	if not s or s.lower() in ['nan', '<na>', '']:
		return ""

	if 'e' not in s.lower():
		return s

	try:
		d = Decimal(s)
		return "{:.30f}".format(d).rstrip('0').rstrip('.')
	except Exception:
		return s

def format_float_array(arr: np.ndarray, precision: int = 10) -> list:
	"""
	Optimized formatter for numpy float arrays.
	Much faster than calling expand_scientific_notation on every element of a calculated float array.
	"""
	# Replace NaNs with empty string
	mask = np.isnan(arr)

	# Format non-NaN values
	# We use basic string formatting which is efficient for floats
	formatted = [f"{x:.{precision}f}".rstrip('0').rstrip('.') if not np.isnan(x) else "" for x in arr]
	return formatted

def clean_dataframe_text(df: pd.DataFrame, columns: list) -> pd.DataFrame:
	"""
	Vectorized cleanup for multiple dataframe columns.
	Replaces commas and en-dashes efficiently.
	"""
	for col in columns:
		if col in df.columns:
			# Ensure string type, replace nan with empty
			df[col] = df[col].astype(str).replace({'nan': '', '<NA>': ''})
			# Vectorized string replace is faster than apply()
			df[col] = df[col].str.replace(',', ';', regex=False).str.replace('–', '-', regex=False).str.strip()
	return df