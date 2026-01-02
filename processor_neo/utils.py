import os
import pandas as pd
from decimal import Decimal, InvalidOperation
from typing import Any

# --- Directory Utilities ---

def ensure_directory(path: str) -> None:
	"""Creates directory if it doesn't exist."""
	os.makedirs(path, exist_ok=True)

# --- Format Utilities ---

def expand_scientific_notation(val: Any) -> str:
	"""
	Converts scientific notation (e.g., 1.23E-4) to a standard decimal string.
	Optimized to fail fast.
	"""
	if val is None:
		return ""

	s = str(val).strip()
	if not s or s.lower() in ('nan', '<na>', ''):
		return ""

	if 'e' not in s.lower():
		return s

	try:
		# Use Decimal to preserve exact precision during expansion
		return "{:.30f}".format(Decimal(s)).rstrip('0').rstrip('.')
	except (InvalidOperation, ValueError, TypeError):
		return s

# --- Date Parsing Utilities ---

def parse_neo_cal_date(date_val: Any) -> str:
	"""
	Parses NEO specific date formats to ISO 8601.
	"""
	s = str(date_val).strip()
	if not s or s.lower() in ('nan', '<na>'):
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