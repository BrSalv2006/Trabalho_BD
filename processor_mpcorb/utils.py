import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Union, Optional

from .config import (
	CENTURY_MAP, MONTH_MAP, DAY_MAP_OFFSET,
	PLANET_MAP, CENTURY_PREFIX_MAP, ORBIT_TYPES,
	MASK_ORBIT_TYPE, MASK_NEO, MASK_1KM_NEO,
	MASK_1_OPPOSITION, MASK_CRITICAL_LIST, MASK_PHA
)

# --- Constants & Lookups (Optimized) ---

BASE62_MAP = {
	c: i for i, c in enumerate(
		"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	)
}

# --- Core Functions ---

def ensure_directory(path: str) -> None:
	import os
	os.makedirs(path, exist_ok=True)

def clean_str(val: Any) -> str:
	if val is None:
		return ""
	s = str(val).strip()
	return "" if s == "" else s

def expand_scientific_notation(val: Any) -> str:
	"""
	Converts scientific notation to standard decimal string with high precision.
	optimized to fail fast on non-scientific strings.
	"""
	if val is None:
		return ""

	s = str(val).strip()
	if not s or s.lower() in ('nan', '<na>', ''):
		return ""

	# Fast path: if no 'e' or 'E', return as is (avoids Decimal overhead for standard numbers)
	if 'e' not in s.lower():
		return s

	try:
		# Use Decimal to preserve exact precision
		# Format with 30 places to cover DECIMAL(30,10) then strip trailing zeros/point
		return "{:.30f}".format(Decimal(s)).rstrip('0').rstrip('.')
	except (InvalidOperation, ValueError, TypeError):
		return s

def _get_base62(char: str) -> int:
	return BASE62_MAP.get(char, 0)

# --- Date Unpacking ---

def unpack_packed_date(packed_date: Any) -> str:
	"""
	Unpacks an MPC packed date string into YYYY-MM-DD.
	"""
	if not isinstance(packed_date, str) or len(packed_date) < 5:
		return ""

	try:
		# 1. Year
		century_base = CENTURY_MAP.get(packed_date[0])
		if century_base is None:
			return ""
		year = century_base + int(packed_date[1:3])

		# 2. Month
		month_char = packed_date[3]
		month = MONTH_MAP.get(month_char) or int(month_char)

		# 3. Day
		day_char = packed_date[4]
		if day_char.isdigit():
			day = int(day_char)
		else:
			day = ord(day_char) - ord('A') + DAY_MAP_OFFSET

		return f"{year}-{month:02d}-{day:02d}"

	except (ValueError, KeyError, IndexError):
		return ""

# --- Designation Unpacking ---

def unpack_designation(packed_desig: Any) -> str:
	"""
	Robust unpacking of MPC designations (Permanent & Provisional).
	"""
	packed = str(packed_desig).strip()
	length = len(packed)

	# 1. Purely Numeric
	if packed.isdigit():
		return str(int(packed))

	# 2. Permanent (5 chars)
	if length == 5:
		first_char = packed[0]
		rest = packed[1:]

		# Tilde Extended (> 619,999)
		if first_char == '~':
			value = 0
			for char in rest:
				value = value * 62 + _get_base62(char)
			return str(value + 620000)

		# Extended Numeric (100,000 - 619,999)
		if first_char.isalpha() and rest.isdigit():
			# Satellites
			if packed.endswith('S') and first_char in PLANET_MAP:
				return f"{PLANET_MAP[first_char]} {int(packed[1:4])}"

			# Asteroids
			base_val = _get_base62(first_char)
			return str(base_val * 10000 + int(rest))

	# 3. Provisional (7 chars)
	if length == 7:
		# Survey (PLS, T1S...)
		if packed.startswith(('PLS', 'T1S', 'T2S', 'T3S')):
			suffix = {'PLS': 'P-L', 'T1S': 'T-1', 'T2S': 'T-2', 'T3S': 'T-3'}.get(packed[:3])
			return f"{packed[3:]} {suffix}"

		# Standard Provisional
		century_prefix = CENTURY_PREFIX_MAP.get(packed[0])
		if century_prefix:
			year = century_prefix + packed[1:3]
			half_month = packed[3]
			cycle_code = packed[4:6]
			last_char = packed[6]

			# Cycle count
			if cycle_code.isdigit():
				cycle_count = int(cycle_code)
			else:
				cycle_count = _get_base62(cycle_code[0]) * 10 + int(cycle_code[1])

			# Determine type
			if last_char == '0':
				return f"{year} {half_month}{cycle_count}"
			elif 'a' <= last_char <= 'z':
				return f"{year} {half_month}{cycle_count}-{last_char.upper()}"
			else:
				suffix = str(cycle_count) if cycle_count > 0 else ""
				return f"{year} {half_month}{last_char}{suffix}"

	return packed

# --- Hex & Calculation Utilities ---

def calculate_tp(epoch_str: Optional[str], mean_anomaly: float, mean_motion: float) -> str:
	"""Calculates Time of Perihelion (tp)."""
	if not epoch_str or not mean_motion:
		return ""
	try:
		epoch = datetime.datetime.strptime(epoch_str, "%Y-%m-%d")
		days_offset = mean_anomaly / mean_motion
		tp_date = epoch - datetime.timedelta(days=days_offset)
		return tp_date.strftime("%Y-%m-%d %H:%M:%S.%f")
	except (ValueError, OverflowError, ZeroDivisionError):
		return ""