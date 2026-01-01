import datetime
from typing import Any
from .config import (
    CENTURY_MAP, MONTH_MAP, DAY_MAP_OFFSET,
    PLANET_MAP, CENTURY_PREFIX_MAP
)

# --- Constants ---

# Base62 Lookup (0-9, A-Z, a-z)
BASE62_MAP = {
    c: i for i, c in enumerate(
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    )
}

def _get_base62(char: str) -> int:
    """Fast lookup for Base62 characters."""
    return BASE62_MAP.get(char, 0)

# --- Specific MPCORB Unpacking Functions ---

def unpack_packed_date(packed_date: str) -> str:
    """
    Unpacks an MPC packed date string into YYYY-MM-DD.
    Example: 'K194R' -> '2019-04-27'
    """
    if not packed_date or not isinstance(packed_date, str) or len(packed_date) < 5:
        return ""

    try:
        # 1. Year
        century_base = CENTURY_MAP.get(packed_date[0])
        if century_base is None: return ""
        year = century_base + int(packed_date[1:3])

        # 2. Month
        month_char = packed_date[3]
        month = MONTH_MAP.get(month_char) or int(month_char) # Fallback to int if digit

        # 3. Day
        day_char = packed_date[4]
        if day_char.isdigit():
            day = int(day_char)
        else:
            day = ord(day_char) - ord('A') + DAY_MAP_OFFSET

        # Validate components
        if not (1 <= month <= 12) or not (1 <= day <= 31):
            return ""

        return f"{year}-{month:02d}-{day:02d}"

    except (ValueError, KeyError, IndexError):
        return ""

def unpack_designation(packed_desig: Any) -> str:
    """
    Robust unpacking of MPC designations (Permanent & Provisional).
    Handles packed formats like 'K19A01A' -> '2019 AA1'.
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
                # Comet/Sat generic
                return f"{year} {half_month}{cycle_count}"
            elif 'a' <= last_char <= 'z':
                 # Comet Fragment
                 return f"{year} {half_month}{cycle_count}-{last_char.upper()}"
            else:
                # Minor Planet
                suffix = str(cycle_count) if cycle_count > 0 else ""
                return f"{year} {half_month}{last_char}{suffix}"

    return packed

def calculate_tp(epoch_str: str, mean_anomaly: float, mean_motion: float) -> str:
    """
    Calculates Time of Perihelion (tp) when it is missing from the dataset.
    Formula: tp = epoch - (mean_anomaly / mean_motion)
    """
    if not epoch_str or not mean_motion or mean_motion == 0:
        return ""
    try:
        epoch = datetime.datetime.strptime(epoch_str, "%Y-%m-%d")
        days_offset = mean_anomaly / mean_motion
        tp_date = epoch - datetime.timedelta(days=days_offset)
        return tp_date.strftime("%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        return ""