"""All normalization logic: cell values, column/header names, and row_data helpers."""
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

# "dmy" = day/month/year, "mdy" = month/day/year, "ymd" = year/month/day
DateFormatHint = str


INTERNAL_TYPED_KEY = "__typed__"
NULL_VALUES = {"null", "none", "na", "n/a", "nan", "-", ""}

_NUMBER_CLEAN_RE = re.compile(r"[$€£¥₹,\s]")
_NUMBER_VALID_RE = re.compile(r"^[-+]?\d*\.?\d+$")
# Money: raw string contains a currency symbol (used to decide money vs plain number at ingest)
_CURRENCY_SYMBOLS = re.compile(r"[$€£¥₹]")
_SYMBOL_TO_CURRENCY: Dict[str, str] = {
    "$": "USD",
    "€": "EUR",
    "£": "GBP",
    "¥": "JPY",
    "₹": "INR",
}

# For value-based "looks like money" detection: symbol/code at front or code at end + rest parses as number (commas stripped).
CURRENCY_SYMBOLS = set("$€£¥₩₹฿₺₽")
CURRENCY_CODES = frozenset({"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR", "KRW", "THB", "TRY", "RUB"})

# Known measurement units (lowercase); number + unit at end (e.g. "100 kg") or unit + number (e.g. "kg 100").
MEASUREMENT_UNITS = frozenset({
    "kg", "g", "mg", "lb", "lbs", "oz", "t", "ton", "tons",
    "m", "km", "cm", "mm", "ft", "in", "mi", "yd",
    "l", "ml", "gal", "qt", "pt",
    "°c", "°f", "c", "f", "k",  # temperature
    "mph", "kph", "m/s",
    "sq", "sq m", "sq ft", "sqm", "sqft",
})
# Number then unit (e.g. "100 kg") or unit then number (e.g. "kg 100")
_MEASUREMENT_NUM_UNIT_RE = re.compile(r"^\s*(-?\d+(?:[.,]\d+)*)\s+(.+?)\s*$")
_MEASUREMENT_UNIT_NUM_RE = re.compile(r"^\s*(.+?)\s+(-?\d+(?:[.,]\d+)*)\s*$")

# (standard_unit, multiplier, offset): value_std = value * multiplier + offset. Enables "1 kg" and "1000 g" → same standard.
_MEASUREMENT_TO_STANDARD: Dict[str, Tuple[str, float, float]] = {
    # Mass -> kg
    "g": ("kg", 0.001, 0.0),
    "mg": ("kg", 0.000_001, 0.0),
    "kg": ("kg", 1.0, 0.0),
    "lb": ("kg", 0.453_592, 0.0),
    "lbs": ("kg", 0.453_592, 0.0),
    "oz": ("kg", 0.028_349_5, 0.0),
    "t": ("kg", 1000.0, 0.0),
    "ton": ("kg", 1000.0, 0.0),
    "tons": ("kg", 1000.0, 0.0),
    # Length -> m
    "m": ("m", 1.0, 0.0),
    "km": ("m", 1000.0, 0.0),
    "cm": ("m", 0.01, 0.0),
    "mm": ("m", 0.001, 0.0),
    "ft": ("m", 0.304_8, 0.0),
    "in": ("m", 0.025_4, 0.0),
    "mi": ("m", 1609.344, 0.0),
    "yd": ("m", 0.914_4, 0.0),
    # Volume -> L
    "l": ("L", 1.0, 0.0),
    "ml": ("L", 0.001, 0.0),
    "gal": ("L", 3.785_41, 0.0),
    "qt": ("L", 0.946_353, 0.0),
    "pt": ("L", 0.473_176, 0.0),
    # Temperature -> °C
    "°c": ("°C", 1.0, 0.0),
    "c": ("°C", 1.0, 0.0),
    "°f": ("°C", 5.0 / 9.0, -160.0 / 9.0),
    "f": ("°C", 5.0 / 9.0, -160.0 / 9.0),
    "k": ("°C", 1.0, -273.15),
    # Speed -> m/s
    "m/s": ("m/s", 1.0, 0.0),
    "mph": ("m/s", 0.447_04, 0.0),
    "kph": ("m/s", 1.0 / 3.6, 0.0),
}
# Area: keep as-is or pick sq m as standard; skip conversion for now to avoid ambiguity (sq vs sq m)

_ISO_DATE_RE = re.compile(
    r"^(\d{4})[\/.-](\d{1,2})[\/.-](\d{1,2})(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?$"
)
_DMY_OR_MDY_RE = re.compile(
    r"^(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{2,4})(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?$"
)


def is_internal_key(key: str) -> bool:
    return key.startswith("__")


# ─── Column / header name normalization ───────────────────────────────────────


def normalize_headers(headers: List[str]) -> List[str]:
    """Normalize header names: strip, collapse internal whitespace to single space, empty → col_{index}, dedupe with _2, _3, …."""
    seen: Dict[str, int] = {}
    normalized: List[str] = []
    for idx, header in enumerate(headers):
        raw = (header or "").strip()
        base = " ".join(raw.split()) if raw else ""
        if not base:
            base = f"col_{idx + 1}"
        key = base
        if key in seen:
            seen[key] += 1
            key = f"{base}_{seen[base]}"
        else:
            seen[key] = 1
        normalized.append(key)
    return normalized


# ─── Cell value normalization ───────────────────────────────────────────────


def normalize_text_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    value_str = unicodedata.normalize("NFC", str(value))
    value_str = value_str.replace("\xa0", " ")
    value_str = " ".join(value_str.split())
    if value_str.lower() in NULL_VALUES:
        return None
    return value_str


def parse_number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    raw = str(value).strip()
    if not raw:
        return None
    lowered = raw.lower()
    if lowered in {"na", "n/a", "nan", "none", "null"}:
        return None

    is_negative_parentheses = raw.startswith("(") and raw.endswith(")")
    if is_negative_parentheses:
        raw = raw[1:-1].strip()

    raw = _NUMBER_CLEAN_RE.sub("", raw)

    multiplier = 1.0
    if raw and raw[-1].lower() in {"k", "m", "b"}:
        suffix = raw[-1].lower()
        raw = raw[:-1]
        if suffix == "k":
            multiplier = 1_000.0
        elif suffix == "m":
            multiplier = 1_000_000.0
        else:
            multiplier = 1_000_000_000.0

    if raw.endswith("%"):
        raw = raw[:-1]

    raw = re.sub(r"[^0-9.\-+]", "", raw)
    if not _NUMBER_VALID_RE.match(raw):
        return None

    try:
        parsed = float(raw) * multiplier
    except ValueError:
        return None

    if is_negative_parentheses:
        parsed *= -1.0
    return parsed


def _infer_currency(raw: str) -> Optional[str]:
    """Infer currency code from the first currency symbol in the string. Returns None if none found."""
    if not raw:
        return None
    for symbol, code in _SYMBOL_TO_CURRENCY.items():
        if symbol in raw:
            return code
    return None


def _format_money_canonical(num: float) -> str:
    """Format a number as canonical money string (no commas, no symbol, always 2 decimal places)."""
    return f"{num:.2f}"


def parse_money(value: Any) -> Optional[Tuple[str, float, Optional[str]]]:
    """
    If value looks like money (contains currency symbol) and parses as a number,
    return (canonical_string, number, currency_code). Otherwise None.
    """
    if value is None:
        return None
    raw = str(value).strip()
    if not raw or not _CURRENCY_SYMBOLS.search(raw):
        return None
    num = parse_number(raw)
    if num is None:
        return None
    currency = _infer_currency(raw)
    return (_format_money_canonical(num), float(num), currency)


# Cap rows scanned for money-column inference (avoids scanning huge datasets).
MAX_ROWS_FOR_MONEY_INFERENCE = 2000


def _looks_like_money(value: str) -> bool:
    """True if currency symbol at front, or currency code at front/end, and the rest (commas stripped) parses as a number."""
    v = value.strip()
    if not v:
        return False
    # Symbol at front + rest is number (e.g. "$100", "€ 1,000")
    if v[0] in CURRENCY_SYMBOLS:
        rest = v[1:].strip().replace(",", "")
        return parse_number(rest) is not None
    # Currency code at front (e.g. "USD 100", "EUR 1,000") — longest match first
    vu = v.upper()
    for code in sorted(CURRENCY_CODES, key=len, reverse=True):
        if vu.startswith(code):
            rest = v[len(code):].strip().replace(",", "")
            if parse_number(rest) is not None:
                return True
            break
    # Currency code at end (e.g. "100 USD", "1,000 EUR")
    for code in sorted(CURRENCY_CODES, key=len, reverse=True):
        if vu.endswith(code):
            rest = v[:-len(code)].strip().replace(",", "")
            if parse_number(rest) is not None:
                return True
            break
    return False


def is_money_column(values: List[str]) -> bool:
    """Returns True if at least one value looks like money (no threshold; columns with many nulls can still be money)."""
    for v in values:
        if v is not None and str(v).strip() and _looks_like_money(str(v)):
            return True
    return False


def infer_money_columns(
    normalized_headers: List[str],
    rows: List[List[str]],
    *,
    max_samples_per_column: int = 500,
) -> Set[int]:
    """
    Infer which column indices are money columns by scanning up to MAX_ROWS_FOR_MONEY_INFERENCE rows.
    A column is money if any value looks like money (currency symbol or code); no threshold,
    so columns with many nulls can still be detected as money.
    """
    ncols = len(normalized_headers)
    rows_to_scan = rows[:MAX_ROWS_FOR_MONEY_INFERENCE]
    out: Set[int] = set()
    for i in range(ncols):
        values = []
        for row in rows_to_scan:
            if len(values) >= max_samples_per_column:
                break
            cell = row[i] if i < len(row) else ""
            values.append(str(cell) if cell is not None else "")
        if is_money_column(values):
            out.add(i)
    return out


# ─── Unit measurements ───────────────────────────────────────────────────────

MEASUREMENT_COLUMN_THRESHOLD = 0.8
MAX_ROWS_FOR_MEASUREMENT_INFERENCE = 2000


def _normalize_unit_for_lookup(unit: str) -> str:
    """Lowercase for lookup; preserve ° for temperature."""
    u = unit.strip()
    return u.lower() if u else ""


def _parse_measurement_parts(value: str) -> Optional[Tuple[float, str]]:
    """If value is 'number unit' or 'unit number' with known unit, return (number, unit); else None."""
    v = value.strip()
    if not v:
        return None
    # Number then unit (e.g. "100 kg", "1,000.5 m")
    m = _MEASUREMENT_NUM_UNIT_RE.match(v)
    if m:
        num_str, unit_part = m.group(1).replace(",", ""), m.group(2).strip()
        unit_norm = _normalize_unit_for_lookup(unit_part)
        if unit_norm in MEASUREMENT_UNITS:
            num = parse_number(num_str)
            if num is not None:
                return (float(num), unit_part.strip())
    # Unit then number (e.g. "kg 100")
    m = _MEASUREMENT_UNIT_NUM_RE.match(v)
    if m:
        unit_part, num_str = m.group(1).strip(), m.group(2).replace(",", "")
        unit_norm = _normalize_unit_for_lookup(unit_part)
        if unit_norm in MEASUREMENT_UNITS:
            num = parse_number(num_str)
            if num is not None:
                return (float(num), unit_part)
    return None


def _looks_like_measurement(value: str) -> bool:
    """True if value is 'number unit' or 'unit number' with a known unit."""
    return _parse_measurement_parts(value) is not None


def _convert_measurement_to_standard(value: float, unit: str) -> Optional[Tuple[float, str]]:
    """Convert (value, unit) to (value_in_standard_unit, standard_unit). Returns None if unit has no standard."""
    key = _normalize_unit_for_lookup(unit)
    if not key or key not in _MEASUREMENT_TO_STANDARD:
        return None
    std_unit, mult, offset = _MEASUREMENT_TO_STANDARD[key]
    value_std = value * mult + offset
    return (value_std, std_unit)


def _format_measurement_canonical(value: float, unit: str) -> str:
    """Format as 'number unit' with sensible decimals."""
    if value == int(value) and abs(value) < 1e15:
        return f"{int(value)} {unit}"
    s = f"{value:.4f}".rstrip("0").rstrip(".")
    return f"{s} {unit}"


def parse_measurement(value: Any) -> Optional[Tuple[str, float, str]]:
    """
    If value looks like a measurement (number + known unit), return (canonical_str, number, unit).
    Values are converted to standard units where defined (e.g. 1000 g → 1 kg, 1 kg → 1 kg).
    """
    if value is None:
        return None
    parts = _parse_measurement_parts(str(value).strip())
    if parts is None:
        return None
    num, unit = parts
    converted = _convert_measurement_to_standard(float(num), unit)
    if converted is not None:
        value_std, standard_unit = converted
        canonical_str = _format_measurement_canonical(value_std, standard_unit)
        return (canonical_str, value_std, standard_unit)
    # No standard for this unit (e.g. sq ft); keep as-is
    canonical_str = _format_measurement_canonical(float(num), unit)
    return (canonical_str, float(num), unit)


def is_measurement_column(values: List[str], threshold: float = MEASUREMENT_COLUMN_THRESHOLD) -> bool:
    """Returns True if majority of non-empty values look like measurements."""
    non_empty = [v.strip() for v in values if v and str(v).strip()]
    if not non_empty:
        return False
    hits = sum(1 for v in non_empty if _looks_like_measurement(v))
    return hits / len(non_empty) >= threshold


def infer_measurement_columns(
    normalized_headers: List[str],
    rows: List[List[str]],
    *,
    max_samples_per_column: int = 500,
) -> Set[int]:
    """
    Infer which column indices are measurement columns (number + unit) by scanning up to
    MAX_ROWS_FOR_MEASUREMENT_INFERENCE rows.
    """
    ncols = len(normalized_headers)
    rows_to_scan = rows[:MAX_ROWS_FOR_MEASUREMENT_INFERENCE]
    out: Set[int] = set()
    for i in range(ncols):
        values = []
        for row in rows_to_scan:
            if len(values) >= max_samples_per_column:
                break
            cell = row[i] if i < len(row) else ""
            values.append(str(cell) if cell is not None else "")
        if is_measurement_column(values):
            out.add(i)
    return out


def parse_date(value: Any) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    iso_match = _ISO_DATE_RE.match(text)
    if iso_match:
        year = int(iso_match.group(1))
        month = int(iso_match.group(2))
        day = int(iso_match.group(3))
        try:
            parsed = datetime(year, month, day, tzinfo=timezone.utc)
            return {"iso_date": parsed.date().isoformat(), "epoch_seconds": int(parsed.timestamp())}
        except ValueError:
            return None

    dmy_or_mdy = _DMY_OR_MDY_RE.match(text)
    if dmy_or_mdy:
        a = int(dmy_or_mdy.group(1))
        b = int(dmy_or_mdy.group(2))
        year_raw = int(dmy_or_mdy.group(3))
        year = year_raw + 2000 if year_raw < 100 else year_raw

        day = a
        month = b
        if a <= 12 and b > 12:
            month = a
            day = b
        elif a <= 12 and b <= 12:
            # Ambiguous dates default to day/month to match existing dataset conventions.
            day = a
            month = b

        try:
            parsed = datetime(year, month, day, tzinfo=timezone.utc)
            return {"iso_date": parsed.date().isoformat(), "epoch_seconds": int(parsed.timestamp())}
        except ValueError:
            return None

    try:
        parsed_epoch = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
        return {"iso_date": parsed_epoch.date().isoformat(), "epoch_seconds": int(parsed_epoch.timestamp())}
    except ValueError:
        return None


def _parse_dmy_or_mdy_parts(
    a: int, b: int, year_raw: int, fmt: Optional[DateFormatHint]
) -> Optional[Dict[str, Any]]:
    """Interpret a, b, year_raw as date parts; fmt is 'dmy', 'mdy', or None (ambiguous)."""
    year = year_raw + 2000 if year_raw < 100 else year_raw
    if fmt == "dmy":
        day, month = a, b
    elif fmt == "mdy":
        month, day = a, b
    else:
        # Ambiguous: both <= 12. Default dmy (day/month/year).
        day, month = a, b
    try:
        parsed = datetime(year, month, day, tzinfo=timezone.utc)
        return {"iso_date": parsed.date().isoformat(), "epoch_seconds": int(parsed.timestamp())}
    except ValueError:
        return None


def parse_date_with_format(
    value: Any, fmt: Optional[DateFormatHint] = None
) -> Optional[Dict[str, Any]]:
    """Parse date using optional format hint for ambiguous DD/MM/YY vs MM/DD/YY. Returns same shape as parse_date."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    iso_match = _ISO_DATE_RE.match(text)
    if iso_match:
        year = int(iso_match.group(1))
        month = int(iso_match.group(2))
        day = int(iso_match.group(3))
        try:
            parsed = datetime(year, month, day, tzinfo=timezone.utc)
            return {"iso_date": parsed.date().isoformat(), "epoch_seconds": int(parsed.timestamp())}
        except ValueError:
            return None

    dmy_or_mdy = _DMY_OR_MDY_RE.match(text)
    if dmy_or_mdy:
        a = int(dmy_or_mdy.group(1))
        b = int(dmy_or_mdy.group(2))
        year_raw = int(dmy_or_mdy.group(3))
        return _parse_dmy_or_mdy_parts(a, b, year_raw, fmt)

    try:
        parsed_epoch = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
        return {"iso_date": parsed_epoch.date().isoformat(), "epoch_seconds": int(parsed_epoch.timestamp())}
    except ValueError:
        return None


def _date_like_parts(text: str) -> Optional[Tuple[int, int, int]]:
    """If text matches DD/MM/YY or similar, return (a, b, year_raw). Else None."""
    text = text.strip()
    if not text:
        return None
    if _ISO_DATE_RE.match(text):
        return None  # already unambiguous
    m = _DMY_OR_MDY_RE.match(text)
    if not m:
        return None
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


def infer_column_date_format(samples: List[str]) -> Optional[DateFormatHint]:
    """
    Infer date format for a column from a list of date-like strings.
    Returns 'dmy', 'mdy', or None. Uses disambiguating values (e.g. day > 12) when present.
    """
    first_is_day = False
    second_is_day = False
    for s in samples:
        parts = _date_like_parts(s)
        if parts is None:
            continue
        a, b, year_raw = parts
        if a > 12:
            first_is_day = True
        if b > 12:
            second_is_day = True
    if first_is_day and not second_is_day:
        return "dmy"
    if second_is_day and not first_is_day:
        return "mdy"
    if first_is_day and second_is_day:
        return None  # inconsistent column, leave ambiguous
    return "dmy"  # all ambiguous (e.g. 01/01/01): default day/month/year


def infer_date_formats_for_columns(
    normalized_headers: List[str],
    rows: List[List[str]],
    *,
    max_samples_per_column: int = 500,
) -> Dict[int, Optional[DateFormatHint]]:
    """Infer date format per column index by sampling column values. Used to disambiguate 01/01/01-style dates."""
    ncols = len(normalized_headers)
    by_col: Dict[int, List[str]] = {i: [] for i in range(ncols)}
    for row in rows:
        for i in range(ncols):
            if len(by_col[i]) >= max_samples_per_column:
                continue
            cell = row[i] if i < len(row) else None
            if cell is None:
                continue
            text = str(cell).strip()
            if not text:
                continue
            if _ISO_DATE_RE.match(text) or _DMY_OR_MDY_RE.match(text):
                by_col[i].append(text)
    return {
        i: infer_column_date_format(by_col[i]) if by_col[i] else None
        for i in range(ncols)
    }


def _cell_value(val: Any) -> Any:
    """Normalize a cell value for storage: either legacy (plain str) or { original, normalized }."""
    if isinstance(val, dict) and "normalized" in val:
        return val
    if isinstance(val, dict) and "n" in val:
        return {"original": val.get("o"), "normalized": val.get("n")}
    return {"original": val, "normalized": val}


def get_normalized_value(row_data: Dict[str, Any], column: str) -> Any:
    """Return the normalized value for a column. Supports legacy (plain string) and new shape."""
    val = row_data.get(column)
    if isinstance(val, dict) and "normalized" in val:
        return val["normalized"]
    if isinstance(val, dict) and "n" in val:
        return val["n"]
    return val


def get_original_value(row_data: Dict[str, Any], column: str) -> Any:
    """Return the original raw value for a column. Supports legacy (returns same as normalized) and new shape."""
    val = row_data.get(column)
    if isinstance(val, dict) and "original" in val:
        return val["original"]
    if isinstance(val, dict) and "o" in val:
        return val["o"]
    return val


def normalize_row_obj(
    normalized_headers: list[str],
    row: list[str],
    *,
    store_original: bool = True,
    date_format_by_column: Optional[Dict[int, Optional[DateFormatHint]]] = None,
    money_columns: Optional[Set[int]] = None,
    measurement_columns: Optional[Set[int]] = None,
) -> Dict[str, Any]:
    """Build row_data: keys are normalized column names; values are { original, normalized } or legacy plain normalized.
    When date_format_by_column is set, dates are parsed with that format and normalized value becomes ISO (YYYY-MM-DD).
    When money_columns is set, a cell is only typed as money if its column index is in the set (avoids false positives).
    When measurement_columns is set, a cell is only typed as measurement if its column index is in the set.
    """
    result: Dict[str, Any] = {}
    typed: Dict[str, Dict[str, Any]] = {}

    for i in range(len(normalized_headers)):
        key = normalized_headers[i]
        raw = row[i] if i < len(row) else None
        text_normalized = normalize_text_value(raw)
        fmt = (date_format_by_column or {}).get(i) if date_format_by_column else None
        date_value = parse_date_with_format(raw, fmt) if raw is not None else None
        if date_value is not None:
            iso_date = date_value["iso_date"]
            if store_original:
                result[key] = {"original": raw, "normalized": iso_date}
            else:
                result[key] = iso_date
            typed[key] = {"type": "date", **date_value}
            continue

        is_money_column = money_columns is not None and i in money_columns
        money_value = parse_money(raw) if raw is not None else None
        if money_value is not None and (money_columns is None or is_money_column):
            canonical_str, num, currency = money_value
            if store_original:
                result[key] = {"original": raw, "normalized": canonical_str}
            else:
                result[key] = canonical_str
            typed[key] = {
                "type": "money",
                "number": num,
                "currency": currency,
            }
            continue

        # In a money column, treat plain numbers as money too (no symbol → currency None)
        if is_money_column and raw is not None:
            plain_num = parse_number(normalize_text_value(raw) or "")
            if plain_num is not None:
                canonical_str = _format_money_canonical(plain_num)
                if store_original:
                    result[key] = {"original": raw, "normalized": canonical_str}
                else:
                    result[key] = canonical_str
                typed[key] = {
                    "type": "money",
                    "number": float(plain_num),
                    "currency": None,
                }
                continue

        # Measurement column: number + unit (e.g. "100 kg", "kg 100")
        is_measurement_col = measurement_columns is not None and i in measurement_columns
        measurement_value = parse_measurement(raw) if raw is not None else None
        if measurement_value is not None and is_measurement_col:
            canonical_str, num, unit = measurement_value
            if store_original:
                result[key] = {"original": raw, "normalized": canonical_str}
            else:
                result[key] = canonical_str
            typed[key] = {"type": "measurement", "number": num, "unit": unit}
            continue

        normalized = text_normalized
        if store_original:
            result[key] = {"original": raw if raw is not None else None, "normalized": normalized}
        else:
            result[key] = normalized
        if normalized is None:
            continue

        numeric_value = parse_number(normalized)
        if numeric_value is not None:
            typed[key] = {"type": "number", "number": numeric_value}
            continue

    if typed:
        result[INTERNAL_TYPED_KEY] = typed
    return result


def get_typed_value(row_data: Dict[str, Any], column: str) -> Optional[Dict[str, Any]]:
    typed = row_data.get(INTERNAL_TYPED_KEY)
    if not isinstance(typed, dict):
        return None
    item = typed.get(column)
    if not isinstance(item, dict):
        return None
    return item


def get_column_currency(row_data: Dict[str, Any], column: str) -> Optional[str]:
    """If the column is typed as money, return its currency code (e.g. USD); else None."""
    item = get_typed_value(row_data, column)
    if not item or item.get("type") != "money":
        return None
    return item.get("currency")


def get_numeric_value(row_data: Dict[str, Any], column: str) -> Optional[float]:
    typed = get_typed_value(row_data, column)
    if typed:
        t = typed.get("type")
        if t in ("number", "money", "measurement"):
            raw = typed.get("number")
            try:
                return float(raw)
            except (TypeError, ValueError):
                return None
    return parse_number(get_normalized_value(row_data, column))


def strip_internal_fields(row_data: Dict[str, Any]) -> Dict[str, Any]:
    """Remove __typed__ and other internal keys. Column values stay as { original, normalized } or plain."""
    return {k: v for k, v in row_data.items() if not is_internal_key(k)}


def flatten_row_data_to_normalized(row_data: Dict[str, Any]) -> Dict[str, Any]:
    """Return a dict keyed by column with only normalized values (for API display where a single value per cell is expected)."""
    return {
        k: get_normalized_value(row_data, k)
        for k in row_data
        if not is_internal_key(k)
    }
