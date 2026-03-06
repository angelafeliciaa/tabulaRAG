import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


INTERNAL_TYPED_KEY = "__typed__"
INTERNAL_NORMALIZED_KEY = "__normalized__"
NULL_VALUES = {"null", "none", "na", "n/a", "nan", "-", "", "—", "–", "missing", "unknown", "undefined"}

_NUMBER_CLEAN_RE = re.compile(r"[$€£¥₹,\s]")
_NUMBER_VALID_RE = re.compile(r"^[-+]?\d*\.?\d+$")
_ISO_DATE_RE = re.compile(
    r"^(\d{4})[\/.-](\d{1,2})[\/.-](\d{1,2})(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?$"
)
_DMY_OR_MDY_RE = re.compile(
    r"^(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{2,4})(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?$"
)


def is_internal_key(key: str) -> bool:
    return key.startswith("__")


def _strip_accents(value: str) -> str:
    """Remove diacritical marks (accents) from a string."""
    nfkd = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in nfkd if unicodedata.category(ch) != "Mn")


def normalize_text_value(value: Any) -> Optional[str]:
    """Normalize a raw cell value for display.

    Handles Unicode normalization (NFC), non-breaking spaces,
    whitespace collapsing, curly quotes, and null detection.
    Returns None for null-like values.
    """
    if value is None:
        return None
    value_str = unicodedata.normalize("NFC", str(value))
    value_str = value_str.replace("\xa0", " ")
    value_str = value_str.replace("\u200b", "")  # zero-width space
    value_str = value_str.replace("\ufeff", "")  # BOM
    # Normalize curly/smart quotes to ASCII equivalents
    value_str = value_str.replace("\u2018", "'").replace("\u2019", "'")
    value_str = value_str.replace("\u201c", '"').replace("\u201d", '"')
    # Normalize dashes
    value_str = value_str.replace("\u2013", "-").replace("\u2014", "-")
    value_str = " ".join(value_str.split())
    if value_str.lower().strip() in NULL_VALUES:
        return None
    return value_str


def normalize_column_name(name: str) -> str:
    """Produce a canonical normalized column name for matching.

    Lowercases, strips accents, replaces non-alphanumeric characters
    with underscores, and collapses consecutive underscores.
    """
    result = _strip_accents(name.strip())
    result = result.lower()
    result = re.sub(r"[^a-z0-9]+", "_", result)
    result = re.sub(r"_+", "_", result)
    result = result.strip("_")
    return result or "col"


def normalize_cell_value(value: Any) -> Optional[str]:
    """Produce a canonical normalized cell value for matching and search.

    Applies display normalization, then lowercases and strips accents
    for case-insensitive, accent-insensitive comparisons.
    """
    display = normalize_text_value(value)
    if display is None:
        return None
    result = _strip_accents(display)
    result = result.lower()
    result = " ".join(result.split())
    return result


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


def normalize_row_obj(
    headers: List[str],
    row: List[str],
    normalized_headers: Optional[List[str]] = None,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    typed: Dict[str, Dict[str, Any]] = {}
    normalized: Dict[str, Optional[str]] = {}

    for i in range(len(headers)):
        key = headers[i]
        norm_key = (
            normalized_headers[i]
            if normalized_headers and i < len(normalized_headers)
            else normalize_column_name(key)
        )
        display = normalize_text_value(row[i] if i < len(row) else None)
        result[key] = display
        normalized[norm_key] = normalize_cell_value(row[i] if i < len(row) else None)

        if display is None:
            continue

        numeric_value = parse_number(display)
        if numeric_value is not None:
            typed[key] = {"type": "number", "number": numeric_value}
            continue

        date_value = parse_date(display)
        if date_value is not None:
            typed[key] = {"type": "date", **date_value}

    if typed:
        result[INTERNAL_TYPED_KEY] = typed
    result[INTERNAL_NORMALIZED_KEY] = normalized
    return result


def get_typed_value(row_data: Dict[str, Any], column: str) -> Optional[Dict[str, Any]]:
    typed = row_data.get(INTERNAL_TYPED_KEY)
    if not isinstance(typed, dict):
        return None
    item = typed.get(column)
    if not isinstance(item, dict):
        return None
    return item


def get_numeric_value(row_data: Dict[str, Any], column: str) -> Optional[float]:
    typed = get_typed_value(row_data, column)
    if typed and typed.get("type") == "number":
        raw = typed.get("number")
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None
    return parse_number(row_data.get(column))


def strip_internal_fields(row_data: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in row_data.items() if not is_internal_key(k)}
