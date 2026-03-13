"""All normalization logic: cell values, column/header names, and row_data helpers."""
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# "dmy" = day/month/year, "mdy" = month/day/year, "ymd" = year/month/day
DateFormatHint = str


INTERNAL_TYPED_KEY = "__typed__"
NULL_VALUES = {"null", "none", "na", "n/a", "nan", "-", ""}

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
) -> Dict[str, Any]:
    """Build row_data: keys are normalized column names; values are { original, normalized } or legacy plain normalized.
    When date_format_by_column is set, dates are parsed with that format and normalized value becomes ISO (YYYY-MM-DD).
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


def get_numeric_value(row_data: Dict[str, Any], column: str) -> Optional[float]:
    typed = get_typed_value(row_data, column)
    if typed and typed.get("type") == "number":
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
