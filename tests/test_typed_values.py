"""Tests for app.normalization – parsing, normalization, typed extraction."""

from app.normalization import (
    is_internal_key,
    normalize_text_value,
    parse_number,
    parse_date,
    normalize_row_obj,
    get_typed_value,
    get_numeric_value,
    strip_internal_fields,
    INTERNAL_TYPED_KEY,
)


# ── is_internal_key ───────────────────────────────────────────────


def test_internal_key():
    assert is_internal_key("__typed__") is True
    assert is_internal_key("__hidden") is True
    assert is_internal_key("name") is False
    assert is_internal_key("_single") is False


# ── normalize_text_value ──────────────────────────────────────────


def test_normalize_text_none():
    assert normalize_text_value(None) is None


def test_normalize_text_null_strings():
    for val in ["null", "None", "NA", "n/a", "NaN", "-", ""]:
        assert normalize_text_value(val) is None


def test_normalize_text_whitespace():
    assert normalize_text_value("  hello   world  ") == "hello world"


def test_normalize_text_nbsp():
    assert normalize_text_value("hello\xa0world") == "hello world"


# ── parse_number ──────────────────────────────────────────────────


def test_parse_number_int():
    assert parse_number(42) == 42.0


def test_parse_number_float():
    assert parse_number(3.14) == 3.14


def test_parse_number_string():
    assert parse_number("123") == 123.0


def test_parse_number_currency():
    assert parse_number("$1,000") == 1000.0
    assert parse_number("€2,500.50") == 2500.5


def test_parse_number_suffix_k():
    assert parse_number("10k") == 10_000.0


def test_parse_number_suffix_m():
    assert parse_number("2.5M") == 2_500_000.0


def test_parse_number_suffix_b():
    assert parse_number("1B") == 1_000_000_000.0


def test_parse_number_percentage():
    assert parse_number("85%") == 85.0


def test_parse_number_parentheses_negative():
    assert parse_number("(100)") == -100.0


def test_parse_number_none():
    assert parse_number(None) is None


def test_parse_number_bool():
    assert parse_number(True) is None


def test_parse_number_null_strings():
    for val in ["na", "n/a", "nan", "none", "null"]:
        assert parse_number(val) is None


def test_parse_number_invalid():
    assert parse_number("hello") is None


def test_parse_number_empty():
    assert parse_number("") is None


# ── parse_date ────────────────────────────────────────────────────


def test_parse_date_iso():
    result = parse_date("2024-01-15")
    assert result is not None
    assert result["iso_date"] == "2024-01-15"


def test_parse_date_iso_with_time():
    result = parse_date("2024-01-15 10:30:00")
    assert result is not None
    assert result["iso_date"] == "2024-01-15"


def test_parse_date_dmy():
    result = parse_date("15/06/2024")
    assert result is not None
    assert result["iso_date"] == "2024-06-15"


def test_parse_date_short_year():
    result = parse_date("15/06/24")
    assert result is not None
    assert "2024" in result["iso_date"]


def test_parse_date_mdy_unambiguous():
    # 01/15/2024 – month=1, day=15 (15 > 12 so must be day)
    result = parse_date("01/15/2024")
    assert result is not None
    assert result["iso_date"] == "2024-01-15"


def test_parse_date_none():
    assert parse_date(None) is None


def test_parse_date_empty():
    assert parse_date("") is None


def test_parse_date_invalid():
    assert parse_date("not-a-date") is None


def test_parse_date_invalid_values():
    assert parse_date("2024-13-01") is None  # month 13


def test_parse_date_iso_with_t():
    result = parse_date("2024-01-15T10:30")
    assert result is not None


def test_parse_date_dot_separator():
    result = parse_date("2024.06.15")
    assert result is not None
    assert result["iso_date"] == "2024-06-15"


def test_parse_date_isoformat_with_tz():
    result = parse_date("2024-01-15T10:30:00Z")
    assert result is not None
    assert result["iso_date"] == "2024-01-15"


# ── normalize_row_obj ─────────────────────────────────────────────


def test_normalize_row_obj_basic():
    result = normalize_row_obj(["name", "age"], ["Alice", "30"])
    assert result["name"]["normalized"] == "Alice"
    assert result["age"]["normalized"] == "30"
    assert INTERNAL_TYPED_KEY in result
    assert result[INTERNAL_TYPED_KEY]["age"]["type"] == "number"


def test_normalize_row_obj_short_row():
    result = normalize_row_obj(["a", "b", "c"], ["x"])
    assert result["a"]["normalized"] == "x"
    assert result["b"]["normalized"] is None
    assert result["c"]["normalized"] is None


def test_normalize_row_obj_no_typed():
    result = normalize_row_obj(["name"], ["Alice"])
    assert INTERNAL_TYPED_KEY not in result


# ── get_typed_value / get_numeric_value ───────────────────────────


def test_get_typed_value():
    row = {"age": "30", INTERNAL_TYPED_KEY: {"age": {"type": "number", "number": 30.0}}}
    assert get_typed_value(row, "age")["type"] == "number"
    assert get_typed_value(row, "missing") is None


def test_get_typed_value_no_typed():
    assert get_typed_value({"name": "Alice"}, "name") is None


def test_get_typed_value_non_dict_typed():
    assert get_typed_value({INTERNAL_TYPED_KEY: "not a dict"}, "x") is None


def test_get_numeric_value():
    row = {"amt": "100", INTERNAL_TYPED_KEY: {"amt": {"type": "number", "number": 100.0}}}
    assert get_numeric_value(row, "amt") == 100.0


def test_get_numeric_value_fallback_parse():
    row = {"amt": "$1,500"}
    assert get_numeric_value(row, "amt") == 1500.0


def test_get_numeric_value_none():
    assert get_numeric_value({}, "missing") is None


def test_get_numeric_value_bad_typed():
    row = {INTERNAL_TYPED_KEY: {"x": {"type": "number", "number": "not_a_number"}}}
    assert get_numeric_value(row, "x") is None


# ── strip_internal_fields ─────────────────────────────────────────


def test_strip_internal_fields():
    row = {"name": "Alice", "__typed__": {"name": {}}, "__other": "x"}
    result = strip_internal_fields(row)
    assert "name" in result
    assert "__typed__" not in result
    assert "__other" not in result
