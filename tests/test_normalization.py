"""Unit tests for app.normalization (headers, values, money, measurements, dates, row_data helpers)."""
import io
import json
import pytest
from unittest.mock import patch
from sqlalchemy import text

from app.normalization import (
    normalize_headers,
    normalize_text_value,
    parse_number,
    parse_money,
    _looks_like_money,
    is_money_column,
    infer_money_columns,
    parse_measurement,
    _looks_like_measurement,
    is_measurement_column,
    infer_measurement_columns,
    parse_date,
    parse_date_with_format,
    infer_column_date_format,
    infer_date_formats_for_columns,
    normalize_row_obj,
    get_normalized_value,
    get_original_value,
    get_typed_value,
    get_numeric_value,
    get_column_currency,
    strip_internal_fields,
    flatten_row_data_to_normalized,
    INTERNAL_TYPED_KEY,
)


def make_csv(content: str, filename: str = "test.csv"):
    return {"file": (filename, io.BytesIO(content.encode("utf-8")), "text/csv")}


# ─── Headers ──────────────────────────────────────────────────────────────────


def test_normalize_headers_strip_and_dedupe():
    assert normalize_headers(["a", "b", "a"]) == ["a", "b", "a_2"]


def test_normalize_headers_empty_becomes_col_index():
    assert normalize_headers(["", "x", "  "]) == ["col_1", "x", "col_3"]


def test_normalize_headers_collapse_internal_whitespace():
    assert normalize_headers(["foo   bar", "baz"]) == ["foo bar", "baz"]


def test_normalize_headers_strip_only():
    assert normalize_headers(["  a  ", "b"]) == ["a", "b"]


# ─── Text values ───────────────────────────────────────────────────────────────


def test_normalize_text_value_null_like():
    assert normalize_text_value("na") is None
    assert normalize_text_value("n/a") is None
    assert normalize_text_value("-") is None
    assert normalize_text_value("") is None


def test_normalize_text_value_collapse_whitespace():
    assert normalize_text_value("  a  b  ") == "a b"


def test_normalize_text_value_passthrough():
    assert normalize_text_value("Alice") == "Alice"


# ─── Numbers ───────────────────────────────────────────────────────────────────


def test_parse_number_integer():
    assert parse_number("42") == 42.0
    assert parse_number(42) == 42.0


def test_parse_number_decimal():
    assert parse_number("3.14") == 3.14


def test_parse_number_strips_currency_and_commas():
    assert parse_number("$1,234.56") == 1234.56


def test_parse_number_null_like_returns_none():
    assert parse_number("na") is None
    assert parse_number("") is None


# ─── Money ────────────────────────────────────────────────────────────────────


def test_parse_money_symbol_at_front():
    out = parse_money("$100")
    assert out is not None
    assert out[0] == "100.00"
    assert out[1] == 100.0
    assert out[2] == "USD"


def test_parse_money_with_comma():
    out = parse_money("$1,234.56")
    assert out is not None
    assert out[0] == "1234.56"
    assert out[1] == 1234.56
    assert out[2] == "USD"


def test_parse_money_eur():
    out = parse_money("€99.00")
    assert out is not None
    assert out[2] == "EUR"


def test_parse_money_no_symbol_returns_none():
    assert parse_money("100") is None
    assert parse_money("100 kg") is None


def test_looks_like_money_symbol():
    assert _looks_like_money("$100") is True
    assert _looks_like_money("€ 1,000") is True


def test_looks_like_money_code_front_or_end():
    assert _looks_like_money("USD 100") is True
    assert _looks_like_money("100 USD") is True


def test_looks_like_money_plain_number_false():
    assert _looks_like_money("100") is False
    assert _looks_like_money("100 kg") is False


def test_is_money_column_any_money_like():
    # No threshold: column is money if any value looks like money (handles many nulls)
    assert is_money_column(["$10", "$20", "other"]) is True
    assert is_money_column(["$10", "—", "", None]) is True
    assert is_money_column(["a", "b", "c"]) is False


def test_infer_money_columns_empty():
    assert infer_money_columns(["a", "b"], []) == set()


def test_infer_money_columns_detects_column():
    headers = ["product", "price"]
    rows = [["A", "$10"], ["B", "$20"], ["C", "$30"]]
    assert infer_money_columns(headers, rows) == {1}


# ─── Measurements ──────────────────────────────────────────────────────────────


def test_parse_measurement_number_unit():
    out = parse_measurement("100 kg")
    assert out is not None
    assert out[0] == "100 kg"
    assert out[1] == 100.0
    assert out[2] == "kg"


def test_parse_measurement_unit_number():
    out = parse_measurement("kg 100")
    assert out is not None
    assert out[1] == 100.0
    assert out[2] == "kg"


def test_parse_measurement_converts_to_standard():
    out = parse_measurement("1000 g")
    assert out is not None
    assert out[0] == "1 kg"
    assert out[1] == 1.0
    assert out[2] == "kg"


def test_looks_like_measurement():
    assert _looks_like_measurement("100 kg") is True
    assert _looks_like_measurement("100") is False


def test_is_measurement_column():
    assert is_measurement_column(["100 kg", "200 kg", "50 m"], threshold=0.6) is True
    assert is_measurement_column(["a", "b", "c"], threshold=0.5) is False


def test_infer_measurement_columns():
    headers = ["id", "weight"]
    rows = [["1", "10 kg"], ["2", "20 kg"], ["3", "30 kg"]]
    assert infer_measurement_columns(headers, rows) == {1}


# ─── Dates ───────────────────────────────────────────────────────────────────


def test_parse_date_iso():
    out = parse_date("2022-01-15")
    assert out is not None
    assert out["iso_date"] == "2022-01-15"


def test_parse_date_with_format_dmy():
    out = parse_date_with_format("15/01/2022", "dmy")
    assert out is not None
    assert out["iso_date"] == "2022-01-15"


def test_parse_date_with_format_mdy():
    out = parse_date_with_format("01/15/2022", "mdy")
    assert out is not None
    assert out["iso_date"] == "2022-01-15"


def test_infer_column_date_format_disambiguate():
    assert infer_column_date_format(["13/01/2022", "15/02/2022"]) == "dmy"
    assert infer_column_date_format(["01/13/2022", "02/15/2022"]) == "mdy"


def test_infer_date_formats_for_columns():
    headers = ["date", "x"]
    rows = [["2022-01-01", "a"], ["02/03/2022", "b"]]
    out = infer_date_formats_for_columns(headers, rows)
    assert isinstance(out, dict)
    # Column 0 has ISO and ambiguous dates → may infer "dmy" or "mdy" for the ambiguous one
    assert out.get(0) in (None, "dmy", "mdy")
    # Column 1 is non-date text
    assert out.get(1) is None


# ─── normalize_row_obj and row_data helpers ───────────────────────────────────


def test_normalize_row_obj_plain_text_and_number():
    out = normalize_row_obj(["name", "age"], ["Alice", "30"], store_original=True)
    assert out["name"]["original"] == "Alice"
    assert out["name"]["normalized"] == "Alice"
    assert out["age"]["original"] == "30"
    assert out["age"]["normalized"] == "30"
    assert out[INTERNAL_TYPED_KEY]["age"]["type"] == "number"
    assert out[INTERNAL_TYPED_KEY]["age"]["number"] == 30.0


def test_normalize_row_obj_date_iso():
    out = normalize_row_obj(
        ["d"],
        ["2022-01-04"],
        store_original=True,
    )
    assert out["d"]["original"] == "2022-01-04"
    assert out["d"]["normalized"] == "2022-01-04"
    assert out[INTERNAL_TYPED_KEY]["d"]["type"] == "date"


def test_normalize_row_obj_money_when_column_marked():
    out = normalize_row_obj(
        ["product", "price"],
        ["Widget", "$1,234.56"],
        store_original=True,
        money_columns={1},
    )
    assert out["price"]["original"] == "$1,234.56"
    assert out["price"]["normalized"] == "1234.56"
    assert out[INTERNAL_TYPED_KEY]["price"]["type"] == "money"
    assert out[INTERNAL_TYPED_KEY]["price"]["currency"] == "USD"
    assert out[INTERNAL_TYPED_KEY]["price"]["number"] == 1234.56


def test_normalize_row_obj_measurement_when_column_marked():
    out = normalize_row_obj(
        ["id", "weight"],
        ["1", "100 kg"],
        store_original=True,
        measurement_columns={1},
    )
    assert out["weight"]["original"] == "100 kg"
    assert out["weight"]["normalized"] == "100 kg"
    assert out[INTERNAL_TYPED_KEY]["weight"]["type"] == "measurement"
    assert out[INTERNAL_TYPED_KEY]["weight"]["unit"] == "kg"


def test_get_normalized_value():
    row = {"a": {"original": "x", "normalized": "y"}}
    assert get_normalized_value(row, "a") == "y"
    assert get_normalized_value({"a": "plain"}, "a") == "plain"


def test_get_original_value():
    row = {"a": {"original": "x", "normalized": "y"}}
    assert get_original_value(row, "a") == "x"


def test_get_numeric_value_from_typed():
    row = {"x": "42", INTERNAL_TYPED_KEY: {"x": {"type": "number", "number": 42.0}}}
    assert get_numeric_value(row, "x") == 42.0


def test_get_numeric_value_money():
    row = {
        "p": {"original": "$10", "normalized": "10.00"},
        INTERNAL_TYPED_KEY: {"p": {"type": "money", "number": 10.0, "currency": "USD"}},
    }
    assert get_numeric_value(row, "p") == 10.0


def test_get_column_currency():
    row = {
        INTERNAL_TYPED_KEY: {"p": {"type": "money", "number": 10.0, "currency": "USD"}},
    }
    assert get_column_currency(row, "p") == "USD"
    assert get_column_currency(row, "other") is None


def test_strip_internal_fields():
    row = {"a": 1, "__typed__": {"a": {"type": "number"}}}
    assert strip_internal_fields(row) == {"a": 1}


def test_flatten_row_data_to_normalized():
    row = {"a": {"original": "x", "normalized": "y"}, "b": "z"}
    assert flatten_row_data_to_normalized(row) == {"a": "y", "b": "z"}


# ─── Integration: DB row_data shape (moved from test_ingest) ───────────────────


def test_db_rows_stored(client, test_engine):
    """Row data stores { original, normalized } per column after ingest."""
    client.post("/ingest", files=make_csv("name,age\nAlice,30\n"))
    with test_engine.connect() as conn:
        rows = conn.execute(text("SELECT row_data FROM dataset_rows")).fetchall()
    assert len(rows) == 1
    data = json.loads(rows[0].row_data)
    assert data["name"]["normalized"] == "Alice"
    assert data["name"]["original"] == "Alice"
    assert data["age"]["normalized"] == "30"
    assert data["age"]["original"] == "30"


def test_db_rows_money_normalized(client, test_engine):
    """Money cells get canonical normalized string and __typed__ with type money + currency."""
    # Quote price so comma in $1,234.56 is not treated as CSV delimiter
    client.post(
        "/ingest",
        files=make_csv('product,price\nWidget,"$1,234.56"\nGadget,€99.00\n'),
    )
    with test_engine.connect() as conn:
        rows = conn.execute(text("SELECT row_data FROM dataset_rows ORDER BY id")).fetchall()
    assert len(rows) == 2
    typed = json.loads(rows[0].row_data).get("__typed__", {})
    assert typed.get("price", {}).get("type") == "money"
    assert typed["price"].get("currency") == "USD"
    assert typed["price"].get("number") == 1234.56
    data0 = json.loads(rows[0].row_data)
    assert data0["price"]["original"] == "$1,234.56"
    assert data0["price"]["normalized"] == "1234.56"
    data1 = json.loads(rows[1].row_data)
    assert data1["price"]["original"] == "€99.00"
    assert data1["price"]["normalized"] == "99.00"
    assert json.loads(rows[1].row_data).get("__typed__", {}).get("price", {}).get("currency") == "EUR"
