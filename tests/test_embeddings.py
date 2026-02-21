from app.embeddings import row_to_text


def test_row_to_text_basic():
    row = {"name": "Alice", "age": "30", "city": "London"}
    result = row_to_text(row)
    assert result == "name: Alice | age: 30 | city: London"


def test_row_to_text_skips_none_and_empty():
    row = {"name": "Alice", "age": None, "city": "", "country": "UK"}
    result = row_to_text(row)
    assert result == "name: Alice | country: UK"


def test_row_to_text_all_empty():
    row = {"name": None, "age": "", "city": None}
    result = row_to_text(row)
    assert result == ""


def test_row_to_text_single_field():
    row = {"name": "Bob"}
    result = row_to_text(row)
    assert result == "name: Bob"


def test_row_to_text_preserves_numeric_strings():
    row = {"id": "123", "score": "99.5"}
    result = row_to_text(row)
    assert result == "id: 123 | score: 99.5"
