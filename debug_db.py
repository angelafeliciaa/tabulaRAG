#!/usr/bin/env python3
"""Debug script to inspect the test database."""

import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent / "backend"))

from sqlalchemy import create_engine, text, inspect

# Use the same in-memory DB URL as tests
TEST_DATABASE_URL = "sqlite://"

engine = create_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
)

# Create tables
from app.models import Base
Base.metadata.create_all(bind=engine)

# Inspect the database
inspector = inspect(engine)

print("=" * 60)
print("Database Tables:")
print("=" * 60)
tables = inspector.get_table_names()
print(f"Tables: {tables}\n")

for table in tables:
    print(f"\n--- Table: {table} ---")
    columns = inspector.get_columns(table)
    print(f"Columns:")
    for col in columns:
        print(f"  - {col['name']}: {col['type']}")

print("\n" + "=" * 60)
print("Table Contents:")
print("=" * 60)

with engine.connect() as conn:
    for table in tables:
        result = conn.execute(text(f"SELECT * FROM {table}"))
        rows = result.fetchall()
        print(f"\n{table}: {len(rows)} rows")
        if rows:
            for row in rows:
                print(f"  {row}")
