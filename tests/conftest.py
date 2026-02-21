import sys
import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

import app.db as app_db
import app.main as app_main
from app.models import Base

@pytest.fixture(scope="session")
def test_engine():
    engine = create_engine(
        #use sqlite:///.test_debug.db" if you want to see the contents of the DB after tests run. Use DB Browswer for SQLite and open test_debug.db
        # "sqlite:///./test_debug.db", 
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,  # forces all connections to share the same in-memory DB
    )

    app_db.engine = engine
    app_main.engine = engine
    app_db.SessionLocal.configure(bind=engine)
    app_main.SessionLocal.configure(bind=engine)

    Base.metadata.create_all(bind=engine)

    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def client(test_engine):
    with TestClient(app_main.app) as c:
        yield c


@pytest.fixture(autouse=True)
def clean_tables(test_engine):
    yield
    #comment code below if you wanna see the contents of the DB after tests run. This is useful for debugging test failures and inspecting the state of the DB after ingestion.
    with test_engine.connect() as conn:
        conn.execute(text("DELETE FROM dataset_rows"))
        conn.execute(text("DELETE FROM dataset_columns"))
        conn.execute(text("DELETE FROM datasets"))
        conn.commit()