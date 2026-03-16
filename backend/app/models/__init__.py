from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    UniqueConstraint,
    false,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.db import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    provider = Column(String(32), nullable=False)  # "github" or "google"
    provider_id = Column(String(255), nullable=False)
    email = Column(String(320), nullable=True)
    name = Column(String(255), nullable=False)
    avatar_url = Column(String(1024), nullable=True, default="")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    datasets = relationship("Dataset", back_populates="owner")

    __table_args__ = (
        UniqueConstraint("provider", "provider_id", name="uq_users_provider_id"),
        Index("ix_users_provider_provider_id", "provider", "provider_id"),
    )


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=True)
    name = Column(String(255), nullable=False)
    source_filename = Column(String(512), nullable=True)
    delimiter = Column(String(8), nullable=False, default=",")
    has_header = Column(Boolean, nullable=False, default=True)
    row_count = Column(Integer, nullable=False, default=0)
    column_count = Column(Integer, nullable=False, default=0)
    is_index_ready = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default=false(),
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    owner = relationship("User", back_populates="datasets")
    columns = relationship("DatasetColumn", back_populates="dataset", cascade="all, delete-orphan")
    rows = relationship("DatasetRow", back_populates="dataset", cascade="all, delete-orphan")


class DatasetColumn(Base):
    __tablename__ = "dataset_columns"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    column_index = Column(Integer, nullable=False)
    original_name = Column(String(512), nullable=True)  # raw header from CSV
    normalized_name = Column(String(255), nullable=False)  # deduped, safe key for row_data

    dataset = relationship("Dataset", back_populates="columns")

    __table_args__ = (
        UniqueConstraint("dataset_id", "column_index", name="uq_dataset_columns_index"),
        UniqueConstraint("dataset_id", "normalized_name", name="uq_dataset_columns_name"),
        Index("ix_dataset_columns_dataset_id", "dataset_id"),
    )


class DatasetRow(Base):
    __tablename__ = "dataset_rows"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    row_index = Column(Integer, nullable=False)
    row_data = Column(JSON, nullable=False)

    dataset = relationship("Dataset", back_populates="rows")

    __table_args__ = (
        UniqueConstraint("dataset_id", "row_index", name="uq_dataset_rows_index"),
        Index("ix_dataset_rows_dataset_id", "dataset_id"),
    )


__all__ = ["Base", "User", "Dataset", "DatasetColumn", "DatasetRow"]
