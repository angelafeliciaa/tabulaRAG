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


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(String(1024), nullable=True)
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

    columns = relationship("DatasetColumn", back_populates="dataset", cascade="all, delete-orphan")
    rows = relationship("DatasetRow", back_populates="dataset", cascade="all, delete-orphan")


class DatasetColumn(Base):
    __tablename__ = "dataset_columns"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    column_index = Column(Integer, nullable=False)
    name = Column(String(255), nullable=False)

    dataset = relationship("Dataset", back_populates="columns")

    __table_args__ = (
        UniqueConstraint("dataset_id", "column_index", name="uq_dataset_columns_index"),
        UniqueConstraint("dataset_id", "name", name="uq_dataset_columns_name"),
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


__all__ = ["Base", "Dataset", "DatasetColumn", "DatasetRow"]
