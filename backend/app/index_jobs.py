from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from typing import Dict, List, Literal, TypedDict

IndexState = Literal["queued", "indexing", "ready", "error"]


class IndexJobStatus(TypedDict):
    dataset_id: int
    state: IndexState
    progress: float
    processed_rows: int
    total_rows: int
    message: str
    started_at: str | None
    updated_at: str
    finished_at: str | None


_jobs: Dict[int, IndexJobStatus] = {}
_lock = Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def queue_index_job(dataset_id: int, total_rows: int) -> None:
    now = _now_iso()
    with _lock:
        _jobs[dataset_id] = {
            "dataset_id": dataset_id,
            "state": "queued",
            "progress": 0.0,
            "processed_rows": 0,
            "total_rows": max(total_rows, 0),
            "message": "Queued for vector indexing.",
            "started_at": None,
            "updated_at": now,
            "finished_at": None,
        }


def start_index_job(dataset_id: int, total_rows: int) -> None:
    now = _now_iso()
    with _lock:
        current = _jobs.get(dataset_id)
        _jobs[dataset_id] = {
            "dataset_id": dataset_id,
            "state": "indexing",
            "progress": 0.0,
            "processed_rows": 0,
            "total_rows": max(total_rows, 0),
            "message": "Indexing vectors...",
            "started_at": current["started_at"] if current and current.get("started_at") else now,
            "updated_at": now,
            "finished_at": None,
        }


def update_index_job(dataset_id: int, processed_rows: int, total_rows: int) -> None:
    total = max(total_rows, 0)
    processed = max(processed_rows, 0)
    if total > 0:
        processed = min(processed, total)
    progress = (processed / total * 100.0) if total > 0 else 0.0
    now = _now_iso()

    with _lock:
        current = _jobs.get(dataset_id)
        _jobs[dataset_id] = {
            "dataset_id": dataset_id,
            "state": "indexing",
            "progress": progress,
            "processed_rows": processed,
            "total_rows": total,
            "message": "Indexing vectors...",
            "started_at": current["started_at"] if current and current.get("started_at") else now,
            "updated_at": now,
            "finished_at": None,
        }


def mark_index_job_ready(dataset_id: int, total_rows: int) -> None:
    total = max(total_rows, 0)
    now = _now_iso()
    with _lock:
        current = _jobs.get(dataset_id)
        _jobs[dataset_id] = {
            "dataset_id": dataset_id,
            "state": "ready",
            "progress": 100.0,
            "processed_rows": total,
            "total_rows": total,
            "message": "Vector index is ready.",
            "started_at": current["started_at"] if current and current.get("started_at") else now,
            "updated_at": now,
            "finished_at": now,
        }


def mark_index_job_error(dataset_id: int, total_rows: int, error_message: str) -> None:
    total = max(total_rows, 0)
    now = _now_iso()
    with _lock:
        current = _jobs.get(dataset_id)
        _jobs[dataset_id] = {
            "dataset_id": dataset_id,
            "state": "error",
            "progress": 100.0 if total == 0 else min(current["progress"], 99.9)
            if current
            else 0.0,
            "processed_rows": current["processed_rows"] if current else 0,
            "total_rows": total,
            "message": error_message or "Vector indexing failed.",
            "started_at": current["started_at"] if current and current.get("started_at") else now,
            "updated_at": now,
            "finished_at": now,
        }


def get_index_job(dataset_id: int) -> IndexJobStatus | None:
    with _lock:
        current = _jobs.get(dataset_id)
        return dict(current) if current else None


def get_index_jobs(dataset_ids: List[int]) -> Dict[int, IndexJobStatus]:
    with _lock:
        return {
            dataset_id: dict(_jobs[dataset_id])
            for dataset_id in dataset_ids
            if dataset_id in _jobs
        }


def clear_index_job(dataset_id: int) -> None:
    with _lock:
        _jobs.pop(dataset_id, None)
