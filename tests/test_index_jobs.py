"""Tests for app.index_jobs – in-memory job tracking."""

from app.index_jobs import (
    queue_index_job,
    start_index_job,
    update_index_job,
    mark_index_job_ready,
    mark_index_job_error,
    get_index_job,
    get_index_jobs,
    clear_index_job,
    _jobs,
)


def _cleanup():
    _jobs.clear()


def test_queue_index_job():
    _cleanup()
    queue_index_job(1, 100)
    job = get_index_job(1)
    assert job is not None
    assert job["state"] == "queued"
    assert job["total_rows"] == 100
    assert job["progress"] == 0.0
    _cleanup()


def test_start_index_job():
    _cleanup()
    queue_index_job(1, 50)
    start_index_job(1, 50)
    job = get_index_job(1)
    assert job["state"] == "indexing"
    assert job["started_at"] is not None
    _cleanup()


def test_start_index_job_preserves_started_at():
    _cleanup()
    queue_index_job(1, 50)
    start_index_job(1, 50)
    first_started = get_index_job(1)["started_at"]
    start_index_job(1, 50)
    assert get_index_job(1)["started_at"] == first_started
    _cleanup()


def test_update_index_job():
    _cleanup()
    queue_index_job(1, 100)
    start_index_job(1, 100)
    update_index_job(1, 50, 100)
    job = get_index_job(1)
    assert job["progress"] == 50.0
    assert job["processed_rows"] == 50
    _cleanup()


def test_update_index_job_clamps_processed():
    _cleanup()
    queue_index_job(1, 10)
    start_index_job(1, 10)
    update_index_job(1, 999, 10)
    job = get_index_job(1)
    assert job["processed_rows"] == 10
    _cleanup()


def test_mark_index_job_ready():
    _cleanup()
    queue_index_job(1, 20)
    start_index_job(1, 20)
    mark_index_job_ready(1, 20)
    job = get_index_job(1)
    assert job["state"] == "ready"
    assert job["progress"] == 100.0
    assert job["finished_at"] is not None
    _cleanup()


def test_mark_index_job_error_with_current():
    _cleanup()
    queue_index_job(1, 100)
    start_index_job(1, 100)
    update_index_job(1, 30, 100)
    mark_index_job_error(1, 100, "boom")
    job = get_index_job(1)
    assert job["state"] == "error"
    assert job["message"] == "boom"
    assert job["progress"] <= 99.9
    _cleanup()


def test_mark_index_job_error_no_current():
    _cleanup()
    mark_index_job_error(99, 0, "")
    job = get_index_job(99)
    assert job["state"] == "error"
    assert job["message"] == "Vector indexing failed."
    _cleanup()


def test_get_index_jobs_multiple():
    _cleanup()
    queue_index_job(1, 10)
    queue_index_job(2, 20)
    queue_index_job(3, 30)
    result = get_index_jobs([1, 3, 999])
    assert 1 in result
    assert 3 in result
    assert 999 not in result
    _cleanup()


def test_clear_index_job():
    _cleanup()
    queue_index_job(1, 10)
    clear_index_job(1)
    assert get_index_job(1) is None
    _cleanup()


def test_clear_index_job_nonexistent():
    _cleanup()
    clear_index_job(999)  # should not raise
    _cleanup()


def test_get_index_job_nonexistent():
    _cleanup()
    assert get_index_job(12345) is None
    _cleanup()


def test_update_zero_total():
    _cleanup()
    queue_index_job(1, 0)
    start_index_job(1, 0)
    update_index_job(1, 5, 0)
    job = get_index_job(1)
    assert job["progress"] == 0.0
    _cleanup()


def test_queue_negative_total():
    _cleanup()
    queue_index_job(1, -5)
    job = get_index_job(1)
    assert job["total_rows"] == 0
    _cleanup()
