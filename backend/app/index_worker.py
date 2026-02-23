from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass
from threading import Condition, Thread
from typing import Callable, Deque, List, Set

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _QueuedIndexJob:
    dataset_id: int
    total_rows: int


class IndexWorker:
    """In-memory queue for dataset indexing jobs with configurable worker parallelism."""

    def __init__(self, processor: Callable[[int, int], None], worker_count: int = 1) -> None:
        self._processor = processor
        self._worker_count = max(1, worker_count)
        self._condition = Condition()
        self._queue: Deque[_QueuedIndexJob] = deque()
        self._queued_ids: Set[int] = set()
        self._active_dataset_ids: Set[int] = set()
        self._threads: List[Thread] = []
        self._stopping = False

    def start(self) -> None:
        with self._condition:
            if self._threads and any(thread.is_alive() for thread in self._threads):
                return
            self._stopping = False
            self._threads = []
            for idx in range(self._worker_count):
                thread = Thread(
                    target=self._run,
                    name=f"index-worker-{idx + 1}",
                    daemon=True,
                )
                thread.start()
                self._threads.append(thread)

    def stop(self, timeout_seconds: float = 5.0) -> None:
        threads: List[Thread]
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
            threads = list(self._threads)

        for thread in threads:
            thread.join(timeout=timeout_seconds)

    def enqueue(self, dataset_id: int, total_rows: int) -> bool:
        """Queue a dataset for indexing if it is not already queued/running."""
        with self._condition:
            if dataset_id in self._active_dataset_ids or dataset_id in self._queued_ids:
                return False

            self._queue.append(
                _QueuedIndexJob(dataset_id=dataset_id, total_rows=max(total_rows, 0))
            )
            self._queued_ids.add(dataset_id)
            self._condition.notify()
            return True

    def _run(self) -> None:
        while True:
            with self._condition:
                while not self._queue and not self._stopping:
                    self._condition.wait()

                if self._stopping and not self._queue:
                    return

                job = self._queue.popleft()
                self._queued_ids.discard(job.dataset_id)
                self._active_dataset_ids.add(job.dataset_id)

            try:
                self._processor(job.dataset_id, job.total_rows)
            except Exception:
                logger.exception(
                    "Index worker failed for dataset_id=%s",
                    job.dataset_id,
                )
            finally:
                with self._condition:
                    self._active_dataset_ids.discard(job.dataset_id)
