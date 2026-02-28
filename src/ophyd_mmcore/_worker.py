from __future__ import annotations

import asyncio
from concurrent.futures import Future
from dataclasses import dataclass
from queue import SimpleQueue
from threading import Thread
from typing import Any

from pymmcore_plus import CMMCorePlus


@dataclass
class _WorkItem:
    """A unit of work submitted to the MMCoreWorker queue."""

    fn: Any  # Callable[[], Any] — kept as Any to avoid generic variance issues
    fut: Future[Any]


class _Sentinel:
    """Unique type used to signal the worker thread to stop."""


_STOP = _Sentinel()


class MMCoreWorker:
    """A single background thread that serialises all CMMCorePlus calls.

    Parameters
    ----------
    core:
        The ``CMMCorePlus`` instance to wrap.  If *None* a new singleton is
        created.  Ownership transfers to the worker — do not call blocking
        MMCore methods from any other thread after handing the instance in.
    """

    def __init__(self, core: CMMCorePlus | None = None) -> None:
        self._core = core or CMMCorePlus.instance()
        self._queue: SimpleQueue[_WorkItem | _Sentinel] = SimpleQueue()
        self._thread = Thread(
            target=self._run_loop,
            name="MMCoreWorker",
            daemon=True,
        )
        self._thread.start()

    @property
    def core(self) -> CMMCorePlus:
        """The wrapped CMMCorePlus instance (read-only reference)."""
        return self._core

    def submit(self, fn: Any) -> Future[Any]:
        """Schedule *fn* on the worker thread and return a Future for its result.

        The returned ``concurrent.futures.Future`` can be awaited inside an
        asyncio coroutine via ``await asyncio.wrap_future(fut)``.
        """
        fut: Future[Any] = Future()
        self._queue.put(_WorkItem(fn=fn, fut=fut))
        return fut

    async def run(self, fn: Any) -> Any:
        """Submit *fn* to the worker thread and await its result."""
        return await asyncio.wrap_future(self.submit(fn))

    def stop(self) -> None:
        """Gracefully stop the worker thread."""
        self._queue.put(_STOP)
        self._thread.join()

    def _run_loop(self) -> None:
        while True:
            item = self._queue.get()
            if isinstance(item, _Sentinel):
                return
            if item.fut.set_running_or_notify_cancel():
                try:
                    item.fut.set_result(item.fn())
                except Exception as exc:
                    item.fut.set_exception(exc)
