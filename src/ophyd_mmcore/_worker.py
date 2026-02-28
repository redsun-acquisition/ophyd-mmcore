from __future__ import annotations

import asyncio
from concurrent.futures import Future
from queue import SimpleQueue
from threading import Thread
from typing import TYPE_CHECKING, Any

from pymmcore_plus import CMMCorePlus

if TYPE_CHECKING:
    from collections.abc import Callable


class MMCoreWorker:
    """A single background thread that serialises all CMMCorePlus calls.

    Parameters
    ----------
    core:
        The ``CMMCorePlus`` instance to wrap.  If *None* a new singleton is
        created.  Ownership transfers to the worker — do not call blocking
        MMCore methods from any other thread after handing the instance in.
    """

    _SENTINEL = object()  # signals the worker to stop

    def __init__(self, core: CMMCorePlus | None = None) -> None:
        self._core = core or CMMCorePlus.instance()
        self._queue: SimpleQueue[tuple[Callable[[], Any], Future[Any]] | object] = (
            SimpleQueue()
        )
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

    def submit(self, fn: Callable[[], Any]) -> Future[Any]:
        """Schedule *fn* on the worker thread and return a Future for its result.

        The returned ``concurrent.futures.Future`` can be awaited inside an
        asyncio coroutine via ``await asyncio.wrap_future(fut)``.
        """
        fut: Future[Any] = Future()
        self._queue.put((fn, fut))
        return fut

    async def run(self, fn: Callable[[], Any]) -> Any:
        """Submit *fn* to the worker thread and await its result."""
        return await asyncio.wrap_future(self.submit(fn))

    def stop(self) -> None:
        """Gracefully stop the worker thread."""
        self._queue.put(self._SENTINEL)
        self._thread.join()

    def _run_loop(self) -> None:
        while True:
            item = self._queue.get()
            if item is self._SENTINEL:
                return
            fn, fut = item  # type: ignore[misc]
            if fut.set_running_or_notify_cancel():
                try:
                    fut.set_result(fn())
                except Exception as exc:  # noqa: BLE001
                    fut.set_exception(exc)
