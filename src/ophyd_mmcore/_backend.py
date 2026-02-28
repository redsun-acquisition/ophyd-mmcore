from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from ophyd_async.core import (
    SignalBackend,
    SignalDatatypeT,
    make_datakey,
)
from ophyd_async.core._signal_backend import make_metadata

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future
    from typing import Any

    from bluesky.protocols import Descriptor, Reading
    from ophyd_async.core import Callback

    from ._worker import MMCoreWorker


class MMPropertyBackend(SignalBackend[SignalDatatypeT]):
    """ophyd-async ``SignalBackend`` for a single Micro-Manager device property.

    Parameters
    ----------
    device_label:
        The MM device label (e.g. ``"Camera"``).
    property_name:
        The MM property name (e.g. ``"Exposure"``).
    worker:
        The shared ``MMCoreWorker``.  Inject one instance per application so
        that all backends serialise through the same thread.
    datatype:
        The Python type for signal values.  Inferred from ``PropertyType`` if
        not given.
    """

    def __init__(
        self,
        device_label: str,
        property_name: str,
        worker: MMCoreWorker,
        datatype: type[SignalDatatypeT] | None = None,
    ) -> None:
        self._dev = device_label
        self._prop = property_name
        self._worker = worker

        if datatype is None:
            pt = worker.core.getPropertyType(device_label, property_name)
            datatype = pt.to_python() or str

        self._reading_callback: Callback[Reading[SignalDatatypeT]] | None = None
        # The psygnal slot we connected (kept so we can disconnect it later)
        self._psygnal_slot: Callable[..., None] | None = None
        # We need the event loop when wiring callbacks; captured on connect()
        self._loop: asyncio.AbstractEventLoop | None = None

        super().__init__(datatype)

    # ------------------------------------------------------------------
    # SignalBackend interface
    # ------------------------------------------------------------------

    def source(self, name: str, read: bool) -> str:
        return f"mmcore://{self._dev}/{self._prop}"

    async def connect(self, timeout: float) -> None:
        self._loop = asyncio.get_event_loop()
        # Verify the property is reachable; raises if not
        await self._worker.run(
            lambda: self._worker.core.getProperty(self._dev, self._prop)
        )

    async def put(self, value: SignalDatatypeT | None) -> None:
        await self._worker.run(
            lambda: self._worker.core.setProperty(self._dev, self._prop, value)
        )

    async def get_value(self) -> SignalDatatypeT:
        raw = await self._worker.run(
            lambda: self._worker.core.getProperty(self._dev, self._prop)
        )
        return self.datatype(raw) if self.datatype else raw  # type: ignore[call-arg]

    async def get_setpoint(self) -> SignalDatatypeT:
        # MM has no separate setpoint concept; readback == setpoint
        return await self.get_value()

    async def get_reading(self) -> Reading[SignalDatatypeT]:
        value = await self.get_value()
        return {
            "value": value,
            "timestamp": time.time(),
            "alarm_severity": 0,
        }

    async def get_datakey(self, source: str) -> Descriptor:
        value = await self.get_value()
        core = self._worker.core
        metadata = make_metadata(self.datatype)

        if core.hasPropertyLimits(self._dev, self._prop):
            lo = core.getPropertyLowerLimit(self._dev, self._prop)
            hi = core.getPropertyUpperLimit(self._dev, self._prop)
            metadata["limits"]["control"] = {"low": lo, "high": hi}
            metadata["limits"]["display"] = {"low": lo, "high": hi}

        if allowed := core.getAllowedPropertyValues(self._dev, self._prop):
            metadata["choices"] = list(allowed)

        return make_datakey(self.datatype, value, source, metadata)

    def set_callback(self, callback: Callback[Reading[SignalDatatypeT]] | None) -> None:
        """Wire or unwire the per-property psygnal change signal.

        The psygnal slot fires on the worker thread, so the Reading is
        delivered back to the asyncio loop via ``call_soon_threadsafe``.
        """
        core = self._worker.core

        # Always disconnect the existing slot first
        if self._psygnal_slot is not None:
            prop_sig = core.events.devicePropertyChanged(self._dev, self._prop)
            prop_sig.disconnect(self._psygnal_slot)
            self._psygnal_slot = None
        self._reading_callback = None

        if callback is None:
            return

        loop = self._loop
        if loop is None:
            raise RuntimeError(
                "set_callback() called before connect(); "
                "the event loop has not been captured yet."
            )

        datatype = self.datatype

        def _on_property_changed(_dev: str, _prop: str, new_value: str) -> None:
            # Runs on the worker thread — marshal into the asyncio loop
            value = datatype(new_value) if datatype else new_value
            reading: Reading[SignalDatatypeT] = {
                "value": value,
                "timestamp": time.time(),
                "alarm_severity": 0,
            }
            loop.call_soon_threadsafe(callback, reading)

        prop_sig = core.events.devicePropertyChanged(self._dev, self._prop)
        prop_sig.connect(_on_property_changed)
        self._psygnal_slot = _on_property_changed
        self._reading_callback = callback

        # Immediately deliver the current value so the subscriber is
        # synchronised on registration (matches SoftSignalBackend behaviour)
        self.submit(
            lambda: loop.call_soon_threadsafe(
                callback,
                {
                    "value": datatype(core.getProperty(self._dev, self._prop))
                    if datatype
                    else core.getProperty(self._dev, self._prop),
                    "timestamp": time.time(),
                    "alarm_severity": 0,
                },
            )
        )

    def submit(self, fn: Callable[[], Any]) -> Future[Any]:
        """Direct access to the worker queue (for subclasses or tests)."""
        return self._worker.submit(fn)
