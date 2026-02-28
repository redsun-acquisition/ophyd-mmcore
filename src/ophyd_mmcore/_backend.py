from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, cast

from ophyd_async.core import SignalBackend, make_datakey
from ophyd_async.core._signal_backend import Primitive, PrimitiveT, make_metadata

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future

    from bluesky.protocols import Reading
    from event_model import DataKey
    from ophyd_async.core import Callback

    from ._worker import MMCoreWorker


class MMPropertyBackend(SignalBackend[PrimitiveT]):
    """ophyd-async `SignalBackend` for a single Micro-Manager device property.

    Micro-Manager properties are always one of ``str``, ``float``, or ``int``
    (i.e. [`data`][ophyd_async.core.Primitive]), so this backend is typed over
    `PrimitiveT` rather than the full `SignalDatatypeT`.

    Parameters
    ----------
    device_label: str
        The MM device label.
    property_name: str
        The MM property name.
    worker: MMCoreWorker
        The shared `MMCoreWorker`.  Inject one instance per application so
        that all backends serialise through the same thread.
    datatype: type[PrimitiveT] | None
        The Python type for signal values.  Inferred from `PropertyType` if
        not given (falls back to `str` for undefined properties).
    """

    def __init__(
        self,
        device_label: str,
        property_name: str,
        worker: MMCoreWorker,
        datatype: type[PrimitiveT] | None = None,
    ) -> None:
        self._dev = device_label
        self._prop = property_name
        self._worker = worker

        if datatype is None:
            pt = worker.core.getPropertyType(device_label, property_name)
            # to_python() returns str | float | int | None; fall back to str
            inferred: type[Primitive] = pt.to_python() or str
            datatype = cast("type[PrimitiveT]", inferred)

        self._reading_callback: Callback[Reading[PrimitiveT]] | None = None
        # The psygnal slot we connected (kept so we can disconnect it later)
        self._psygnal_slot: Callable[..., None] | None = None
        # We need the event loop when wiring callbacks; captured on connect()
        self._loop: asyncio.AbstractEventLoop | None = None

        super().__init__(datatype)

    # ------------------------------------------------------------------
    # SignalBackend interface
    # ------------------------------------------------------------------

    def source(self, name: str, read: bool) -> str:
        """Return the source URI for this signal."""
        return f"mmcore://{self._dev}/{self._prop}"

    async def connect(self, timeout: float) -> None:
        """Connect to the device property, verifying it is reachable."""
        self._loop = asyncio.get_event_loop()
        await self._worker.run(
            lambda: self._worker.core.getProperty(self._dev, self._prop)
        )

    async def put(self, value: PrimitiveT | None) -> None:
        """Write *value* to the MM property."""
        # setProperty accepts bool | float | int | str — PrimitiveT satisfies this
        await self._worker.run(
            lambda: self._worker.core.setProperty(
                self._dev, self._prop, cast("Primitive", value)
            )
        )

    async def get_value(self) -> PrimitiveT:
        """Return the current property value cast to the declared datatype."""
        assert self.datatype is not None
        raw: str = await self._worker.run(
            lambda: self._worker.core.getProperty(self._dev, self._prop)
        )
        return cast("PrimitiveT", self.datatype(raw))

    async def get_setpoint(self) -> PrimitiveT:
        """Return the setpoint (same as readback for MM properties)."""
        return await self.get_value()

    async def get_reading(self) -> Reading[PrimitiveT]:
        """Return a Bluesky Reading with the current value and timestamp."""
        value = await self.get_value()
        return {
            "value": value,
            "timestamp": time.time(),
            "alarm_severity": 0,
        }

    async def get_datakey(self, source: str) -> DataKey:
        """Return event-model DataKey metadata for this signal."""
        assert self.datatype is not None
        value = await self.get_value()
        core = self._worker.core
        metadata = make_metadata(self.datatype)

        if core.hasPropertyLimits(self._dev, self._prop):
            from event_model import Limits, LimitsRange

            lo = core.getPropertyLowerLimit(self._dev, self._prop)
            hi = core.getPropertyUpperLimit(self._dev, self._prop)
            metadata["limits"] = Limits(
                control=LimitsRange(low=lo, high=hi),
                display=LimitsRange(low=lo, high=hi),
            )

        if allowed := core.getAllowedPropertyValues(self._dev, self._prop):
            metadata["choices"] = list(allowed)

        return make_datakey(self.datatype, value, source, metadata)

    def set_callback(self, callback: Callback[Reading[PrimitiveT]] | None) -> None:
        """Wire or unwire the per-property psygnal change signal.

        The psygnal slot fires on the worker thread; the Reading is delivered
        back to the asyncio loop via ``call_soon_threadsafe``.
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
        assert datatype is not None

        def _on_property_changed(_dev: str, _prop: str, new_value: str) -> None:
            # Runs on the worker thread — marshal into the asyncio loop
            reading: Reading[PrimitiveT] = {
                "value": cast("PrimitiveT", datatype(new_value)),
                "timestamp": time.time(),
                "alarm_severity": 0,
            }
            loop.call_soon_threadsafe(cast("Any", callback), reading)

        prop_sig = core.events.devicePropertyChanged(self._dev, self._prop)
        prop_sig.connect(_on_property_changed)
        self._psygnal_slot = _on_property_changed
        self._reading_callback = callback

        # Immediately deliver the current value so the subscriber is
        # synchronised on registration (matches SoftSignalBackend behaviour)
        self.submit(
            lambda: loop.call_soon_threadsafe(
                cast("Any", callback),
                {
                    "value": cast(
                        "PrimitiveT", datatype(core.getProperty(self._dev, self._prop))
                    ),
                    "timestamp": time.time(),
                    "alarm_severity": 0,
                },
            )
        )

    def submit(self, fn: Callable[[], Any]) -> Future[Any]:
        """Direct access to the worker queue (for subclasses or tests)."""
        return self._worker.submit(fn)
