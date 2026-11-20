"""In-memory stand-ins for HarnessClient / MockAdminClient (same method names, no sockets)."""

import json

from zerobus_test_runner.clients import SdkFault
from zerobus_test_runner.errors import HarnessError


class FakeHarness:
    """Records calls and returns ``(payload, fault)`` like the real client.

    fail_on names a method that raises HarnessError (a harness/infra fault). sdk_fault_on
    names a method whose response carries an SdkFault (the SDK under test reported an error).
    """

    def __init__(
        self, *, fail_on: str = "", sdk_fault_on: str = "", retryable: bool = False, message: str = "sdk error (fake)"
    ):
        self.calls: list = []
        self._fail_on = fail_on
        self._sdk_fault_on = sdk_fault_on
        self._fault = SdkFault(message, retryable)
        self._next_offset = 0

    def _check(self, name):
        if self._fail_on == name:
            raise HarnessError(f"harness.{name} failed: UNAVAILABLE (fake)")

    def _fault_for(self, name):
        return self._fault if self._sdk_fault_on == name else None

    def _offset(self):
        off = self._next_offset
        self._next_offset += 1
        return off

    async def create_stream(self, **kw):
        self._check("create_stream")
        self.calls.append(("create_stream", kw))
        return "stream-A", self._fault_for("create_stream")

    async def ingest_record(self, stream_id, record_json):
        self._check("ingest_record")
        self.calls.append(("ingest_record", stream_id, json.loads(record_json)))
        return self._offset(), self._fault_for("ingest_record")

    async def ingest_records(self, stream_id, records_json):
        self._check("ingest_records")
        self.calls.append(("ingest_records", stream_id, [json.loads(rj) for rj in records_json]))
        # SDK batch ingest assigns the whole batch one offset, so advance once per call.
        return self._offset(), self._fault_for("ingest_records")

    async def await_write_completed(self, stream_id, offset):
        self._check("await_write_completed")
        self.calls.append(("await_write_completed", stream_id, offset))
        return self._fault_for("await_write_completed")

    async def flush(self, stream_id):
        self._check("flush")
        self.calls.append(("flush", stream_id))
        return self._fault_for("flush")

    async def close_stream(self, stream_id, timeout=None):
        self._check("close_stream")
        self.calls.append(("close_stream", stream_id))
        return self._fault_for("close_stream")

    async def shutdown(self):
        self.calls.append(("shutdown",))


class FakeMock:
    """Any MockAdmin query returns ``values[name]`` (callables get the kwargs)."""

    def __init__(self, **values):
        self.values = values
        self.calls: list = []

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)

        async def call(**kw):
            self.calls.append((name, kw))
            if name in ("reset", "inject_stream_error"):
                return None
            if name not in self.values:
                raise KeyError(f"FakeMock has no canned value for {name}")
            v = self.values[name]
            return v(**kw) if callable(v) else v

        return call
