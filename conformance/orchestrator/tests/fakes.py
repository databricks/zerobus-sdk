"""In-memory stand-ins for HarnessClient / MockAdminClient (same method names, no sockets)."""

import json


class FakeHarness:
    def __init__(self, await_error: str = "", fail_on: str = ""):
        self.calls: list = []
        self._await_error = await_error
        self._fail_on = fail_on  # name of a method that raises InfraError
        self._next_offset = 0

    def _maybe_fail(self, name):
        from zerobus_test_runner.errors import InfraError

        if self._fail_on == name:
            raise InfraError(f"harness.{name} failed: UNAVAILABLE (fake)")

    async def create_stream(self, **kw):
        self._maybe_fail("create_stream")
        self.calls.append(("create_stream", kw))
        return "stream-A"

    async def ingest_record(self, stream_id, record_json):
        self._maybe_fail("ingest_record")
        self.calls.append(("ingest_record", stream_id, json.loads(record_json)))
        off = self._next_offset
        self._next_offset += 1
        return off

    async def await_write_completed(self, stream_id, offset, timeout_ms):
        self.calls.append(("await_write_completed", stream_id, offset, timeout_ms))
        return self._await_error

    async def flush(self, stream_id):
        self.calls.append(("flush", stream_id))

    async def close_stream(self, stream_id):
        self.calls.append(("close_stream", stream_id))

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
            if name == "reset":
                return None
            if name not in self.values:
                raise KeyError(f"FakeMock has no canned value for {name}")
            v = self.values[name]
            return v(**kw) if callable(v) else v

        return call
