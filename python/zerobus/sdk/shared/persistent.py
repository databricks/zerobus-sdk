"""
Builder for persistent (Eos) streams, shared by the sync and async SDKs.

A persistent stream records its identity and committed offset on the server. After a
restart, ``resume(stream_id)`` reconnects to the same stream and delivery into Delta stays
exactly-once. The sync and async builders in ``zerobus.sdk.sync`` and ``zerobus.sdk.aio``
add the ``build()`` and ``resume()`` methods.

Persistent streams support the JSON and protobuf record formats, with OAuth or a custom
headers provider for authentication. The Avro format and external-IdP federation are not
supported.
"""

import zerobus._zerobus_core as _core
from zerobus.sdk.shared.builder import _StreamBuilderBase


class _PersistentStreamBuilderBase(_StreamBuilderBase):
    """Collects the settings of a persistent stream. Setters return the builder."""

    _OPEN_METHODS = "build() or resume()"

    def _native_args(self):
        """Validate the collected settings and return the arguments of the native open call."""
        self._check_required()

        table_properties = _core.TableProperties(self._table_name, self._descriptor)
        settings = _core.PersistentStreamSettings(
            client_id=self._client_id,
            client_secret=self._client_secret,
            headers_provider=self._headers_provider,
            **self._settings,
        )
        return table_properties, settings

    @staticmethod
    def _check_stream_id(stream_id: str) -> str:
        if not isinstance(stream_id, str) or not stream_id:
            raise ValueError("stream_id must be a non-empty string")
        return stream_id
