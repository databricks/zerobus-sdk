"""
Federated external-IdP authentication for Zerobus streams.

This module defines :class:`FederatedToken`, the opt-in configuration for
authenticating a stream with an external identity provider (for example
Entra ID) instead of a Databricks OAuth client_id/client_secret.
"""

import uuid
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional, Union

# A zero-arg callback returning the current external IdP token (e.g. an Entra ID
# JWT), either synchronously (``str``) or asynchronously (an awaitable of
# ``str``).
IdpTokenSupplier = Callable[[], Union[str, Awaitable[str]]]


@dataclass
class FederatedToken:
    """Authenticate a Zerobus stream by federating an external IdP token, passed as
    the ``auth`` argument to ``create_stream``.

    Args:
        idp_token_supplier: A zero-arg callable returning the current external
            IdP token as a string, synchronous (``str``) or asynchronous (an
            awaitable of ``str``; async requires the async SDK). Called only when
            a fresh token must be minted, not on every request. Keep a *synchronous*
            callback fast: it runs inline and holds the interpreter (GIL) while it
            blocks, so a slow blocking one stalls the SDK for the whole IdP
            round-trip and is not interrupted by ``recovery_timeout_ms``. This is
            true on the async SDK too — a sync callback is invoked before the
            await point, so it blocks the event loop / a worker just as it blocks
            the sync SDK. For real network I/O, use the async SDK with an
            ``async def`` callback (which the SDK awaits without holding the GIL),
            or return an already-cached token from a fast sync callback.
        databricks_client_id: The Databricks service principal client_id for
            workload identity federation, or ``None`` for account-level
            federation.
    """

    idp_token_supplier: IdpTokenSupplier
    databricks_client_id: Optional[str] = None

    # A stable per-instance identity that partitions the account-level token cache:
    # reusing one FederatedToken shares its cached exchanged token, a distinct
    # instance isolates. Kept as a stable string rather than the native handle's
    # pointer, since the handle is rebuilt per stream and a dropped pointer can be
    # reused by the allocator. Generated once per instance; unused for workload
    # identity federation, which keys by ``databricks_client_id``.
    _cache_identity: str = field(default_factory=lambda: uuid.uuid4().hex, init=False, repr=False, compare=False)


__all__ = [
    "FederatedToken",
    "IdpTokenSupplier",
]
