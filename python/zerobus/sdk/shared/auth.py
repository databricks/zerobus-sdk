"""
Alternative authentication methods for Zerobus streams.

This module defines the opt-in values for the ``auth`` argument of
``create_stream``, used instead of a Databricks OAuth client_id/client_secret:
:class:`FederatedToken` authenticates with an external identity provider (for
example Entra ID), and :class:`PersonalAccessToken` with a Databricks personal
access token.
"""

import uuid
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional, Union

# A zero-arg callback returning the current external IdP token (e.g. an Entra ID
# JWT), either synchronously (``str``) or asynchronously (an awaitable of
# ``str``).
IdpTokenSupplier = Callable[[], Union[str, Awaitable[str]]]


@dataclass(frozen=True)
class FederatedToken:
    """Authenticate a Zerobus stream by federating an external IdP token, passed as
    the ``auth`` argument to ``create_stream``.

    Immutable: a distinct callback is a distinct ``FederatedToken``. Because
    ``_cache_identity`` is fixed at construction and partitions the account-level
    token cache, mutating ``idp_token_supplier`` on an already-used instance would
    keep serving the first callback's cached token (the new callback only runs on
    a miss). Freezing forbids that, so build a new ``FederatedToken`` to switch
    identities.

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


def _require_non_empty_token(token: str) -> str:
    """Return ``token`` if it is a non-empty string; never echo it in the error."""
    if not isinstance(token, str) or not token:
        raise ValueError("personal access token must be a non-empty string")
    return token


@dataclass(frozen=True)
class PersonalAccessToken:
    """Authenticate a Zerobus stream with a Databricks personal access token (PAT),
    passed as the ``auth`` argument to ``create_stream``.

    Beta (private preview): PAT authentication for Zerobus requires server-side
    enablement, and this API can change.

    The PAT must be created with the ``zerobus`` API scope in the workspace that the
    Zerobus endpoint belongs to. The SDK sends it as a bearer token and cannot refresh
    it: once it expires or is revoked, creating or recovering a stream fails with an
    authentication error.

    The token is left out of ``repr()``, so printing or logging the object does not
    reveal it.

    Args:
        token: The personal access token. Must be a non-empty string.

    Raises:
        ValueError: If ``token`` is empty or not a string.
    """

    token: str = field(repr=False)

    def __post_init__(self) -> None:
        _require_non_empty_token(self.token)


__all__ = [
    "FederatedToken",
    "IdpTokenSupplier",
    "PersonalAccessToken",
]
