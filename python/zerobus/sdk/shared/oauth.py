"""
Zerobus OAuth tokens for clients outside the SDK's own streams.

This module defines :class:`OAuthToken`, the result of ``get_token``.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Tuple


@dataclass(frozen=True)
class OAuthToken:
    """A Zerobus-scoped Databricks OAuth access token, returned by ``get_token``.

    Attributes:
        access_token: The bearer token. Excluded from ``repr`` so it isn't logged.
        expires_at: When the token expires, as a timezone-aware UTC datetime
            measured from when the request was sent; ``None`` if the server
            reported no lifetime.
    """

    access_token: str = field(repr=False)
    expires_at: Optional[datetime]


def _from_native(raw: Tuple[str, Optional[int]]) -> OAuthToken:
    access_token, expires_at_ms = raw
    expires_at = None if expires_at_ms is None else datetime.fromtimestamp(expires_at_ms / 1000, tz=timezone.utc)
    return OAuthToken(access_token, expires_at)
