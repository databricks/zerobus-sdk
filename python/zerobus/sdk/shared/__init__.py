"""
Shared utilities for Zerobus SDK.

This module provides Python wrappers around Rust types with comprehensive
documentation and type hints.
"""

# Re-export Rust types that don't need wrappers
from zerobus._zerobus_core import (
    HeadersProvider,
    MultiplexedStreamOptions,
    NonRetriableException,
    RecordType,
    TableProperties,
    ZerobusException,
)

# Import Python wrappers with documentation
from zerobus.sdk.shared.auth import FederatedToken, IdpTokenSupplier, OAuth, PersonalAccessToken
from zerobus.sdk.shared.config import AckCallback, StreamConfigurationOptions

__all__ = [
    "AckCallback",
    "FederatedToken",
    "HeadersProvider",
    "IdpTokenSupplier",
    "MultiplexedStreamOptions",
    "NonRetriableException",
    "OAuth",
    "PersonalAccessToken",
    "RecordType",
    "StreamConfigurationOptions",
    "TableProperties",
    "ZerobusException",
]
