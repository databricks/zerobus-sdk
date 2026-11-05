"""
Sync Python SDK for the Zerobus service (Rust-backed).

This module provides high-performance synchronous ingestion backed by a Rust core.
"""

from zerobus.sdk.sync.zerobus_sdk import (
    HeadersProvider,
    NonRetriableException,
    OAuthToken,
    PersistentStream,
    PersistentStreamBuilder,
    RecordAcknowledgment,
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
    ZerobusArrowStream,
    ZerobusException,
    ZerobusSdk,
    ZerobusStream,
    get_token,
)

__all__ = [
    "ZerobusSdk",
    "PersistentStream",
    "PersistentStreamBuilder",
    "ZerobusStream",
    "ZerobusArrowStream",
    "RecordAcknowledgment",
    "TableProperties",
    "StreamConfigurationOptions",
    "RecordType",
    "HeadersProvider",
    "OAuthToken",
    "get_token",
    "ZerobusException",
    "NonRetriableException",
]
