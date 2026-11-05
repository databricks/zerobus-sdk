"""
Asynchronous Python SDK for the Zerobus service (Rust-backed).

This module provides high-performance asynchronous ingestion backed by a Rust core
with native async/await support.
"""

from zerobus.sdk.aio.zerobus_sdk import (
    HeadersProvider,
    NonRetriableException,
    OAuthToken,
    PersistentStream,
    PersistentStreamBuilder,
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
    "TableProperties",
    "StreamConfigurationOptions",
    "RecordType",
    "HeadersProvider",
    "OAuthToken",
    "get_token",
    "ZerobusException",
    "NonRetriableException",
]
