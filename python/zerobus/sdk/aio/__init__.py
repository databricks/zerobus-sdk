"""
Asynchronous Python SDK for the Zerobus service (Rust-backed).

This module provides high-performance asynchronous ingestion backed by a Rust core
with native async/await support.
"""

from zerobus.sdk.aio.zerobus_sdk import (
    HeadersProvider,
    MultiplexedStream,
    MultiplexedStreamBuilder,
    NonRetriableException,
    PersistentStream,
    PersistentStreamBuilder,
    RecordType,
    StreamBuilder,
    StreamConfigurationOptions,
    TableProperties,
    ZerobusArrowStream,
    ZerobusException,
    ZerobusSdk,
    ZerobusStream,
)

__all__ = [
    "ZerobusSdk",
    "PersistentStream",
    "PersistentStreamBuilder",
    "StreamBuilder",
    "MultiplexedStream",
    "MultiplexedStreamBuilder",
    "ZerobusStream",
    "ZerobusArrowStream",
    "TableProperties",
    "StreamConfigurationOptions",
    "RecordType",
    "HeadersProvider",
    "ZerobusException",
    "NonRetriableException",
]
