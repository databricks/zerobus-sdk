"""
Python SDK for the Ingest API.

This is the synchronous version of the SDK. For the asynchronous version,
please use `from zerobus.sdk.aio import ...`.
"""

# Import from Rust core
from zerobus._zerobus_core import (
    NonRetriableException,
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
    ZerobusException,
)

from . import sync

# Re-export sync classes
ZerobusSdk = sync.ZerobusSdk
ZerobusStream = sync.ZerobusStream
RecordAcknowledgment = sync.RecordAcknowledgment

__all__ = [
    "ZerobusSdk",
    "ZerobusStream",
    "RecordAcknowledgment",
    "TableProperties",
    "StreamConfigurationOptions",
    "RecordType",
    "ZerobusException",
    "NonRetriableException",
]
