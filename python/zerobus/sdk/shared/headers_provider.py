"""
Headers provider for custom authentication.

This module re-exports HeadersProvider from the Rust core for backward compatibility.

Note: The Rust SDK handles OAuth authentication internally by default.
Only use HeadersProvider if you need custom authentication.
"""

# Re-export from Rust core
from zerobus._zerobus_core import HeadersProvider

__all__ = [
    "HeadersProvider",
]
