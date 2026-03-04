import functools
import os
import platform

import pytest

__tests__ = os.path.dirname(__file__)


def raises(msg):
    """
    Decorator to assert that a function raises a ValueError with a specific message.

    Example:
        @raises("Expected error message")
        def test_something():
            raise ValueError("Expected error message")
    """

    def inner(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with pytest.raises(ValueError) as info:
                func(*args, **kwargs)
            exception_str = str(info.value)
            if platform.system() == "Windows":
                exception_str = exception_str.replace(__tests__ + "\\", "")
                exception_str = exception_str.replace("\\", "/")
            else:
                exception_str = exception_str.replace(__tests__ + "/", "")
            assert msg in exception_str

        return wrapper

    return inner
