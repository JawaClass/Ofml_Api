from typing import Any, Callable
import functools


def catch_file_exception(f: Callable[..., Any]):

    @functools.wraps(f)
    def wrapper(*args: Any, **kwargs: Any):
        try:
            result = f(*args, **kwargs)
        except (OSError, IOError) as e:
            return NotAvailable(e)
        return result

    return wrapper


class NotAvailable:

    def __init__(self, error: Exception):
        self.error = error

    def __repr__(self):
        return f"NotAvailable ({self.error})"
