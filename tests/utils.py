from functools import wraps


def decorator():
    def _wrapper(f):
        @wraps(f)
        def _inner(*args, **kwargs):
            return f(*args, **kwargs)

        return _inner

    return _wrapper
