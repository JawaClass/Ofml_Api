
def catch_file_exception(f):
    def wrapper(*args, **kwargs):
        try:
            result = f(*args, **kwargs)
        except (OSError, IOError) as e:
            return NotAvailable(e)
        return result

    return wrapper


class NotAvailable:

    def __init__(self, error):
        self.error = error

    def __repr__(self):
        return f"NotAvailable({self.error})"