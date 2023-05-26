"""

Various decorators for the project.

"""

import functools
import logging


# LOGGING


def log(msg=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Retrieve the logger dynamically
            logger = logging.getLogger(func.__module__)
            # logger = logging.getLogger(func.__module__.split('.')[0])
            # Log the custom message if provided
            if msg is not None:
                logger.info(msg)

            # Call the original function
            result = func(*args, **kwargs)

            return result

        return wrapper

    return decorator
