from functools import wraps
import time
import psycopg2.errors
import logging


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            tries = 0
            while True:
                current_time = min(border_sleep_time, start_sleep_time * factor ** tries)
                if tries > 0:
                    time.sleep(current_time)
                try:
                    return func(*args, **kwargs)
                except (
                    psycopg2.errors.ConnectionException,
                    psycopg2.errors.ConnectionFailure,
                ) as e:
                    tries += 1
                    logging.error(str(e))
        return inner
    return func_wrapper
