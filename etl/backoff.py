from functools import wraps
import time
import psycopg2.errors
from logs.logging_config import logger


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * (factor ^ n), если t < border_sleep_time
        t = border_sleep_time, иначе
    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз нужно увеличивать время ожидания на каждой итерации
    :param border_sleep_time: максимальное время ожидания
    :return: результат выполнения функции
    """

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
                    logger.error(str(e))
        return inner
    return func_wrapper
