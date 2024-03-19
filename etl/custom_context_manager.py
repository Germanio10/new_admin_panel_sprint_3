from contextlib import contextmanager

import psycopg2
from psycopg2.extras import DictCursor


@contextmanager
def pg_conn_context(cursor_factory=DictCursor, **kwargs):
    conn = psycopg2.connect(**kwargs, cursor_factory=cursor_factory)
    try:
        yield conn
    finally:
        conn.close()
