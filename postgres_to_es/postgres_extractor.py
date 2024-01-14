import psycopg2
from psycopg2.extras import DictCursor
from backoff import backoff
from state.state import State
from settings.settings import dsl
from typing import List, Dict
from logs.logging_config import logger


START_UNIX_TIME = '1970-01-01T00:00:00Z'


class PostgresExtractor:
    "Получение данных из БД с записью крайней даты в состояние"

    def __init__(self, chunk_size: int, state_storage: State) -> None:
        self.connection = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        self.chunk_size = chunk_size
        self.state_storage = state_storage
        self.state_key = "last_processed_date"

    def _fetch_data(self, cursor, query, params):
        cursor.execute(query, params)
        data = cursor.fetchmany(self.chunk_size)
        if data:
            last_processed_date = data[-1]['updated_at'].isoformat()
            self.state_storage.set_state(self.state_key, last_processed_date)
        return data

    @backoff(exceptions=[psycopg2.errors.ConnectionException,
                         psycopg2.errors.ConnectionFailure,])
    def extract_films_info(self) -> List[Dict]:
        """Извлечение данных из бд"""

        last_processed_date = self.state_storage.get_state(self.state_key) or START_UNIX_TIME
        try:
            with self.connection.cursor() as cursor:
                person_query = f"""SELECT
                                    fw.id,
                                    fw.title,
                                    fw.description,
                                    fw.rating,
                                    fw.type,
                                    fw.created_at,
                                    fw.updated_at,
                                    COALESCE (
                                        json_agg(
                                            DISTINCT jsonb_build_object(
                                                'person_role', pfw.role,
                                                'person_id', p.id,
                                                'person_name', p.full_name
                                            )
                                        ) FILTER (WHERE p.id is not null),
                                        '[]'
                                    ) as persons,
                                    array_agg(DISTINCT g.name) as genres
                                    FROM content.film_work fw
                                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                                    LEFT JOIN content.person p ON p.id = pfw.person_id
                                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                                    LEFT JOIN content.genre g ON g.id = gfw.genre_id
                                    WHERE fw.updated_at >= '{last_processed_date}'
                                    GROUP BY fw.id
                                    ORDER BY fw.updated_at
                                    LIMIT 100;"""
                persons_data = self._fetch_data(cursor, person_query, ())
                if persons_data:
                    last_processed_date = max(entry['updated_at'].isoformat() for entry in persons_data)
                    self.state_storage.set_state(self.state_key, last_processed_date)
                yield persons_data
        except Exception as e:
            logger.error(e)
