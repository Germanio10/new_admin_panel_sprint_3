import psycopg2
from psycopg2.extras import DictCursor
from backoff import backoff
from state import State, JsonFileStorage

dsl = {
    'dbname': 'movies_database',
    'user': 'app',
    'password': '123qwe',
    'host': '127.0.0.1',
    'port': 5432
}


class PostgresExtractor:
    def __init__(self, chunk_size, state_storage: State):
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

    @backoff()
    def extract_persons(self):
        last_processed_date = self.state_storage.get_state(self.state_key) or '1970-01-01T00:00:00Z'

        with self.connection.cursor() as cursor:
            person_query = f"""SELECT id, updated_at
                            FROM content.person
                            WHERE updated_at >= '{last_processed_date}'
                            ORDER BY updated_at
                            LIMIT 5;
                            """
            data = list(self._fetch_data(cursor, person_query, ()))
            if data:
                last_processed_date = max(entry['updated_at'].isoformat() for entry in data)
                self.state_storage.set_state(self.state_key, last_processed_date)
            yield data

    @backoff()
    def extract_films_with_persons(self, person_ids):
        last_processed_date = self.state_storage.get_state(self.state_key) or '1970-01-01T00:00:00Z'

        with self.connection.cursor() as cursor:
            film_person_query = """SELECT fw.id, fw.updated_at
                                    FROM content.film_work fw
                                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                                    WHERE pfw.person_id IN ANY(%s)
                                    ORDER BY fw.updated_at
                                    LIMIT 5;
                                """
            films_data = list(self._fetch_data(cursor, film_person_query, (person_ids),))
            if films_data:
                last_processed_date = max(entry['updated_at'].isoformat() for entry in films_data)
                self.state_storage.set_state(self.state_key, last_processed_date)
            yield films_data

    @backoff()
    def extract_films(self, film_ids):
        last_processed_date = self.state_storage.get_state(self.state_key) or '1970-01-01T00:00:00Z'

        with self.connection.cursor() as cursor:
            film_query = """SELECT
                            fw.id as fw_id,
                            fw.title,
                            fw.description,
                            fw.rating,
                            fw.type,
                            fw.created_at,
                            fw.updated_at,
                            pfw.role,
                            p.id,
                            p.full_name,
                            g.name
                        FROM content.film_work fw
                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                        LEFT JOIN content.person p ON p.id = pfw.person_id
                        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                        LEFT JOIN content.genre g ON g.id = gfw.genre_id
                        WHERE fw.id IN %s
                        """
            films_data = list(self._fetch_data(cursor, film_query, (tuple(film_ids),)))
            if films_data:
                last_processed_date = max(entry['updated_at'].isoformat() for entry in films_data)
                self.state_storage.set_state(self.state_key, last_processed_date)
            yield films_data

# Оставляем остальной код без изменений
state_file_path = 'state.json'
json_file_storage = JsonFileStorage(state_file_path)
stage_storage_instance = State(storage=json_file_storage)

ps = PostgresExtractor(100, stage_storage_instance)
