import psycopg2
from psycopg2.extras import DictCursor
from backoff import backoff
from state import State, JsonFileStorage
import json


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
                            LIMIT 100;
                            """
            persons_data = self._fetch_data(cursor, person_query, ())
            if persons_data:
                last_processed_date = max(entry['updated_at'].isoformat() for entry in persons_data)
                self.state_storage.set_state(self.state_key, last_processed_date)
            yield persons_data

    @backoff()
    def extract_films_with_persons(self, person_ids):
        with self.connection.cursor() as cursor:
            film_person_query = """SELECT fw.id, fw.updated_at
                                    FROM content.film_work fw
                                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                                    WHERE pfw.person_id IN %s
                                    ORDER BY fw.updated_at
                                    LIMIT 100;
                                """
            cursor.execute(film_person_query, (tuple(person_ids), ))
            person_film_data = cursor.fetchmany(self.chunk_size)
            yield person_film_data
            

    @backoff()
    def extract_films(self, film_ids):
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
                        WHERE fw.id IN %s;
                        """
            cursor.execute(film_query, (tuple(film_ids), ))
            films_data = cursor.fetchmany(self.chunk_size)
            yield films_data


state_file_path = 'state.json'
json_file_storage = JsonFileStorage(state_file_path)
stage_storage_instance = State(storage=json_file_storage)

ps = PostgresExtractor(1, stage_storage_instance)

# ps_data = ps.extract_persons()
# for data in ps.extract_persons():
#     print(data)

# person_ids = [person['id'] for person in data]

# for data in ps.extract_films_with_persons(person_ids):
#     print(data)

# film_ids = [film['id'] for film in data]

# for data in ps.extract_films(film_ids):
#     print(data)


class Transformer:
    def __init__(self, data: PostgresExtractor):
        self.data = data

    def _get_persons(self):
        person_ids = []
        for item in self.data.extract_persons():
            for person_id in item:
                person_ids.append(person_id['id'])
        return person_ids

    def _get_films_with_persons(self):
        film_ids = []
        for item in self.data.extract_films_with_persons(self._get_persons()):
            for film_id in item:
                film_ids.append(film_id['id'])
        return film_ids

    def transform(self):
        butch = []
        for film in self.data.extract_films(self._get_films_with_persons()):
            for row in film:
                filmwork = {
                    'id': row['fw_id'],
                    'imdb_rating': row['rating'],
                    'genre': row['name'],
                    'title': row['title'],
                    'description': row['description'],
                    'director': row['director'] if row['role'] == 'director' else '',
                    'actors_names': row['full_name'] if row['role'] == 'actor' else '',
                    'writers_names': row['full_name'] if row['role'] == 'writer' else '',
                    'actors': [{'id': row['id'], 'full_name': row['full_name']}] if row['role'] == 'actor' else [],
                    'writers': [{'id': row['id'], 'full_name': row['full_name']}] if row['role'] == 'writer' else [],
                }
                butch.append(filmwork)
        print(butch)

ts = Transformer(ps)
ts.transform()