import psycopg2
from psycopg2.extras import DictCursor
from backoff import backoff
from state import State, JsonFileStorage
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError

es = Elasticsearch(hosts='http://127.0.0.1:9200/')

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
    def extract_films_info(self):
        last_processed_date = self.state_storage.get_state(self.state_key) or '1970-01-01T00:00:00Z'

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
                                WHERE fw.updated_at > '{last_processed_date}'
                                GROUP BY fw.id
                                ORDER BY fw.updated_at
                                LIMIT 100;"""
            persons_data = self._fetch_data(cursor, person_query, ())
            if persons_data:
                last_processed_date = max(entry['updated_at'].isoformat() for entry in persons_data)
                self.state_storage.set_state(self.state_key, last_processed_date)
            yield persons_data

state_file_path = 'state.json'
json_file_storage = JsonFileStorage(state_file_path)
stage_storage_instance = State(storage=json_file_storage)

ps = PostgresExtractor(1, stage_storage_instance)
# for item in ps.extract_films_info():
#     for i in item:
#         print(dict(i))



class Transformer:
    def __init__(self, data: PostgresExtractor):
        self.data = data

    def _get_film_info(self):
        films_info_lst = []
        for objects in self.data.extract_films_info():
            for film_info in objects:
                films_info_lst.append(film_info)
        return films_info_lst

    def transform(self):
        butch = []
        for row in self._get_film_info():
            filmwork = {
                    'id': row['id'],
                    'imdb_rating': row['rating'],
                    'genre': row['genres'],
                    'title': row['title'],
                    'description': row['description'],
                    'director': next((person['person_name'] for person in row.get('persons', []) if person['person_role'] == 'director'), ''),
                    'actors_names': [person['person_name'] for person in row.get('persons', []) if person['person_role'] == 'actor'],
                    'writers_names': [person['person_name'] for person in row.get('persons', []) if person['person_role'] == 'writer'],
                    'actors': [{'id': person['person_id'], 'name': person['person_name']} for person in row.get('persons', []) if person['person_role'] == 'actor'],
                    'writers': [{'id': person['person_id'], 'name': person['person_name']} for person in row.get('persons', []) if person['person_role'] == 'writer'],
                }
            butch.append(filmwork)
        return butch
        

ts = Transformer(ps)
data = ts.transform()


class Loader:
    
    def load(self, data):
        actions = [
            {
                '_index': 'movies',
                '_id': row['id'],
                '_source': row,
            }
            for row in data
        ]
        try:
            helpers.bulk(es, actions)
        except BulkIndexError as e:
            for error in e.errors:
                print(error)
            raise e

ld = Loader()
ld.load(data)