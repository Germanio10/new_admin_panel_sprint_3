from elasticsearch import Elasticsearch, helpers

from backoff import backoff
from common.load_mixin import LoadMixin
from config import Settings
from custom_context_manager import pg_conn_context
from extract.persons import ExtractPersons
from extract.genres import ExtractGenres
from storage import State, JsonFileStorage
from transform.films import FilmsTransform
from transform.persons import PersonsTransform
from transform.genres import GenresTransform

settings = Settings()


class LoadES:
    def __init__(self, first_load: bool = False):
        self.first_load = first_load

    def start_process(self):
        if self.first_load:
            for iteration in range((self.count('person')//settings.parse_size) + 1):
                self.person_etl()
            for iteration in range((self.count('genre')//settings.parse_size) + 1):
                self.genre_etl()
        else:
            self.person_etl()
            self.genre_etl()
            self.film_etl()

    def person_etl(self):
        es_persons, persons_with_updated_at = PersonsTransform(self.first_load).transform()
        person_state = ExtractPersons.state
        self.elastic_load(es_persons)
        self.save_state(persons_with_updated_at, person_state)

    def genre_etl(self):
        es_genres, genres_with_updated_at = GenresTransform(self.first_load).transform()
        genre_state = ExtractGenres.state
        self.elastic_load(es_genres)
        self.save_state(genres_with_updated_at, genre_state)

    @staticmethod
    def count(table: str) -> int:
        """
        Получение общего количества записей в таблице
        :param table: имя таблицы
        :return:
        """
        with pg_conn_context(**settings.db.model_dump()) as pg_conn:
            pg_cursor = pg_conn.cursor()

            query = ('SELECT COUNT(*) FROM content.%s;' % table)

            pg_cursor.execute(query)
            full_count = pg_cursor.fetchone()[0]
        return full_count

    @backoff()
    def elastic_load(self, objects: list) -> None:
        """
        Отправка данных в Elasticsearch
        """
        es = Elasticsearch(settings.elasticsearch.url())
        start_list = 0
        end_list = settings.elasticsearch.load_size
        while objects[start_list:end_list]:
            helpers.bulk(es, objects)
            start_list += settings.elasticsearch.load_size
            end_list += settings.elasticsearch.load_size

    @staticmethod
    def save_state(object_with_updated_at: list, state: State) -> None:
        """
        Сохранение состояния обработанных данных
        """
        for object_id, updated_at in object_with_updated_at:
            state.set_state(object_id, str(updated_at))


class PersonETL(LoadMixin):
    state = 'person.json'
    transform_class = PersonsTransform
    first_load_table = 'person'


class GenreETL(LoadMixin):
    state = 'genre.json'
    transform_class = GenresTransform
    first_load_table = 'genre'


class FilmETL(LoadMixin):
    state = 'film.json'
    transform_class = FilmsTransform
    tables = ['film_work', 'genre', 'person']
    first_load_table = 'genre'

    def etl(self):
        for table in self.tables:
            es_genres, genres_with_updated_at = self.transform_class(table, self.first_load).transform()
            genre_state = State(JsonFileStorage(self.state))
            self.elastic_load(es_genres)
            self.save_state(genres_with_updated_at, genre_state)
