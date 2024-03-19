import abc

from elasticsearch import Elasticsearch, helpers

from backoff import backoff
from common.transform_abstract import TransformAbstract
from config import Settings
from custom_context_manager import pg_conn_context
from storage import State, JsonFileStorage

settings = Settings()


class LoadAbstract(abc.ABC):

    @abc.abstractmethod
    def start_process(self):
        pass

    @abc.abstractmethod
    def etl(self):
        pass

    @abc.abstractmethod
    def transform_class(self, first_load: bool) -> TransformAbstract:
        pass

    @abc.abstractmethod
    def state(self) -> str:
        return str()

    @abc.abstractmethod
    def first_load_table(self) -> str:
        return str()


class LoadMixin(LoadAbstract):
    state = ''
    first_load_table = ''

    class Meta:
        abstract = True

    def __init__(self, first_load: bool = False):
        self.first_load = first_load

    def start_process(self):
        if self.first_load:
            for iteration in range((self.count(self.first_load_table)//settings.parse_size) + 1):
                self.etl()
        else:
            self.etl()

    def transform_class(self, first_load) -> TransformAbstract:
        pass

    def etl(self):
        es_genres, genres_with_updated_at = self.transform_class(self.first_load).transform()
        genre_state = State(JsonFileStorage(self.state))
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
