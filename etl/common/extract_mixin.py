import abc
import datetime

from backoff import backoff
from config import Settings
from custom_context_manager import pg_conn_context
from storage import State, JsonFileStorage

settings = Settings()


class ExtractAbstract(abc.ABC):
    @abc.abstractmethod
    def start_process(self) -> list[list]:
        pass

    @abc.abstractmethod
    def extract(self):
        pass

    @abc.abstractmethod
    def postgres_producer(self) -> bool:
        pass

    @abc.abstractmethod
    def postgres_merger(self) -> None:
        pass


class ExtractFilmAbstract(abc.ABC):
    @abc.abstractmethod
    def postgres_enricher(self) -> bool:
        pass


class ExtractMixin(ExtractAbstract):
    SIZE = settings.parse_size
    state = ''
    object_with_updated_at = []
    objects_ids = []
    data = []
    pg_cursor = None
    offset = 0

    def __init__(self, table: str = None, first_load: bool = False) -> None:
        """
        Инициализация процесса ETL
        :param table: Название таблицы, по которой происходит проверка данных
        :param first_load: Параметр для первоначальной загрузки данных
        """
        if table:
            self.table = table
        else:
            self.table = ''
        self.first_load = first_load

    def start_process(self) -> list[list]:
        """
        Запуск процессов по получению данных
        :return: возвращает список данных полученных из PostgreSQL
        """
        self.extract()

        return [self.data, self.object_with_updated_at]

    @backoff()
    def extract(self):
        """
        Проверка последнего состояния записей и получение обновлённых данных
        :return: Если данные для преобразования были получены, то возвращается "True",
        иначе передаётся "False" и дальнейшие вычисления не производятся
        """
        with pg_conn_context(**settings.db.model_dump()) as self.pg_conn:
            self.pg_cursor = self.pg_conn.cursor()

            while self.postgres_producer():
                for object_id, updated_at in self.object_with_updated_at[:]:
                    if self.get_storage_state().get_state(object_id) == str(updated_at):
                        self.object_with_updated_at.remove([object_id, updated_at])
                if not self.object_with_updated_at:
                    self.offset += self.SIZE
                else:
                    break
            else:
                return False
            self.postgres_merger()
        return True

    def postgres_producer(self) -> bool:
        """
        Получение списка id изменённых объектов для обновления

        :return: Если данные есть, то возвращается "True" для дальнейшей обработки этих данных,
        иначе передаётся "False" и дальнейшие вычисления не производятся
        """
        if self.first_load:
            date_query = ''
        else:
            date_limit = str(datetime.datetime.now() - datetime.timedelta(days=settings.updated_days_limit))
            date_query = 'WHERE updated_at > \'%s\' ' % date_limit

        query = (
                'SELECT id, updated_at '
                'FROM content.%s '
                '%s '
                'ORDER BY updated_at '
                'LIMIT %s '
                'OFFSET %s;' % (self.table, date_query, self.SIZE, self.offset)
        )
        self.pg_cursor.execute(query)
        self.object_with_updated_at = self.pg_cursor.fetchmany(self.SIZE)
        self.objects_ids = [object_id[0] for object_id in self.object_with_updated_at]
        return bool(self.object_with_updated_at)

    def postgres_merger(self) -> None:
        """
        Получение всей информации по персоналиям
        """
        pass

    def get_storage_state(self):
        return State(JsonFileStorage(self.state))
