import datetime

from backoff import backoff
from common.extract_mixin import ExtractMixin, ExtractFilmAbstract
from config import Settings
from custom_context_manager import pg_conn_context

settings = Settings()


class ExtractFilms(ExtractMixin, ExtractFilmAbstract):
    pg_cursor = None
    offset = 0
    first_load = False
    fw = []
    state = 'film.json'

    @backoff()
    def extract(self) -> bool:
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
            self.postgres_enricher()
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

    def postgres_enricher(self) -> None:
        """
        Получение списка id фильмов для обновления
        """
        if self.table == 'film_work':
            self.fw = [fw_id[0] for fw_id in self.object_with_updated_at]
            return None

        query = (
            'SELECT fw.id, fw.updated_at '
            'FROM content.film_work fw '
            'LEFT JOIN content.%s_film_work tfw ON tfw.film_work_id = fw.id '
            'WHERE tfw.%s_id IN (%s) '
            'ORDER BY fw.updated_at; ' % (self.table,
                                          self.table,
                                          '\'' + '\', \''.join(self.objects_ids) + '\'')
        )
        self.pg_cursor.execute(query)
        self.fw = [fw_id[0] for fw_id in self.pg_cursor.fetchall()]
        return None

    def postgres_merger(self) -> None:
        """
        Получение всей информации по фильмам
        """
        if self.fw:
            query = (
                'SELECT fw.id as fw_id, fw.title, fw.description, fw.rating, fw.type, '
                'fw.created_at, fw.updated_at, pfw.role, p.id, p.full_name, g.id ,g.name '
                'FROM content.film_work fw '
                'LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id '
                'LEFT JOIN content.person p ON p.id = pfw.person_id '
                'LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id '
                'LEFT JOIN content.genre g ON g.id = gfw.genre_id '
                'WHERE fw.id IN (%s); '
                '' % ('\'' + '\', \''.join(self.fw) + '\'')
            )
            self.pg_cursor.execute(query)
            self.data = self.pg_cursor.fetchall()
        else:
            self.data = []
        return None
