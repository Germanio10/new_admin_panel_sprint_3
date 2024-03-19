from common.extract_mixin import ExtractMixin
from config import Settings

settings = Settings()


class ExtractPersons(ExtractMixin):
    pg_cursor = None
    offset = 0
    first_load = False
    state = 'person.json'

    def postgres_merger(self) -> None:
        """
        Получение всей информации по персоналиям
        """
        query = (
                'SELECT p.id, p.full_name, fw.id, pfw.role '
                'FROM content.person p '
                'LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id '
                'LEFT JOIN content.film_work fw ON fw.id = pfw.film_work_id '
                'WHERE p.id IN (%s); '
                '' % ('\'' + '\', \''.join(self.objects_ids) + '\'')
        )
        self.pg_cursor.execute(query)
        self.data = self.pg_cursor.fetchall()
        return None
