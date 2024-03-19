from common.extract_mixin import ExtractMixin
from config import Settings

settings = Settings()


class ExtractGenres(ExtractMixin):
    pg_cursor = None
    offset = 0
    first_load = False
    state = 'genre.json'

    def postgres_merger(self) -> None:
        """
        Получение всей информации по жанрам
        """
        query = (
            'SELECT id, name '
            'FROM content.genre;'
        )
        self.pg_cursor.execute(query)
        self.data = self.pg_cursor.fetchall()
        return None
