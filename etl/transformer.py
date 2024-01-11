from postgres_extractor import PostgresExtractor
from typing import Dict, List


class Transformer:
    """Преобразование данных в формат для Elasticsearch"""
    def __init__(self, data: PostgresExtractor) -> None:
        self.data = data

    """Обработка данных для трансформации"""
    def _get_film_info(self):
        films_info_lst = []
        for objects in self.data.extract_films_info():
            for film_info in objects:
                films_info_lst.append(film_info)
        return films_info_lst

    """Трансформация данных для загрузки"""
    def transform(self) -> List[Dict]:
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
