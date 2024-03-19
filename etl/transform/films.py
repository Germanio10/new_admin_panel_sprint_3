from dataclasses import asdict

from common.transform_abstract import TransformAbstract
from elasticsearch_models import ElasticsearchDataRow, ElasticsearchDataNormalized
from extract.films import ExtractFilms


class FilmsTransform(TransformAbstract):
    actions_list = []

    def __init__(self, table: str, first_load: bool = False):
        self.films_data, self.object_with_updated_at = ExtractFilms(table, first_load).start_process()

    def transform(self) -> list[list]:
        """
        Преобразование данных по фильмам в формат для отправки в Elasticsearch
        """
        elastic_data = {}
        for fw_data in self.films_data:
            data = ElasticsearchDataRow(*fw_data)
            directors = []
            actors = []
            writers = []

            if data.id in elastic_data:

                actors = elastic_data[data.id].actors
                directors = elastic_data[data.id].directors
                writers = elastic_data[data.id].writers
                directors_names = elastic_data[data.id].directors_names
                actors_names = elastic_data[data.id].actors_names
                writers_names = elastic_data[data.id].writers_names
                genres = elastic_data[data.id].genre
                genres_list = elastic_data[data.id].genres_list

                if (data.person_role == 'director' and
                        (elastic_data[data.id].directors_names == [] or
                         data.person_name not in elastic_data[data.id].directors_names)):
                    director = {
                        'id': data.person_id,
                        'name': data.person_name
                    }
                    directors.append(director)

                    if elastic_data[data.id].directors_names:
                        directors_names.append(director['name'])
                    else:
                        directors_names = [data.person_name]

                elif (data.person_role == 'actor' and
                      (elastic_data[data.id].actors_names == [] or
                       data.person_name not in elastic_data[data.id].actors_names)):
                    actor = {
                        'id': data.person_id,
                        'name': data.person_name
                    }
                    actors.append(actor)

                    if elastic_data[data.id].actors_names:
                        actors_names.append(actor['name'])
                    else:
                        actors_names = [data.person_name]

                elif (data.person_role == 'writer' and
                      (elastic_data[data.id].writers_names == [] or
                       data.person_name not in elastic_data[data.id].writers_names)):
                    writer = {
                        'id': data.person_id,
                        'name': data.person_name
                    }
                    writers.append(writer)

                    if elastic_data[data.id].writers_names:
                        writers_names.append(writer['name'])
                    else:
                        writers_names = [data.person_name]

                if data.genre not in elastic_data[data.id].genre:
                    genres.append(data.genre)
                    genres_list.append({
                        'id': data.genre_id,
                        'name': data.genre
                    })

                elastic_data[data.id] = ElasticsearchDataNormalized(
                    id=data.id,
                    title=data.title,
                    imdb_rating=data.imdb_rating,
                    genre=genres,
                    genres_list=genres_list,
                    description=data.description,
                    directors_names=directors_names,
                    actors_names=actors_names,
                    writers_names=writers_names,
                    directors=directors,
                    actors=actors,
                    writers=writers,
                )
            else:
                if data.person_role == 'actor':
                    actors = [{
                        'id': data.person_id,
                        'name': data.person_name
                    }]
                elif data.person_role == 'writer':
                    writers = [{
                        'id': data.person_id,
                        'name': data.person_name
                    }]
                elif data.person_role == 'director':
                    directors = [{
                        'id': data.person_id,
                        'name': data.person_name
                    }]

                genres_list = [{
                    'id': data.genre_id,
                    'name': data.genre
                }]

                elastic_data[data.id] = ElasticsearchDataNormalized(
                    id=data.id,
                    title=data.title,
                    imdb_rating=data.imdb_rating,
                    genre=[data.genre],
                    genres_list=genres_list,
                    description=data.description,
                    directors_names=[data.person_name] if data.person_role == 'director' else [],
                    actors_names=[data.person_name] if data.person_role == 'actor' else [],
                    writers_names=[data.person_name] if data.person_role == 'writer' else [],
                    directors=directors,
                    actors=actors,
                    writers=writers,
                )

        for item_id, body in elastic_data.items():
            self.actions_list.append({
                '_id': item_id,
                '_index': 'movies',
                '_source': asdict(body)
            })

        return [self.actions_list, self.object_with_updated_at]
