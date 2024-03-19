from common.transform_abstract import TransformAbstract
from elasticsearch_models import GenreElasticsearchDataRow, \
    GenreElasticsearchDataTransform
from extract.genres import ExtractGenres


class GenresTransform(TransformAbstract):
    actions_list = []

    def __init__(self, first_load: bool = False):
        self.genres_data, self.object_with_updated_at = ExtractGenres('genre', first_load).start_process()

    def transform(self) -> list[list]:
        es_transform_data = {}
        for genre in self.genres_data:
            genre = GenreElasticsearchDataRow(*genre)

            if genre.id not in es_transform_data:
                es_transform_data[genre.id] = GenreElasticsearchDataTransform(
                    id=genre.id,
                    name=genre.name
                )
        for genre_id, genre_data in es_transform_data.items():
            self.actions_list.append({
                '_id': genre_id,
                '_index': 'genres',
                '_source': {
                    'id': genre_data.id,
                    'name': genre_data.name
                }
            })
        return [self.actions_list, self.object_with_updated_at]
