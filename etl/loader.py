from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError
from settings import es
from logging_config import logger


class Loader:
    """Загрузка данных в Elasticsearch"""

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
            logger.error(e)
