from postgres_extractor import PostgresExtractor
from transformer import Transformer
from loader import Loader
from state.state import JsonFileStorage, State
import time
from create_index import create_movies_index
from settings.settings import es
from logs.logging_config import logger


CHUNK_SIZE = 100

if __name__ == '__main__':
    try:
        create_movies_index(es)
        state_file_path = 'state.json'
        json_file_storage = JsonFileStorage(state_file_path)
        stage_storage_instance = State(storage=json_file_storage)
        while True:
            extractor = PostgresExtractor(CHUNK_SIZE, stage_storage_instance)
            transformer = Transformer(extractor)
            loader = Loader()
            loader.load(transformer.transform())
            time.sleep(60)
    except Exception as e:
        logger.critical(e)
