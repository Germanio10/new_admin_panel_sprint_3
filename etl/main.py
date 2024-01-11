from postgres_extractor import PostgresExtractor
from transformer import Transformer
from loader import Loader
from state import JsonFileStorage, State
import time
# from create_index import create_movies_index()
# from settings import es

CHUNK_SIZE = 100

if __name__ == '__main__':
    # create_movies_index(es)
    state_file_path = 'state.json'
    json_file_storage = JsonFileStorage(state_file_path)
    stage_storage_instance = State(storage=json_file_storage)
    while True:
        extractor = PostgresExtractor(CHUNK_SIZE, stage_storage_instance)
        transformer = Transformer(extractor)
        loader = Loader()
        loader.load(transformer.transform())
        time.sleep(1)