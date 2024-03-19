from time import sleep

from config import Settings
from create_index import CreateIndexes
from load.general_load import FilmETL, PersonETL, GenreETL


settings = Settings()


if __name__ == '__main__':
    CreateIndexes(settings.elasticsearch.es())

    FilmETL(first_load=True).start_process()
    GenreETL(first_load=True).start_process()
    PersonETL(first_load=True).start_process()

    while True:
        sleep(10)
        FilmETL().start_process()
        GenreETL().start_process()
        PersonETL().start_process()
