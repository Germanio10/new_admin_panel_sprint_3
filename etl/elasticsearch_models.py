import datetime
import uuid
from dataclasses import dataclass, field


@dataclass
class ElasticsearchDataRow:
    id: uuid.uuid4
    title: str
    description: str
    imdb_rating: float
    type: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    person_role: str
    person_id: uuid.uuid4
    person_name: str
    genre_id: str
    genre: str


@dataclass
class ElasticsearchDataNormalized:
    id: uuid.uuid4
    title: str
    imdb_rating: float
    genre: list
    genres_list: list
    description: str
    directors_names: list = field(default_factory=lambda: [])
    actors_names: list = field(default_factory=lambda: [])
    writers_names: list = field(default_factory=lambda: [])
    directors: list = field(default_factory=lambda: [])
    actors: list = field(default_factory=lambda: [])
    writers: list = field(default_factory=lambda: [])


@dataclass
class PersonElasticsearchDataRow:
    id: uuid.uuid4
    full_name: str
    fw_id: uuid.uuid4
    role: str


@dataclass
class PersonElasticsearchDataTransform:
    id: uuid.uuid4
    full_name: str
    films: dict = field(default_factory=lambda: [])


@dataclass
class GenreElasticsearchDataRow:
    id: uuid.uuid4
    name: str


@dataclass
class GenreElasticsearchDataTransform:
    id: uuid.uuid4
    name: str
