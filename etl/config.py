from pathlib import Path

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from pydantic import Field
from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(f"{BASE_DIR}/.env")


class PostgreSQLSettings(BaseSettings):
    dbname: str = Field(validation_alias='DB_NAME')
    user: str = Field(validation_alias='DB_USER')
    password: str = Field(validation_alias='DB_PASSWORD')
    host: str = Field(validation_alias='DB_HOST')
    port: int = Field(validation_alias='DB_PORT')


class ElasticsearchSettings(BaseSettings):
    host: str = Field(validation_alias='ELASTIC_HOST')
    port: str = Field(validation_alias='ELASTIC_PORT')
    load_size: int = Field(validation_alias='ELASTIC_LOAD_SIZE')

    def url(self):
        return f'http://{self.host}:{self.port}'

    def es(self):
        return Elasticsearch(hosts=self.url())


class Settings(BaseSettings):
    # elasticsearch_url: str
    # elasticsearch_load_size: int
    parse_size: int
    updated_days_limit: int
    rerun: int

    db: PostgreSQLSettings = PostgreSQLSettings()
    elasticsearch: ElasticsearchSettings = ElasticsearchSettings()
