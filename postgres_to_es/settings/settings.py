import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

"""Не получается настроить pydantic_settings так, чтобы не падала бд и эластик"""

load_dotenv()


es = Elasticsearch(hosts='http://elasticsearch:9200/')

dsl = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': 'db',
    'port': os.getenv('DB_PORT'),
}
