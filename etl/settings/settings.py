import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

load_dotenv()


es = Elasticsearch(hosts='http://127.0.0.1:9200/')

dsl = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}
