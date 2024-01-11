from elasticsearch import Elasticsearch

es = Elasticsearch(hosts='http://127.0.0.1:9200/')

dsl = {
    'dbname': 'movies_database',
    'user': 'app',
    'password': '123qwe',
    'host': '127.0.0.1',
    'port': 5432
}
