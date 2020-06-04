import csv
import json
import logging

from abc import ABC, abstractmethod
from elasticsearch import Elasticsearch


ELASTICSEARCH_SOCKET = 'http://0.0.0.0:9200'  # Сокет elasticsearch
DB_INDEX = 'ixbt'  # Название индекса
DB_TABLE = 'news'  # Название таблицы

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('lenta')

HEADERS = (
    'id',
    'title',
    'description',
    'pub_date',
    'author',
    'url',
)


class DB(ABC):
    @abstractmethod
    def add_news(self, data):
        pass

    @abstractmethod
    def get_news(self, id_):
        pass

    @abstractmethod
    def get_all_news(self):
        pass

    @abstractmethod
    def get_words(self, id_):
        pass

    @abstractmethod
    def search(self, field, value):
        pass

    @property
    @abstractmethod
    def count(self):
        pass

    @abstractmethod
    def aggregate(self, field):
        pass


class DbCsv(DB):
    def __init__(self):
        self.file_name = 'db.csv'
        self.create_db()

    def create_db(self):
        with open(self.file_name, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(HEADERS)

    def add_news(self, data):
        with open(self.file_name, 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(data)

    def get_news(self, id_):
        pass

    def get_all_news(self):
        pass

    def get_words(self, id_):
        pass

    def search(self, field, value):
        pass

    @property
    def count(self):
        with open(self.file_name, 'r', encoding='utf-8') as f:
            file_object = csv.reader(f)
            return sum(1 for _ in file_object) - 1

    def aggregate(self, field):
        pass


class DbElastic(DB):
    def __init__(self):
        self.es = Elasticsearch()
        self.delete_index()

    def add_news(self, data):
        """
        Добавление записи в elasticsearch
        """
        response = self.es.index(index=DB_INDEX,
                                 id=data.id,
                                 body=json.dumps({
                                     'title': data.title,
                                     'description': data.description,
                                     'pub_date': data.pub_date,
                                     'url': data.url,
                                     'author': data.author,
                                 }))
        logger.debug(response['result'])
        return response['result']

    def get_news(self, id_):
        """
        Считывание записи из базы данных elasticsearch
        """
        response = self.es.get(index=DB_INDEX, id=id_)
        logger.debug(response['_source'])
        return response['_source']

    def get_all_news(self):
        """
        Считывание всех записей из базы данных elasticsearch
        """
        response = self.es.search(index=DB_INDEX, body={'query': {'match_all': {}}})
        logger.debug(response)
        return response['hits']

    def get_words(self, id_):
        text = self.get_news(id_)['description']
        logger.debug(text)
        # Анализируем текст на набор токенов
        response = self.es.indices.analyze(index=DB_INDEX,
                                           body={
                                               'analyzer': 'russian',
                                               'text': text,
                                           })
        logger.debug(response)
        logger.debug(list(map(lambda record: record['token'], response['tokens'])))
        return list(map(lambda record: record['token'], response['tokens']))

    def search(self, field, value):
        response = self.es.search(index=DB_INDEX,
                                  body={
                                      'query': {
                                          'match': {
                                              field: value,
                                          }
                                      }
                                  })
        logger.debug(response['hits']['total']['value'])
        return response['hits']

    @property
    def count(self):
        return self.es.count(index=DB_INDEX)["count"]

    def aggregate(self, field):
        """
        Агрегация
        """
        body = {
            'size': 0,
            'aggs': {
                'patterns': {
                    'terms': {'field': f'{field}.keyword'}
                }
            }
        }
        response = self.es.search(index=DB_INDEX, body=body)
        logger.debug(response)
        return response

    def delete_index(self):
        if self.es.indices.exists(index=DB_INDEX):
            self.es.indices.delete(index=DB_INDEX)
