import csv

from abc import ABC, abstractmethod

from main import ParseResult


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
    def add_news(self, data: ParseResult):
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


class DbCsv(DB):
    def __init__(self):
        self.file_name = 'db.csv'
        self.create_db()

    def create_db(self):
        with open(self.file_name, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(HEADERS)

    def add_news(self, data: ParseResult):
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
