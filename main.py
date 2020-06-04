import collections
import logging
import requests

from bs4 import BeautifulSoup

from db import DB, DbCsv

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('ixbt')

ParseResult = collections.namedtuple(
    'ParseResult',
    (
        'id',
        'title',
        'description',
        'pub_date',
        'author',
        'url',
    )
)


class Crawler:
    def __init__(self, db: DB) -> None:
        self.session = requests.Session()
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 OPR/68.0.3618.125',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,en-GB;q=0.6'}
        self.url = 'https://www.ixbt.com/news/'
        self.base_url = 'https://www.ixbt.com'
        self._db = db

    @property
    def db(self) -> DB:
        return self._db

    @db.setter
    def db(self, db: DB) -> None:
        self._db = db

    def get_html(self, url):
        try:
            r = self.session.get(url=url)
            r.raise_for_status()
        except requests.exceptions.ConnectionError as e:
            logger.error(f'Connection error: {e}')
        except requests.exceptions.HTTPError as e:
            logger.error(f'Wrong http requests: {e}')
        except requests.exceptions.Timeout:
            logger.error('Timeout')
        except requests.exceptions.TooManyRedirects:
            logger.error('Too many redirects')
        else:
            return r.text
        return

    def parse_html(self, html: str):

        soup = BeautifulSoup(html, 'lxml')

        for tag in soup.find_all('li', {'class': 'item__border'}):
            tag.decompose()
        for tag in soup.find_all('a', {'class': 'comments_link'}):
            tag.decompose()

        container = soup.select('li.item')
        for item in container:
            self.parse_item(item)

    def parse_item(self, item: BeautifulSoup):
        url = self.get_url(item)
        title = self.get_url(item)
        news = self.get_news(url)
        description = news.get('description')
        author = news.get('author')
        pub_date = news.get('pub_date')

        self.db.add_news(ParseResult(
            title=title,
            description=description,
            url=url,
            pub_date=pub_date,
            author=author,
            id=hash(description),
        ))

        logger.debug(f'{title}\n{description}\n{url}\n{pub_date}\n{author}')
        logger.debug('=' * 80)

    def get_url(self, item: BeautifulSoup):
        url_item = item.select_one('a')
        if not url_item:
            logger.error('No url_item')
            return
        url = f"{self.base_url}{url_item.get('href')}"
        if not url:
            logger.error('No href')
            return
        return url

    @staticmethod
    def get_title(item: BeautifulSoup):
        title = item.select_one('strong').text
        if not title:
            logger.error('No title')
            return
        return title

    def get_news(self, url_news):
        if html := self.get_html(url_news):
            return self.parse_html_news(html=html)
        else:
            logger.error(f'Запрашиваемая страница не получена: {url_news}')

    def parse_html_news(self, html: str):
        soup = BeautifulSoup(html, 'lxml')
        result = {
            'description': self.get_description(soup),
            'author': self.get_author(soup),
            'pub_date': self.get_pub_date(soup),
        }
        return result

    @staticmethod
    def get_description(news: BeautifulSoup):
        container_description = news.select_one('div.b-article__content').select('p')
        description = '\n'.join([item.text for item in container_description])
        if not description:
            logger.error('No description')
        return description

    @staticmethod
    def get_author(news: BeautifulSoup):
        author = news.select_one('p.author').select('span')[1].text
        if not author:
            logger.error('No author')
        return author

    @staticmethod
    def get_pub_date(news: BeautifulSoup):
        pub_date = news.find(itemprop='datePublished').get('content')
        if not pub_date:
            logger.error('No pub_date')
        return pub_date

    def run(self):
        if html := self.get_html(self.url):
            self.parse_html(html=html)
            logger.info(f'Получено новостей: {self.db.count}')
        else:
            logger.error(f'Запрашиваемая страница не получена: {self.url}')


if __name__ == '__main__':
    db = DbCsv()
    crawler = Crawler(db)
    crawler.run()
