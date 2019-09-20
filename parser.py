import os
import json
import copy

from tqdm import tqdm
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()


class Parser:

    def __init__(self, parse_name: str, save_to: str):
        self._base_filter = ['_scroll_id', 'hits.total.value']
        self._hits_filter = []

        self.parse_name = parse_name
        self.save_to = save_to

        self._user = os.getenv('HTTP_USER')
        self._password = os.getenv('HTTP_PASSWORD')
        self._max_batch_size = int(os.getenv('MAX_BATCH_SIZE'))
        self._elastic_url = os.getenv('ELASTIC_URL')
        self._scroll_timeout = os.getenv('SCROLL_TIMEOUT')
        self._request_timeout = int(os.getenv('REQUEST_TIMEOUT'))

        self.parsed = list()
        self._es = Elasticsearch(hosts=[self._elastic_url], http_auth=(self._user, self._password))

    def parse(self, query: dict, size: int = 1, scroll: bool = False):
        if size > self._max_batch_size:
            raise Exception('Size value is greater than allowed.')

        filtered_field = self._base_filter + self._hits_filter

        elastic_response = self._es.search(
            body=query,
            size=size,
            request_timeout=self._request_timeout,
            scroll=self._scroll_timeout,
            filter_path=filtered_field)

        if scroll:
            return self._scroll(elastic_response, filtered_field)
        else:
            return elastic_response['hits']['hits']

    def _scroll(self, elastic_response, filtered_fields):
        scroll_id = elastic_response['_scroll_id']
        scroll_size = elastic_response['hits']['total']['value']
        size = copy.copy(scroll_size)

        with tqdm(total=size) as bar:
            while scroll_size:
                self._manage_memory()
                self.parsed.extend(elastic_response['hits']['hits'])
                scroll_size = len(elastic_response['hits']['hits'])
                bar.update(scroll_size)
                elastic_response = self._es.scroll(
                    filter_path=filtered_fields,
                    scroll_id=scroll_id,
                    scroll='2m',
                    request_timeout=120)
                scroll_id = elastic_response['_scroll_id']
            self._save_part()
        return self.parsed

    def _manage_memory(self):
        if len(self.parsed) >= 10000:
            self._save_part()
            self.parsed = []

    @staticmethod
    def _parts_counter():
        i = 1
        while True:
            yield i
            i += 1

    def _save_part(self):
        if not hasattr(self, 'parts_counter'):
            self.parts_counter = self._parts_counter()

        part = next(self.parts_counter)

        with open(f'{self.save_to}{self.parse_name}_{part}', 'w') as f:
            json.dump(self.parsed, f)

    def add_filter(self, fields: list, reset: bool = False):
        if reset:
            self._hits_filter = fields
        else:
            self._hits_filter.extend(fields)


class QueryBuilder:

    def __init__(self):
        self._query = dict({
            'query': {
                'bool': {
                    'must': []
                }
            }
        })

    def match_phrase(self, field: str, value: str):
        if not (field or value):
            Exception('Field or value were not defined.')

        condition = {field: {'query': value}}
        self._query['query']['bool']['must'].append({'match_phrase': condition})

    def timerange(self, day: str = None, range: dict = None, date_format: str = 'yyyy-MM-dd'):
        if day:
            gte, lte = day, day
        elif range:
            gte = range.get('gte')
            lte = range.get('lte')
        else:
            raise Exception('The date should be defined.')

        condition = {'@timestamp': {'gte': gte, 'lte': lte, 'format': date_format}}
        self._query['query']['bool']['must'].append({'range': condition})

    def aggregation(self, name: str, func: str, field: str, without_data: bool = True):
        condition = {func: {'field': field}}
        self._query['aggs'] = {name: condition}
        self._query['size'] = 0 if without_data else None

    def build(self):
        return self._query