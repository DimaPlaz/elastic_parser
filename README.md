# Elasticsearch parser

This tool is a convinient way to build query and get data from elasticsearch.

## Getting Started

```
python3.6 or newer
python-pip
```

### Prerequisites
Install dependencies.

```
pip install -r requirements.txt
```

### Using

Configure these params in .env.
```
HTTP_USER=user
HTTP_PASSWORD=password
ELASTIC_URL=http://ip:port
```

Init the Parser class with parse_name (will be used for saves) and path for saving results.
```python
from elastic_parser import *
parser = Parser(
    parse_name='parse_2019_09_09-15', 
    save_to='/home/sohi/docs/tools/elastic_parser/data/')
])
```

Set query conditions using QueryBuilder.
```python
query = QueryBuilder()
query.match_phrase('client_id', 0)
query.match_phrase('state', 'task_completed')
query.timerange(range={'gte':'2019-09-09', 'lte': '2019-09-15'})
```
Determine which field would you like to get.
```python
parser.add_filter([
    'hits.hits._source.image_url',
    'hits.hits._source.result.category',
    'hits.hits._source.result.sku'
])
```
Start parsing.
```python
parsed = parser.parse(query.build(), size=10000, scroll=True)
```
