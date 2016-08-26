# Stdlib imports
import urllib3
import json
from datetime import datetime

# Third-party app imports
import requests
import certifi
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from elasticsearch import Elasticsearch, helpers

# Removing requests warning
urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# Elasticsearch setup
es = Elasticsearch(
    ['https://search.newsai.org'],
    http_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD),
    port=443,
    use_ssl=True,
    verify_certs=True,
    ca_certs=certifi.where(),
)


def sync_entities_es(new_index_name, entities):
    if entities:
        to_append = []
        for entity in entities:
            doc = {
                '_type': 'entity',
                '_index': new_index_name,
                'data': entity
            }
            to_append.append(doc)
        res = helpers.bulk(es, to_append)
        print res


def create_alias():
    es.indices.put_alias(index='entities', name='entity')


def change_alias(new_index_name):
    es.indices.delete_alias(index='_all', name='entity')
    es.indices.put_alias(index=new_index_name, name='entity')


def delete_previous_index(previous_index):
    es.indices.delete(index=previous_index, ignore=[400, 404])


def get_newest_index():
    indices = es.cat.indices(format='json')
    for i in indices:
        if 'entities' in i:
            return i
    return indices[2]


def generate_next_index(index):
    if 'v' in index:
        index = index.split('_v')
        index_number = int(index[1]) + 1
        index = index[0] + '_v' + str(index_number)
    else:
        index = index + '_v1'
    return index


def deploy_new_update():
    previous_index = get_newest_index()['index']
    new_index = generate_next_index(previous_index)
    es.indices.create(index=new_index, ignore=[400, 404])
    get_entities(new_index)
    change_alias(new_index)
    delete_previous_index(previous_index)


def reset_elastic():
    es.indices.delete(index='entities', ignore=[400, 404])
    es.indices.create(index='entities', ignore=[400, 404])
    get_entities()

deploy_new_update()
