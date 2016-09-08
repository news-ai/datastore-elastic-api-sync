# Stdlib imports
import urllib3
import os

# Third-party app imports
import requests
import certifi
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from elasticsearch import Elasticsearch, helpers
from gcloud import datastore

# Imports from app
from taskrunner import app

# Elasticsearch
ELASTICSEARCH_USER = os.environ['NEWSAI_ELASTICSEARCH_USER']
ELASTICSEARCH_PASSWORD = os.environ['NEWSAI_ELASTICSEARCH_PASSWORD']

# Setup datastore connection for Google Cloud
client = datastore.Client('newsai-1166')

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


def contact_struct_to_es(contact, media_list):
    if 'CustomFields.Name' in contact:
        del contact['CustomFields.Name']
    if 'CustomFields.Value' in contact:
        del contact['CustomFields.Value']

    contact_id = contact.key.id
    contact['Id'] = int(contact_id)

    if media_list:
        media_list_id = media_list.key.id
        contact['ListId'] = int(media_list_id)

    doc = {
        '_type': 'contact',
        '_index': 'contacts',
        'data': contact
    }
    return doc


def search_contact_in_elastic(contact_id):
    return es.search(index="contacts", body={
        "query": {"match": {"data.Id": contact_id}}})


@app.task
def contact_id_to_es_sync(contact_id):
    key = client.key('Contact', int(contact_id))
    contact = client.get(key)

    elastic_contact = search_contact_in_elastic(contact_id)
    if elastic_contact and 'hits' in elastic_contact and 'hits' in elastic_contact['hits'] and len(elastic_contact['hits']['hits']) > 0:
        elastic_contact_id = elastic_contact[
            'hits']['hits'][0]['_id']
        res = es.delete(
            index='contacts', doc_type='contact', id=elastic_contact_id)

    doc = contact_struct_to_es(contact, None)
    res = helpers.bulk(es, [doc])
    return True
