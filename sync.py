# Stdlib imports
import urllib3
import os
import json
from datetime import datetime

# Third-party app imports
import requests
import certifi
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from elasticsearch import Elasticsearch, helpers
from gcloud import datastore

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


def sync_es(index_name, kind, result_type):
    to_append = []
    query = client.query(kind=kind)
    limit = 0
    total = 0
    for result in query.fetch():
        print result
        result_id = result.key.id
        result['Id'] = int(result_id)

        doc = {
            '_type': result_type,
            '_index': index_name,
            'data': result
        }
        to_append.append(doc)
        print limit
        if limit == 100:
            res = helpers.bulk(es, to_append)
            print res
            to_append = []
            limit = 0
        limit = limit + 1
        total = total + 1
        print total
    # If there are any that weren't processed at the end
    if len(to_append) > 0:
        res = helpers.bulk(es, to_append)
        print res


def sync_list_contacts():
    query = client.query(kind='MediaList')
    total = 0
    for media_list in query.fetch():
        if 'Contacts' in media_list:
            to_append = []
            limit = 0
            for contact_id in media_list['Contacts']:
                key = client.key('Contact', int(contact_id))
                contact = client.get(key)

                print contact

                if 'CustomFields.Name' in contact:
                    del contact['CustomFields.Name']
                if 'CustomFields.Value' in contact:
                    del contact['CustomFields.Value']

                contact_id = contact.key.id
                contact['Id'] = int(contact_id)

                media_list_id = media_list.key.id
                contact['ListId'] = int(media_list_id)

                doc = {
                    '_type': 'contact',
                    '_index': 'contacts',
                    'data': contact
                }

                to_append.append(doc)

                if limit == 100:
                    res = helpers.bulk(es, to_append)
                    print res
                    to_append = []
                    limit = 0

                limit = limit + 1

            # If any left at the end
            if len(to_append) > 0:
                res = helpers.bulk(es, to_append)
                print res


def reset_elastic(kind):
    es.indices.delete(index=kind, ignore=[400, 404])
    es.indices.create(index=kind, ignore=[400, 404])

# Publications
# reset_elastic('publications')
# sync_es('publications', 'Publication', 'publication')

# Agencies
# reset_elastic('agencies')
# sync_es('agencies', 'Agency', 'agency')

# Agencies
# reset_elastic('contacts')
sync_list_contacts()
