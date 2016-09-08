# Stdlib imports
import urllib3
import os
import json
from datetime import datetime, timedelta

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
                doc = contact_struct_to_es(contact, media_list)
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


def search_contact_in_elastic(contact_id):
    return es.search(index="contacts", body={
        "query": {"match": {"data.Id": contact_id}}})


def sync_lists_contacts_hourly():
    an_hour_ago = datetime.today() - timedelta(hours=1)

    # Find all contacts updated in the last hour
    query = client.query(kind='Contact')
    query.add_filter('Updated', '>', an_hour_ago)
    contacts = list(query.fetch())

    contact_id_to_contact = {}

    for contact in contacts:
        contact_id = contact.key.id
        contact_id_to_contact[contact_id] = contact

    # Go through each list
    query = client.query(kind='MediaList')
    for media_list in query.fetch():
        if 'Contacts' in media_list:
            to_append = []
            limit = 0
            for contact_id in media_list['Contacts']:
                # If it contains then we delete the record (to recreate)
                if contact_id in contact_id_to_contact:
                    # If contact exists
                    elastic_contact = search_contact_in_elastic(contact_id)
                    doc = contact_struct_to_es(contact_id_to_contact[
                                               contact_id], media_list)
                    if elastic_contact and 'hits' in elastic_contact and 'hits' in elastic_contact['hits'] and len(elastic_contact['hits']['hits']) > 0:
                        elastic_contact_id = elastic_contact[
                            'hits']['hits'][0]['_id']
                        res = es.delete(
                            index='contacts', doc_type='contact', id=elastic_contact_id)
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


@app.task
def contact_id_to_es_sync(contact_id):
    key = client.key('Contact', int(contact_id))
    contact = client.get(key)
    doc = contact_struct_to_es(contact, None)
    res = helpers.bulk(es, [doc])
    return True


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
# sync_list_contacts()
# sync_lists_contacts_hourly()
