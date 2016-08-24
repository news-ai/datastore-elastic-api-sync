# Elasticsearch API sync

Syncing data between elasticsearch and the internal NewsAI datastore.

### Problems & solution

1. Syncing data from API and Elasticsearch without downtime:
    - Have an alias pointing to index `publications_v1` and `agencies_v1`.
    - Create index `publications_v2` and `agencies_v2`, populate data, point alias to new index.
    - Delete old index `publications_v1` and `agencies_v1`.

### Installing & Running

`pip install -r requirements.txt`, `python sync.py`

### Running in the background

This command makes it run every 6 hours

`nohup python scripts/reindex_elasticsearch.py &`
