# Elasticsearch API sync

Syncing data between elasticsearch and the internal NewsAI datastore.

Deploy: `gcloud alpha functions deploy sync --bucket datastore_elastic_api_sync --trigger-topic datastore-sync-contacts-functions --region us-central1`

Get logs: `gcloud alpha functions get-logs sync`
