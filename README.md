# Elasticsearch API sync

Syncing data between elasticsearch and the internal NewsAI datastore.

Deploy contacts: `cd contacts` then `gcloud alpha functions deploy syncContacts --bucket datastore_elastic_api_sync --trigger-topic datastore-sync-contacts-functions --region us-central1`

Deploy publications: `cd publications` then `gcloud alpha functions deploy syncPublications --bucket datastore_elastic_api_sync --trigger-topic datastore-sync-publications-functions --region us-central1`

Get logs: `gcloud alpha functions get-logs syncContacts`
