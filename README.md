# Elasticsearch API sync

Syncing data between elasticsearch and the internal NewsAI datastore.

### Contacts

Deploy contacts: `cd contacts` then `gcloud alpha functions deploy syncContacts --bucket datastore_elastic_api_sync --trigger-topic datastore-sync-contacts-functions --region us-central1`

Get logs for contacts: `gcloud alpha functions get-logs syncContacts`

### Publications

Deploy publications: `cd publications` then `gcloud alpha functions deploy syncPublications --bucket datastore_elastic_api_sync --trigger-topic datastore-sync-publications-functions --region us-central1`

Get logs for publications: `gcloud alpha functions get-logs syncPublications`
