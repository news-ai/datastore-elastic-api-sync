# Stdlib imports
import os
import json
import sys

# Third-party app imports
from gcloud import pubsub

from sync import contact_id_to_es_sync


PROJECT_ID = 'newsai-1166'

if __name__ == '__main__':
    pubsub_client = pubsub.Client(PROJECT_ID)
    topic = pubsub_client.topic("datastore-sync-contacts")
    sub = pubsub.Subscription("datastore-sync-contacts_sub", topic=topic)
    while True:
        messages = sub.pull(return_immediately=False, max_messages=2)
        if messages:
            for ack_id, message in messages:
                json_data = json.loads(message.data)
                contact_id_to_es_sync.delay(json_data["Id"])

                # Acknowledge that we've gotten the message right away
                sub.acknowledge([ack_id])
