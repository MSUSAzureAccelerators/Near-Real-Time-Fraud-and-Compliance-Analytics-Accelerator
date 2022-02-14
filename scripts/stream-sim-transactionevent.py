#!/usr/bin/env python

# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

"""
Example to show streaming sending events with different options to an Event Hub.
"""

# pylint: disable=C0111

import time
import os
import json

from azure.eventhub import EventHubProducerClient, EventData



CONNECTION_STR = ''
EVENTHUB_NAME = 'incoming-stream'


start_time = time.time()

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENTHUB_NAME
)
to_send_message_cnt = 500
bytes_per_message = 16384

with open('dataset_final.json','r') as data_file:
    json_data = data_file.read()

data = json.loads(json_data)
with producer:
    for i in data:
        event_data_batch = producer.create_batch()
        event_data =EventData(json.dumps(i))
        try:
            event_data_batch.add(event_data)
            producer.send_batch(event_data_batch)
            print("semnding data", event_data)
            time.sleep(10)
        except ValueError:
            producer.send_batch(event_data_batch)
            event_data_batch = producer.create_batch()
            event_data_batch.add(event_data)
        finally:
            print("in finally")
    

print("Send messages in {} seconds.".format(time.time() - start_time))
