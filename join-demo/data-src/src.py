"""
Name:       src.py
Purpose:    Test script to push data into kafka for consumption by the example spark streaming app.
            Not intended for any kind of serious purpose.
            usage: src.py kafka_broker num_to_send
             e.g.: src.py 192.168.12.24 250
Author:     PNDA team

Created:    31/07/2017

Copyright (c) 2016 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc. and/or its affiliated entities, under various laws including copyright, international treaties, patent, and/or contract. Any use of the material herein must be in accordance with the terms of the License. All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import io
import sys
import time
import avro.schema
import avro.io
import os
from random import randint
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

kafka = KafkaClient(sys.argv[1])
producer = SimpleProducer(kafka)

# Path to user.avsc avro schema
schema_path="dataplatform-raw.avsc"

# Kafka topic
topic = "avro.events"
schema = avro.schema.parse(open(schema_path).read())

current_milli_time = lambda: int(round(time.time() * 1000))

seq = 0

MAX_CONTEXT_ID = 16000000

while True:
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write({"src": "raw-events",
                  "timestamp": current_milli_time(),
                  "host_ip": "0.0.0.0",
                  "rawdata": '{"id":"%s", "context_id":"%s", "gen_ts":"%s", "afield":"avalue"}' %(seq,randint(0, MAX_CONTEXT_ID),current_milli_time())}, encoder)
    raw_bytes = bytes_writer.getvalue()
    producer.send_messages(topic, raw_bytes)
    seq += 1
    time.sleep(1)