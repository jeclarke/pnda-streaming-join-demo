#
# Name:       Job.py
# Purpose:    Application entry point to create, configure and start spark streaming job.
# Author:     PNDA team
#
# Created:    31/07/2017
#
#
#
# Copyright (c) 2017 Cisco and/or its affiliates.
#
# This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
# and/or its affiliated entities, under various laws including copyright, international treaties, patent,
# and/or contract. Any use of the material herein must be in accordance with the terms of the License.
# All rights not expressly granted by the License are reserved.
#
# Unless required by applicable law or agreed to separately in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
#
import time
import io
import json
from datetime import datetime
from operator import add

import requests
import avro.io as avro_io
import avro.schema
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

current_milli_time = lambda: int(round(time.time() * 1000))

LOG_LEVEL_ERROR = 0
LOG_LEVEL_INFO = 1
LOG_LEVEL_DEBUG = 2

def log_level_name(level):
    if level == LOG_LEVEL_DEBUG:
        return 'DEBUG'
    elif level == LOG_LEVEL_INFO:
        return 'INFO'
    else:
        return 'ERROR'

def log_level_value(level):
    if level == 2:
        return 'DEBUG'
    elif level == 1:
        return 'INFO'
    else:
        return 'ERROR'

def log_out(level, message):
    if app_log_level >= level:
        print "%s %s %s" % (str(datetime.now()), log_level_name(level), message)

# Load properties, this should be made available by using the --py-files spark-submit argument
app_log_level = LOG_LEVEL_INFO
log_out(LOG_LEVEL_INFO, 'Loading application.properties')
properties = dict(line.strip().split('=', 1) if not line.strip().startswith('#') else [line, None] for line in open('application.properties'))
app_log_level = log_level_value(['component.log_level'])

logger_url = properties['environment.metric_logger_url']
app_name = properties['component.application']
batch_size_seconds = int(properties['component.batch_size_seconds'])
context_dataset_partitions = int(properties['component.context_dataset_partitions'])

sc = SparkContext(appName=app_name)

# Load avro schema, this should be made available by using the --py-files spark-submit argument
avro_schema = avro.schema.parse(open('dataplatform-raw.avsc').read())

# Load context dataset setting the key to the id field from each row
context_dataset = sc.textFile(properties['component.context_dataset_path']).map(lambda row: (str(json.loads(row)['id']), row)).partitionBy(context_dataset_partitions).cache()

# getOrCreate function to set up the processing pipeline
def create_pipeline():
    log_out(LOG_LEVEL_INFO, 'Creating spark context')
    ssc = StreamingContext(sc, batch_size_seconds)

    topics = properties['component.input_topic'].split(',')
    kafka_params = {'metadata.broker.list': properties['environment.kafka_brokers']}
    if properties['component.consume_from_beginning'].lower() == 'true':
        kafka_params['auto.offset.reset'] = 'smallest'

    if 'component.input_dstream_partitions' in properties:
        input_dstream_partitions = int(properties['component.input_dstream_partitions'])
    else:
        input_dstream_partitions = 0

    if 'component.join_output_partitions' in properties and int(properties['component.join_output_partitions']) > 0:
        join_output_partitions = int(properties['component.join_output_partitions'])
    else:
        join_output_partitions = None

    def decode_avro(value):
        avro_message = avro_io.BinaryDecoder(io.BytesIO(value))
        avro_reader = avro_io.DatumReader(avro_schema)
        message = avro_reader.read(avro_message)
        return message

    log_out(LOG_LEVEL_INFO, 'Kafka parameters: %s' % kafka_params)
    messages = KafkaUtils.createDirectStream(ssc, topics, kafka_params, valueDecoder=decode_avro)

    # Optional repartition to set the parallelism for processing on the input RDD
    if input_dstream_partitions > 0:
        messages = messages.repartition(input_dstream_partitions)

    # Add the context ID as the key
    def extract_key(event):
        key = json.loads(event[1]['rawdata'])['context_id']
        return (key, event[1]['rawdata'])

    messages = messages.map(extract_key)

    # Join to context_dataset
    def format_joined_record(record):
        return (record[0], json.loads(record[1][1])['gen_ts'])

    messages_with_context = messages.transform(lambda rdd: context_dataset.join(rdd, join_output_partitions).map(format_joined_record))
    messages_with_context.pprint()
    return ssc

log_out(LOG_LEVEL_INFO, 'Creating pipeline')
application_context = create_pipeline()

# Start the app running
log_out(LOG_LEVEL_INFO, 'Starting spark streaming execution')
log_out(LOG_LEVEL_INFO, 'Logger url: ' + logger_url)
application_context.start()
application_context.awaitTermination()
