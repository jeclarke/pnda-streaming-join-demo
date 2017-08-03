/**
  * Name:       App
  * Purpose:    Application entry point to create, configure and start spark streaming job.
  * Author:     PNDA team
  *
  * Created:    07/04/2017
  */

/*
Copyright (c) 2017 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
and/or its affiliated entities, under various laws including copyright, international treaties, patent,
and/or contract. Any use of the material herein must be in accordance with the terms of the License.
All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied.
*/

package com.cisco.pnda.examples.joindemo;

import com.cisco.pnda.examples.joindemo._
import com.cisco.pnda.examples.joindemo.model._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.{StreamingContext, Seconds, Minutes, Time, Duration}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import net.liftweb.json._
import net.liftweb.json.{DefaultFormats, Formats}
import org.apache.log4j.Logger;

object App {

    private[this] val logger = Logger.getLogger(getClass().getName());

    def main(args: Array[String]) = {
        val props = AppConfig.loadProperties()
        val appName = props.getProperty("component.application")
        val batchSizeSeconds = Integer.parseInt(props.getProperty("component.batch_size_seconds"))
        val contextDatasetPartitions = props.getProperty("component.context_dataset_partitions")
        val partitioner = new HashPartitioner(contextDatasetPartitions.toInt)

        val conf = new SparkConf().setAppName(appName)
        conf.registerKryoClasses(Array(classOf[DefaultFormats]))
        val sc = new SparkContext(conf)

        case class ContextEntry(id: Integer, cust_id: String, device_type: String, dev_id: String, dev_ip_a: String, connection_ref: String)
        val contextDataset = sc.textFile(props.getProperty("component.context_dataset_path")).map(row => {
            implicit val formats: Formats = DefaultFormats
            val jValue = parse(row)
            val contextEntry = jValue.extract[ContextEntry]
            (contextEntry.id.toString, row)
        }).partitionBy(partitioner).cache()

        def creatingFunc(): StreamingContext = {

            val ssc = new StreamingContext(sc, Seconds(batchSizeSeconds))

            def readFromKafka (ssc: StreamingContext): DStream[DataPlatformEvent] = {
                val topicsSet = props.getProperty("component.input_topic").split(",").toSet;
                val kafkaParams = collection.mutable.Map[String, String]("metadata.broker.list" -> props.getProperty("environment.kafka_brokers"))
                if (props.getProperty("component.consume_from_beginning").toBoolean)
                {
                    kafkaParams.put("auto.offset.reset", "smallest");
                }
                val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams.toMap, topicsSet);

                val avroSchemaString = StaticHelpers.loadResourceFile("dataplatform-raw.avsc");
                val rawMessages = messages.mapPartitions(partition => {
                    val eventDecoder = new DataPlatformEventDecoder(avroSchemaString);
                    partition.map( x => {
                        val payload = x._2;
                        val dataPlatformEvent = eventDecoder.decode(payload);
                        dataPlatformEvent;
                    });
                });
                rawMessages;
            };

            val eventStream = readFromKafka(ssc)

            case class EventData(id: String, context_id: String, gen_ts: String, afield: String)
            val eventStreamPair = eventStream.map(event => {
                implicit val formats: Formats = DefaultFormats
                val jValue = parse(event.getRawdata())
                val eventData = jValue.extract[EventData]
                (eventData.context_id, event.getRawdata())
            })

            val eventsWithContext = eventStreamPair.transform(rdd => contextDataset.join(rdd, partitioner).map(record => {
                val contextId = record._1
                val eventData = record._2._2
                val contextData = record._2._1
                (contextId, eventData, contextData)
            }))

            eventsWithContext.print
            ssc
        }

        // Create the streaming context, or load a saved one from disk
        val ssc = creatingFunc();

        sys.ShutdownHookThread {
            logger.info("Gracefully stopping Spark Streaming Application")
            ssc.stop(true, true)
            logger.info("Application stopped")
        }

        logger.info("Starting spark streaming execution")
        ssc.start()
        ssc.awaitTermination()
  }
}
