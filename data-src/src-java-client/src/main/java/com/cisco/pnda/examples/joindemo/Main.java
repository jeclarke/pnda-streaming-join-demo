/*
Name:       Main.java
Purpose:    Test program to push data into kafka for consumption by the example spark streaming app.
            Not intended for any kind of serious purpose.
Author:     PNDA team

Created:    31/07/2017

Copyright (c) 2017 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc. and/or its affiliated entities, under various laws including copyright, international treaties, patent, and/or contract. Any use of the material herein must be in accordance with the terms of the License. All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*/
package com.cisco.pnda.examples.joindemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.cisco.pnda.examples.joindemo.EventDecoder;
import com.cisco.pnda.examples.joindemo.Event;
import java.util.concurrent.ThreadLocalRandom;

import java.util.Properties;


public final class Main {

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: java -jar join-demosrc.jar kafka-broker1 topic [nsleep tsleep]\noptionally sleep for tsleep milliseconds every nsleep messages to control send rate");
      System.exit(1);
    }
    int nsleep = 0;
    int tsleep = 0;
    if (args.length == 4) {
        nsleep = Integer.parseInt(args[2]);
        tsleep = Integer.parseInt(args[3]);
    }
    Properties props = new Properties();
    props.put("bootstrap.servers", args[0]);
    props.put("acks", "1");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    Producer<byte[], byte[]> producer = new KafkaProducer<>(props);
    long seq = 0;
    try {
      EventDecoder avroCoder = new EventDecoder("{\"namespace\": \"pnda.entity\", \"type\": \"record\", \"name\": \"event\", \"fields\": [ {\"name\": \"timestamp\", \"type\": \"long\"}, {\"name\": \"src\", \"type\": \"string\"}, {\"name\": \"host_ip\", \"type\": \"string\"}, {\"name\": \"rawdata\", \"type\": \"bytes\"}]}\"");
      while(true) {
          long contextId = ThreadLocalRandom.current().nextLong(0, 16000000);
          long timeMillis = System.currentTimeMillis();
          String payload = String.format("{\"id\":\"%d\", \"context_id\":\"%s\", \"gen_ts\":\"%s\", \"afield\":\"avalue\"}", seq, contextId, timeMillis);
          Event event = new Event("raw-events", System.currentTimeMillis(), "0.0.0.0", payload);
          producer.send(new ProducerRecord<byte[], byte[]>(args[1], avroCoder.encode(event)));
          seq++;
          // Use this to control the send rate
          if (nsleep > 0 && seq % nsleep == 0) {
            Thread.sleep(tsleep);
          }
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
    finally {
      producer.close();
    }
  }
}
