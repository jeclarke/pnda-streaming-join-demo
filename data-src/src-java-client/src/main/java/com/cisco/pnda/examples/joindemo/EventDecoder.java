/*
Name:       EventDecoder.java
Purpose:    Decode and encode avro events for PNDA
Author:     PNDA team

Created:    31/07/2017

Copyright (c) 2017 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc. and/or its affiliated entities, under various laws including copyright, international treaties, patent, and/or contract. Any use of the material herein must be in accordance with the terms of the License. All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*/
package com.cisco.pnda.examples.joindemo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

public class EventDecoder
{
    private Schema.Parser _parser = new Schema.Parser();
    private DatumReader<GenericRecord> _reader;
    private DatumWriter<GenericRecord> _writer;
    private Schema _schema;

    public EventDecoder(String schemaDef) throws IOException
    {
        _schema = _parser.parse(schemaDef);
        _reader = new GenericDatumReader<GenericRecord>(_schema);
        _writer = new GenericDatumWriter<GenericRecord>(_schema);
    }

    public Event decode(byte[] data) throws IOException
    {

        try
        {
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        GenericRecord r = _reader.read(null, decoder);
        return new Event((String) r.get("src").toString(),
                (Long) r.get("timestamp"),
                (String) r.get("host_ip").toString(),
                new String(((ByteBuffer)r.get("rawdata")).array()));
        }
        catch(Exception ex)
        {
            throw ex;
        }
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
    public static String hexStr(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public byte[] encode(Event e) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericRecord datum = new GenericData.Record(_schema);
        datum.put("src", e.getSrc());
        datum.put("timestamp", e.getTimestamp());
        datum.put("host_ip", e.getHostIp());
        datum.put("rawdata", ByteBuffer.wrap(e.getRawdata().getBytes("UTF-8")));
        _writer.write(datum, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }
}