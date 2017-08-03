/*
Name:       Event.java
Purpose:    A PNDA event.
Author:     PNDA team

Created:    31/07/2017

Copyright (c) 2017 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc. and/or its affiliated entities, under various laws including copyright, international treaties, patent, and/or contract. Any use of the material herein must be in accordance with the terms of the License. All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*/
package com.cisco.pnda.examples.joindemo;

import java.io.Serializable;

public class Event implements Serializable
{
    private static final long serialVersionUID = 1L;

    private String _src;
    private Long _timestamp;
    private String _hostIp;
    private String _rawdata;

    public Event(String src, Long timestamp, String host_ip, String rawdata)
    {
        _src = src;
        _timestamp = timestamp;
        _hostIp = host_ip;
        _rawdata = rawdata;
    }

    public String getSrc()
    {
        return _src;
    }

    public void setSrc(String src)
    {
        _src = src;
    }

    public Long getTimestamp()
    {
        return _timestamp;
    }

    public void setTimestamp(Long timestamp)
    {
        _timestamp = timestamp;
    }

    public String getHostIp()
    {
        return _hostIp;
    }

    public void setHostIp(String host_ip)
    {
        _hostIp = host_ip;
    }

    public String getRawdata()
    {
        return _rawdata;
    }

    public void setRawdata(String rawdata)
    {
        _rawdata = rawdata;
    }

    @Override
    public boolean equals(Object other)
    {
        boolean result = false;
        if (other instanceof Event)
        {
            Event that = (Event) other;
            result = (this.getSrc().equals(that.getSrc())
                    && this.getTimestamp().equals(that.getTimestamp())
                    && this.getHostIp().equals(that.getHostIp())
                    && this.getRawdata().equals(that.getRawdata()));
        }
        return result;

    }
}
