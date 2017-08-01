#
# Name:       context.py
# Purpose:    generate a context dataset
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

import uuid
import json
from random import randint

MAX_CONTEXT_ID = 16000000

with open('context.json', 'w') as f:
    for context_id in xrange(MAX_CONTEXT_ID):
        record = {
            "id": context_id,
            "cust_id":"cust_%03d" % randint(0, 9999),
            "device_type":"genco box %d000" % randint(1, 5),
            "dev_id":"%s" % str(uuid.uuid4()),
            "dev_ip_a":"2001:0db8:85a3:0000:0000:8a2e:%04d:%04d" % (randint(0, 9999), randint(0, 9999)),
            "connection_ref":"link%s" % randint(0, 100)
        }
        f.write(json.dumps(record) + '\n')

