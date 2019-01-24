#!/usr/bin/env python
#-*-coding=utf-8-*-
# Copyright (C) 2017, Tencent
# All rights reserved.

from kafka import KafkaConsumer
import os
import sys


topic_name=sys.argv[1]

consumer = KafkaConsumer(
    topic_name,
    group_id="groupid1",
    bootstrap_servers="10.0.0.168:6668",
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="testuser",
    sasl_plain_password="abcd123456"
)

for message in consumer:
    print message.value
