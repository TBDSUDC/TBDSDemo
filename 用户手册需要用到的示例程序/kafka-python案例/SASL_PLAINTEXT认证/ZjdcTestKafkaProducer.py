#!/usr/bin/env python
#-*-coding=utf-8-*-
# Copyright (C) 2016, Tencent
# All rights reserved.
#
# Filename   : ZjdcTestKafkaProducer.py
# Description: Act as kafka producer to send massage to kafka
# Author     : mikealzhou@tencent.com
# Date       : 2016-08-04
# Last Update: 2016-08-04
# Linux      : 64-bits system

import kafka
import os
import sys


topic_name=sys.argv[1]
message="this is the kafka message"

for i in range(100):
    producer = kafka.KafkaProducer(
        bootstrap_servers='10.0.0.168:6668',
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username='testuser',
        sasl_plain_password='abcd123456'
    )

    producer.send(topic_name, message)
    time.sleep(1)
