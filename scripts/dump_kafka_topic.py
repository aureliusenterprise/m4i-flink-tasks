# -*- coding: utf-8 -*-
"""
Created on Tue Nov 29 23:39:24 2022

@author: andre
"""

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'DETERMINED_CHANGE',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='mya-group',
     value_deserializer=lambda x: x.decode('utf-8'))

for message in consumer:
    print(message.value)