# -*- coding: utf-8 -*-
"""
Created on Tue Nov 29 23:39:24 2022

@author: andre
"""

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import sys

def process(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='mya-group',
        value_deserializer=lambda x: x.decode('utf-8'))

    ii = 0
    for message in consumer:
        print(ii)
        ii = ii+1
        print(message.value)

if __name__ == "__main__":
    topic_name = sys.argv[1]
    if topic_name:
        process(topic_name)
    else:
        print("usage: python dump_kafka_topic.py <topic_name>")
        print("available topic names: ATLAS_ENTITIES,DETERMINED_CHANGE,ENRICHED_ENTITIES,ENRICHED_ENTITIES_SAVED,SYNC_ELASTIC,DEAD_LETTER_BOX")
