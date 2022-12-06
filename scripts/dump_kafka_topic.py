# -*- coding: utf-8 -*-
"""
Created on Tue Nov 29 23:39:24 2022

@author: andre
"""

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import sys
import os


def process(topic_name, namespace):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['kafka.'+namespace+'.svc.cluster.local:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='mya-group',
        value_deserializer=lambda x: x.decode('utf-8'))

    for message in consumer:
        print(message.value)

if __name__ == "__main__":
    NAMESPACE = os.getenv('NAMESPACE')
    topic_name = None
    if len(sys.argv)>1:
        topic_name = sys.argv[1]
    if topic_name:
        process(topic_name, NAMESPACE)
    else:
        print("usage: python dump_kafka_topic.py <topic_name>")
        print("available topic names: ATLAS_ENTITIES,DETERMINED_CHANGE,ENRICHED_ENTITIES,ENRICHED_ENTITIES_SAVED,SYNC_ELASTIC,DEAD_LETTER_BOX")
