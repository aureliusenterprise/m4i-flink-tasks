import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional
from pyflink.common.typeinfo import Types

import requests
# from dataclasses_json import (DataClassJsonMixin, LetterCase, dataclass_json)
from m4i_atlas_core import AtlasChangeMessage, ConfigStore, EntityAuditAction, get_entity_by_guid
from pyflink.common.serialization import SimpleStringSchema, JsonRowSerializationSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, RuntimeContext

from config import config
from credentials import credentials

store = ConfigStore.get_instance()


class MyMapFunction(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        store.load({**config, **credentials})

    def map(self, kafka_notification: str):

        async def func(kafka_notification):
            # return kafka_notification

            logging.warning(repr(kafka_notification))
            kafka_notification = AtlasChangeMessage.from_json(kafka_notification)

            if kafka_notification.message.operation_type in [EntityAuditAction.ENTITY_CREATE, EntityAuditAction.ENTITY_UPDATE, EntityAuditAction.ENTITY_DELETE]:
                entity_guid = kafka_notification.message.entity.guid
                await get_entity_by_guid.cache.clear()
                event_entity = await get_entity_by_guid(guid=entity_guid, ignore_relationships=False)
                logging.warning(repr(kafka_notification))
                logging.warning(repr(event_entity))
                kafka_notification_json =  json.loads(kafka_notification.to_json())
                entity_json =   json.loads(event_entity.to_json())

                logging.warning(json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : entity_json}))
                logging.warning(str(json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : entity_json})))
                return json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : entity_json})

            return json.dumps({"kafka_notification" : {}, "atlas_entity" : {}})
        # END func

        return asyncio.run(func(kafka_notification))



def run_get_entity_job():
    def set_env(env: StreamExecutionEnvironment):
        env.set_python_executable(
            "/mnt/c/users/chari/Downloads/flink_env/bin/python")
        env.set_python_requirements(
            "/mnt/c/users/chari/Downloads/requirements.txt")

        # use this to run code on the cluster 

        # env.add_python_archive(f"flink_env.zip")

        # env.set_python_executable(f"flink_env.zip/flink_env/bin/python")
        # env.add_python_file(
        # "/mnt/c/users/chari/Downloads/flink_env/bin/python")
    env = StreamExecutionEnvironment.get_execution_environment()
    set_env(env)
    env.set_parallelism(1)

    # download JARs
    kafka_jar = f"file:///mnt/c/users/chari/Downloads/jars/flink-connector-kafka_2.11-1.13.6.jar"
    kafka_client = f"file:///mnt/c/users/chari/Downloads/jars/kafka-clients-2.2.1.jar"

    # kafka_jar = f"file:///mnt/c/users/chari/Downloads/flink_jars/flink-connector-kafka_1.15.0.jar"
    # kafka_client = f"file:///mnt/c/users/chari/Downloads/flink_jars/kafka-clients-1.15.0.jar"

    env.add_jars(kafka_jar, kafka_client)

    kafka_source = FlinkKafkaConsumer(topics="ATLAS_ENTITIES",
                                      properties={'bootstrap.servers': '127.0.0.1:9027',
                                                  'group.id': 'test',
                                                  'auto.offset.reset': 'earliest',
                                                  "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
                                                  "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"},
                                      deserialization_schema=SimpleStringSchema()).set_commit_offsets_on_checkpoints(True)

    data_stream = env.add_source(kafka_source).name(f"atlas_events")

    data_stream = data_stream.map(MyMapFunction(), Types.STRING()).name("my_mapping")

    data_stream.add_sink(FlinkKafkaProducer(topic="pyfink_sink",
                                     producer_config={"bootstrap.servers": '127.0.0.1:9027',"max.request.size": "14999999", 'group.id': 'test'},
                                     serialization_schema=SimpleStringSchema())).name("write_to_kafka")




    env.execute("write to pyfink_sink")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")
    run_get_entity_job()
