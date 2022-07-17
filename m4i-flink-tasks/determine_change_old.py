import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional
from pyflink.common.typeinfo import Types
from m4i_atlas_core import AtlasChangeMessage, ConfigStore as m4i_ConfigStore, EntityAuditAction, get_entity_by_guid, Entity

import requests
# from dataclasses_json import (DataClassJsonMixin, LetterCase, dataclass_json)
# from m4i_atlas_core import AtlasChangeMessage, ConfigStore as m4i_ConfigStore, EntityAuditAction, get_entity_by_guid
from pyflink.common.serialization import SimpleStringSchema, JsonRowSerializationSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, RuntimeContext

from config import config
from credentials import credentials

import pandas as pd

from m4i_data_management import make_elastic_connection, retrieve_elastic_data
from m4i_data_management import ConfigStore as m4i_ConfigStore

m4i_store = m4i_ConfigStore.get_instance()
m4i_store = m4i_ConfigStore.get_instance()

elastic_search_index = "atlas-dev-test"


class MyMapFunction(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        m4i_store.load({**config, **credentials})
        m4i_store.load({**config, **credentials})

    def map(self, kafka_notification: str):
        logging.warning("start")
        logging.warning(kafka_notification)

        kafka_notification_json = json.loads(kafka_notification)

        if not kafka_notification_json.get("kafka_notification") or not kafka_notification_json.get("atlas_entity"):
            return "result"

        atlas_kafka_notification_json = kafka_notification_json["kafka_notification"]
        atlas_entity_json = kafka_notification_json["atlas_entity"]

        atlas_kafka_notification = json.dumps(atlas_kafka_notification_json)
        atlas_kafka_notification = AtlasChangeMessage.from_json(atlas_kafka_notification)

        if atlas_kafka_notification.message.operation_type == EntityAuditAction.ENTITY_UPDATE:

            atlas_entity = json.dumps(atlas_entity_json)
            atlas_entity = Entity.from_json(atlas_entity)

            latest_update_time = atlas_entity.update_time
            entity_guid = atlas_entity.guid

            elastic = make_elastic_connection()

            query = {
                    "bool": {
                        "filter": [
                        {
                            "match": {
                            "guid.keyword": entity_guid
                            }
                        },
                        {
                            "range": {
                            "updateTime": {
                                "lt": latest_update_time
                            }
                            }
                        }
                        ]
                    }
                }


            sort = {
                "updateTime": {"numeric_type" : "long", "order": "desc"}
            }

            result = elastic.search(index = elastic_search_index, query = query, sort = sort, size = 1) 
        # return "result"
            if result["hits"]["total"]["value"] >= 1:
                previous_entity = result["hits"]["hits"][0]["_source"]
                logging.warning(previous_entity)
                # previous_entity = pd.DataFrame(previous_entity)
                previous_entity = pd.DataFrame.from_dict(previous_entity, orient = "index").transpose()
                
                # logging.warning(previous_entity)
                # previous_entity = json.dumps(previous_entity)
                # previous_entity = Entity.from_json(previous_entity)

                attributes = pd.json_normalize(previous_entity['attributes'].tolist()).add_prefix('attributes.')
                relationship_attributes = pd.json_normalize(previous_entity['relationshipAttributes'].tolist()).add_prefix('relationshipAttributes.')

                previous_entity = previous_entity.drop(columns=["attributes","relationshipAttributes"])

                previous_entity = pd.concat([previous_entity, attributes, relationship_attributes], axis = 1)
                logging.warning(previous_entity.to_string())

                current_atlas_entity = pd.DataFrame.from_dict(atlas_entity_json, orient = "index").transpose()
                # current_atlas_entity = pd.DataFrame(atlas_entity_json)
                

                attributes = pd.json_normalize(current_atlas_entity['attributes'].tolist()).add_prefix('attributes.')
                relationship_attributes = pd.json_normalize(current_atlas_entity['relationshipAttributes'].tolist()).add_prefix('relationshipAttributes.')

                current_atlas_entity = current_atlas_entity.drop(columns=["attributes","relationshipAttributes"])

                current_atlas_entity = pd.concat([current_atlas_entity, attributes, relationship_attributes], axis = 1)
                
                logging.warning(atlas_entity_json)
                logging.warning(current_atlas_entity.to_string())
                

                comparison = current_atlas_entity.iloc[0]==previous_entity.iloc[0]

                changed_attributes = comparison[comparison==False].index.to_list()

                logging.warning(changed_attributes)

        logging.warning("end")
            

        return "result"
    
def determine_change():
    def set_env(env: StreamExecutionEnvironment):
        env.set_python_executable(
            "/mnt/c/users/chari/Downloads/flink_env/bin/python")
        env.set_python_requirements(
            "/mnt/c/users/chari/Downloads/requirements.txt")
        # env.add_python_file(
        # "/mnt/c/users/chari/Downloads/flink_env/bin/python")
    env = StreamExecutionEnvironment.get_execution_environment()
    set_env(env)
    env.set_parallelism(1)

    # download JARs
    kafka_jar = f"file:///mnt/c/users/chari/Downloads/jars/flink-connector-kafka_2.11-1.13.6.jar"
    kafka_client = f"file:///mnt/c/users/chari/Downloads/jars/kafka-clients-2.2.1.jar"

    env.add_jars(kafka_jar, kafka_client)

    kafka_source = FlinkKafkaConsumer(topics="pyfink_sink",
                                      properties={'bootstrap.servers': '127.0.0.1:9027',
                                                  'group.id': 'test',
                                                  'auto.offset.reset': 'earliest',
                                                  "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
                                                  "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"},
                                      deserialization_schema=SimpleStringSchema()).set_commit_offsets_on_checkpoints(True).set_start_from_earliest()

    data_stream = env.add_source(kafka_source)

    data_stream = data_stream.map(MyMapFunction(), Types.STRING()).name("my_mapping")

    data_stream.print()

    env.execute("determine change")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")
    determine_change()