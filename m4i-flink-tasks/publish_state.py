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
from m4i_atlas_core import AtlasChangeMessage, ConfigStore as m4i_ConfigStore, EntityAuditAction, get_entity_by_guid, Entity
from pyflink.common.serialization import SimpleStringSchema, JsonRowSerializationSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, RuntimeContext

from config import config
from credentials import credentials

from m4i_data_management import make_elastic_connection
from m4i_data_management import ConfigStore as m4i_ConfigStore

m4i_store = m4i_ConfigStore.get_instance()
m4i_store = m4i_ConfigStore.get_instance()


class MyMapFunction(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        m4i_store.load({**config, **credentials})
        m4i_store.load({**config, **credentials})

    def map(self, kafka_notification: str):
        # kafka_notification = '{"kafka_notification": {"version": {"version": "1.0.0", "versionParts": [1]}, "msgCompressionKind": "NONE", "msgSplitIdx": 1, "msgSplitCount": 1, "msgSourceIP": "127.0.1.1", "msgCreatedBy": "", "msgCreationTime": 1655718091920, "message": {"eventTime": 1655718090946, "operationType": "ENTITY_UPDATE", "type": "ENTITY_NOTIFICATION_V2", "entity": {"typeName": "hdfs_path", "attributes": {"path": "charif-61", "createTime": 1654812000000, "qualifiedName": "charif", "name": "charif"}, "classifications": [], "createTime": null, "createdBy": null, "customAttributes": null, "guid": "36b7f0d4-76f2-405c-8043-f8c143d2c387", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": null, "proxy": null, "relationshipAttributes": null, "status": null, "updateTime": null, "updatedBy": null, "version": null}, "relationship": null}}, "atlas_entity": {"typeName": "hdfs_path", "attributes": {"owner": null, "modifiedTime": 1654812000000, "replicatedTo": [], "userDescription": null, "isFile": false, "numberOfReplicas": 0, "replicatedFrom": [], "qualifiedName": "charif", "displayName": null, "description": null, "extendedAttributes": null, "nameServiceId": null, "path": "charif-61", "posixPermissions": null, "createTime": 1654812000000, "fileSize": 0, "clusterName": null, "name": "charif", "isSymlink": false, "group": null}, "classifications": [], "createTime": 1654853726481, "createdBy": "admin", "customAttributes": null, "guid": "36b7f0d4-76f2-405c-8043-f8c143d2c387", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": null, "proxy": null, "relationshipAttributes": {"inputToProcesses": [], "schema": [], "meanings": [], "outputFromProcesses": []}, "status": "ACTIVE", "updateTime": 1655718090946, "updatedBy": "admin", "version": 0}}'

        kafka_notification_json = json.loads(kafka_notification)
        atlas_entity_json = kafka_notification_json["atlas_entity"]
        atlas_entity = json.dumps(atlas_entity_json)
        logging.warning(atlas_entity)

        atlas_entity = Entity.from_json(atlas_entity)
        
        doc_id = "{}_{}".format(atlas_entity.guid, atlas_entity.update_time)
        
        logging.warning(kafka_notification)
        logging.warning(type(atlas_entity_json))
        
        elastic = make_elastic_connection()
        elastic.index(index="atlas-dev-test", id = doc_id, document=atlas_entity_json)
        elastic.close()

        return kafka_notification
       
def run_publish_state_job():
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
                                      deserialization_schema=SimpleStringSchema()).set_commit_offsets_on_checkpoints(True)

    data_stream = env.add_source(kafka_source)

    

    data_stream = data_stream.map(MyMapFunction(), Types.STRING()).name("my_mapping")


    data_stream.print()


    # data_stream.add_sink(FlinkKafkaProducer(topic="pyfink_sink",
    #                                  producer_config={"bootstrap.servers": '127.0.0.1:9027',"max.request.size": "14999999", 'group.id': 'test'},
    #                                  serialization_schema=SimpleStringSchema())).name("write_to_kafka")



    

    env.execute("puplish state to elastic search")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")
    run_publish_state_job()
