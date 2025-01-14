import asyncio
import json
import logging
import sys
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional
from elastic_app_search import Client

from pyflink.common.typeinfo import Types
from m4i_flink_tasks import create_document, delete_document,  handle_updated_attributes, handle_deleted_attributes, handle_inserted_relationships, handle_deleted_relationships

from m4i_atlas_core import ConfigStore
from config import config
from credentials import credentials

config_store = ConfigStore.get_instance()

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import FlatMapFunction


from m4i_flink_tasks import EntityMessage
from m4i_flink_tasks import DeadLetterBoxMesage
from m4i_flink_tasks.operation import UpdateLocalAttributeProcessor, OperationEvent, OperationChange, Sequence, WorkflowEngine
from m4i_flink_tasks.operation.SynchronizeAppsearchLocal import SynchronizeAppsearchLocal

import time
from kafka import KafkaProducer
import traceback
from elastic_enterprise_search import EnterpriseSearch, AppSearch
# from set_environment import set_env
import jsonpickle
import uuid
import datetime

class FailedSendingDeadLetterMessage(Exception):
    pass


class SynchronizeAppsearch(MapFunction,SynchronizeAppsearchLocal):
    bootstrap_server_hostname=None
    bootstrap_server_port=None
    dead_lettter_box_topic = None
    producer = None
    config_store = None
    cnt = 0

    def open(self, runtime_context: RuntimeContext):
        config_store.load({**config, **credentials})
        self.bootstrap_server_hostname, self.bootstrap_server_port =  config_store.get_many("kafka.bootstrap.server.hostname", "kafka.bootstrap.server.port")
        self.dead_lettter_box_topic = config_store.get("exception.events.topic.name")

        self.open_local(config, credentials, config_store)


    def get_deadletter(self):
        if self.producer==None:
            self.producer = KafkaProducer(
                    bootstrap_servers=  f"{self.bootstrap_server_hostname}:{self.bootstrap_server_port}",
                    value_serializer=str.encode,
                    request_timeout_ms = 1000,
                    api_version = (2,0,2),
                    retries = 1,
                    linger_ms = 1000
                )
        return self.producer


    def map(self, kafka_notification: str):
        self.cnt = self.cnt + 1
        logging.info(f"recevied events SynchronizeAppsearch: {self.cnt}")
        try:
            res = (self.map_local(kafka_notification))
            logging.info("received result: "+repr(res))
            return res
        except Exception as e:
            logging.error("Exception during processing:")
            logging.error((e))

            exc_info = sys.exc_info()
            e1 = (''.join(traceback.format_exception(*exc_info)))

            event = DeadLetterBoxMesage(timestamp=time.time(), original_notification=kafka_notification, job="synchronize_appsearch", description = (e1),
                                        exception_class = type(e).__name__, remark= None)
            logging.error("this goes into dead letter box: ")
            logging.error(repr(event))

            retry = 0
            while retry <2:
                try:
                    producer_ = self.get_deadletter()
                    producer_.send(topic=self.dead_lettter_box_topic, value=event.to_json())
                    return
                except Exception as e2:
                    logging.error("error dumping data into deadletter topic "+repr(e2))
                retry = retry + 1
            raise FailedSendingDeadLetterMessage("failed sending message with content "+repr(e2))


class GetResultSyncronizeAppSearch(FlatMapFunction):
    cnt = 0

    def flat_map(self, input_list):
        for element in input_list:
            logging.info(element)
            self.cnt = self.cnt+1
            logging.info(f"submitted event count GetResultSyncronizeAppSearch: {self.cnt}")
            yield element



def run_synchronize_app_search_job():

    env = StreamExecutionEnvironment.get_execution_environment()
    # set_env(env)
    env.set_parallelism(1)

    path = os.path.dirname(__file__)

    # download JARs
    kafka_jar = f"file:///" + path + "/../flink_jars/flink-connector-kafka-1.15.1.jar"
    kafka_client = f"file:///" + path + "/../flink_jars/kafka-clients-2.2.1.jar"

    env.add_jars(kafka_jar, kafka_client)

    bootstrap_server_hostname = config.get("kafka.bootstrap.server.hostname")
    bootstrap_server_port = config.get("kafka.bootstrap.server.port")
    source_topic_name = config.get("determined.events.topic.name")
    sink_topic_name = config.get("sync_elastic.events.topic.name")
    kafka_consumer_group_id = config.get("kafka.consumer.group.id")

    kafka_source = FlinkKafkaConsumer(topics = source_topic_name,
                                      properties={'bootstrap.servers':  f"{bootstrap_server_hostname}:{bootstrap_server_port}",
                                                  'group.id': kafka_consumer_group_id+"_sync_appsearch",
                                                  "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
                                                  "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"},
                                      deserialization_schema=SimpleStringSchema())
    if kafka_source==None:
        logging.warning("kafka source is empty")
        logging.warning(f"bootstrap_servers: {bootstrap_server_hostname}:{bootstrap_server_port}")
        logging.warning(f"group.id: {kafka_consumer_group_id}")
        logging.warning(f"topcis: {source_topic_name}")
        raise Exception("kafka source is empty")
    kafka_source.set_commit_offsets_on_checkpoints(True).set_start_from_latest()

    data_stream = env.add_source(kafka_source).name("consuming determined change events _sync_appsearch")

    data_stream = data_stream.map(SynchronizeAppsearch(), Types.LIST(element_type_info = Types.STRING())).name("synchronize appsearch").filter(lambda notif: notif)

    data_stream = data_stream.flat_map(GetResultSyncronizeAppSearch(), Types.STRING()).name("parse change _sync_appsearch")

    data_stream.add_sink(FlinkKafkaProducer(topic = sink_topic_name,
        producer_config={"bootstrap.servers": f"{bootstrap_server_hostname}:{bootstrap_server_port}","max.request.size": "14999999", 'group.id': kafka_consumer_group_id+"_sync_appsearch2"},
        serialization_schema=SimpleStringSchema())).name("write_to_kafka_sink sync_appsearch")

    env.execute("synchronize app search")



if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")

    run_synchronize_app_search_job()

