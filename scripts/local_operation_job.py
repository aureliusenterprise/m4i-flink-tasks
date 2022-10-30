import asyncio
import json
import logging
import sys
import os
from pyflink.common.typeinfo import Types
from m4i_atlas_core import AtlasChangeMessage, ConfigStore as m4i_ConfigStore, EntityAuditAction, Entity

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, RuntimeContext

from config import config
from credentials import credentials

from m4i_flink_tasks.synchronize_app_search import make_elastic_connection,get_child_entity_guids,make_elastic_app_search_connect
from m4i_flink_tasks.operation import OperationEvent
from m4i_flink_tasks.operation import WorkflowEngine
from m4i_flink_tasks.operation import LocalOperationLocal
from m4i_flink_tasks import DeadLetterBoxMesage
import time
from kafka import KafkaProducer
from copy import copy
import traceback
import re
from m4i_atlas_core import get_entity_audit
from m4i_atlas_core import AtlasChangeMessage, EntityAuditAction, get_entity_by_guid, get_keycloak_token
from pyflink.datastream.functions import FlatMapFunction
import copy

m4i_store = m4i_ConfigStore.get_instance()


class LocalOperation(MapFunction, LocalOperationLocal):
    app_search = None
    local_operation = None
    bootstrap_server_hostname = None
    bootstrap_server_port = None
    producer = None
    dead_lettter_box_topic = None
    cnt = 0

    def open(self, runtime_context: RuntimeContext):
        m4i_store.load({**config, **credentials})
        self.open_local(config, credentials, m4i_store)
        self.bootstrap_server_hostname, self.bootstrap_server_port =  m4i_store.get_many("kafka.bootstrap.server.hostname", "kafka.bootstrap.server.port")
        self.dead_lettter_box_topic = m4i_store.get("exception.events.topic.name")


    def get_producer(self):
        if self.producer == None:
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
        logging.info(f"recevied events LocalOperation: {self.cnt}")

        try:
            res = self.map_local(kafka_notification)
            return res

        except Exception as e:
            logging.error("The Kafka notification received could not be handled.")

            exc_info = sys.exc_info()
            e1 = (''.join(traceback.format_exception(*exc_info)))
            logging.error(repr(e))

            event = DeadLetterBoxMesage(timestamp=time.time(), original_notification=kafka_notification, job="local_operation", description = (e1),
                                        exception_class = type(e).__name__, remark= None)
            retry = 0
            while retry<3:
                try:
                    producer_ = self.get_producer()
                    producer_.send(topic = self.dead_lettter_box_topic, value=event.to_json())
                    return
                except Exception as e:
                    logging.error(f"Problems sending a deadletter message : {str(e)}")
                    retry = retry+1
                    self.producer = None

# end of class LocalOperation


class GetResultLocalOperation(FlatMapFunction):
    cnt = 0

    def flat_map(self, input_list):
        for element in input_list:
            self.cnt = self.cnt+1
            logging.info(f"event count GetResultLocalOperation: {self.cnt}")
            yield element



def local_operation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    path = os.path.dirname(__file__)

    # download JARs
    kafka_jar = f"file:///" + path + "/../flink_jars/flink-connector-kafka-1.15.1.jar"
    kafka_client = f"file:///" + path + "/../flink_jars/kafka-clients-2.2.1.jar"


    env.add_jars(kafka_jar, kafka_client)

    bootstrap_server_hostname = config.get("kafka.bootstrap.server.hostname")
    bootstrap_server_port = config.get("kafka.bootstrap.server.port")
    source_topic_name = config.get("sync_elastic.events.topic.name")
    sink_topic_name = source_topic_name
    dead_lettter_box_topic = config.get("exception.events.topic.name")
    kafka_consumer_group_id = config.get("kafka.consumer.group.id")

    kafka_source = FlinkKafkaConsumer(topics = source_topic_name,
                                      properties={'bootstrap.servers':  f"{bootstrap_server_hostname}:{bootstrap_server_port}",
                                                  'group.id': kafka_consumer_group_id+"_local_operation_job",
                                                  "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
                                                  "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"},
                                      deserialization_schema=SimpleStringSchema())
    if kafka_source==None:
        logging.warning("kafka source is empty")
        logging.warning(f"bootstrap_servers: {bootstrap_server_hostname}:{bootstrap_server_port}")
        logging.warning(f"group.id: {kafka_consumer_group_id}_local_operation_job")
        logging.warning(f"topcis: {source_topic_name}")
        raise Exception("kafka source is empty")
    kafka_source.set_commit_offsets_on_checkpoints(True).set_start_from_latest()

    data_stream = env.add_source(kafka_source).name("consuming local operation events")

    data_stream = data_stream.map(LocalOperation(), Types.LIST(element_type_info = Types.STRING())).name("local operation").filter(lambda notif: notif)

    data_stream = data_stream.flat_map(GetResultLocalOperation(), Types.STRING()).name("process operation local_operation")

    #data_stream.print()

    data_stream.add_sink(FlinkKafkaProducer(topic = sink_topic_name,
        producer_config={"bootstrap.servers": f"{bootstrap_server_hostname}:{bootstrap_server_port}","max.request.size": "14999999", 'group.id': kafka_consumer_group_id+"_local_operation_job2"},
        serialization_schema=SimpleStringSchema())).name("write_to_kafka_sink local_operation")


    env.execute("local_operation")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")



    local_operation()
