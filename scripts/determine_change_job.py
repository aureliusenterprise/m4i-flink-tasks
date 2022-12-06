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

import pandas as pd
from m4i_flink_tasks.synchronize_app_search import make_elastic_connection
from m4i_flink_tasks import EntityMessage
from m4i_flink_tasks import DeadLetterBoxMesage
import time
from kafka import KafkaProducer
from copy import copy
import traceback
import re
from m4i_atlas_core import get_entity_audit
from m4i_atlas_core import AtlasChangeMessage, EntityAuditAction, get_entity_by_guid, get_keycloak_token
from m4i_flink_tasks.operation.DetermineChangeLocal import DetermineChangeLocal
from pyflink.datastream.functions import FlatMapFunction

store = m4i_ConfigStore.get_instance()

# inserted_attributes = []
# changed_attributes = []
# deleted_attributes = []

# inserted_relationships = {}
# changed_relationships = {}
# deleted_relationships = {}


class DetermineChange(MapFunction,DetermineChangeLocal):
    bootstrap_server_hostname=None
    bootstrap_server_port=None
    dead_lettter_box_topic = "deadletterbox"
    store = None
    cnt = 0
    producer = None

    def open(self, runtime_context: RuntimeContext):
        store.load({**config, **credentials})
        self.bootstrap_server_hostname, self.bootstrap_server_port =  store.get_many("kafka.bootstrap.server.hostname", "kafka.bootstrap.server.port")
        self.dead_lettter_box_topic = store.get("exception.events.topic.name")

        self.open_local(config, credentials, store)

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
        logging.info(f"recevied events DetermineChange: {self.cnt}")
        try:
            res = self.map_local(kafka_notification)
            logging.info("received result: "+repr(res))
            return res
        except Exception as e:
            logging.error("Exception during processing:")
            logging.error(repr(e))

            exc_info = sys.exc_info()
            e1 = (''.join(traceback.format_exception(*exc_info)))

            event = DeadLetterBoxMesage(timestamp=time.time(), original_notification=kafka_notification, job="determine_change", description = (e1),
                                        exception_class = type(e).__name__, remark= None)
            logging.error("this goes into dead letter box: ")
            logging.error(repr(event))

            retry = 0
            while retry <10:
                try:
                    producer_ = self.get_deadletter()
                    producer_.send(topic=self.dead_lettter_box_topic, value=event.to_json())
                    return
                except Exception as e2:
                    logging.error("error dumping data into deadletter topic "+repr(e2))
                    retry = retry + 1
# end of class DetermineChange



class GetResultDetermineChange(FlatMapFunction):
    cnt = 0

    def flat_map(self, input_list):
        for element in input_list:
            self.cnt = self.cnt+1
            logging.info(f"submitted event count GetResultDetermineChange: {self.cnt}")
            yield element



def run_determine_change_job(output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    path = os.path.dirname(__file__)

    # download JARs
    kafka_jar = "file:///" + path + "/../flink_jars/flink-connector-kafka-1.15.1.jar"
    kafka_client = "file:///" + path + "/../flink_jars/kafka-clients-2.2.1.jar"
    file_sink = "file:///" + path + "/../flink_jars/kafka-clients-2.2.1.jar"

    env.add_jars(kafka_jar, kafka_client)

    bootstrap_server_hostname = config.get("kafka.bootstrap.server.hostname")
    bootstrap_server_port = config.get("kafka.bootstrap.server.port")
    source_topic_name = config.get("enriched.events.topic.name")+"_SAVED"
    sink_topic_name = config.get("determined.events.topic.name")
    kafka_consumer_group_id = config.get("kafka.consumer.group.id")

    kafka_source = FlinkKafkaConsumer(topics = source_topic_name,
                                      properties={'bootstrap.servers':  f"{bootstrap_server_hostname}:{bootstrap_server_port}",
                                                  'group.id': kafka_consumer_group_id+"_determine_change_job",
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

    data_stream = env.add_source(kafka_source).name("consuming enriched atlas events determine_change")

    data_stream = data_stream.map(DetermineChange(), Types.LIST(element_type_info = Types.STRING())).name("determine change").filter(lambda notif: notif)

    data_stream = data_stream.flat_map(GetResultDetermineChange(), Types.STRING()).name("parse change determine_change")

    data_stream.add_sink(FlinkKafkaProducer(topic = sink_topic_name,
        producer_config={"bootstrap.servers": f"{bootstrap_server_hostname}:{bootstrap_server_port}","max.request.size": "14999999", 'group.id': kafka_consumer_group_id+"_determine_change_job2"},
        serialization_schema=SimpleStringSchema())).name("write_to_kafka_sink determine_change")

    # define the sink
    if output_path is not None:
        data_stream.add_sink(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("determine_change_result_")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )

    env.execute("determine_change")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")
    print(sys.argv)
    debug = False
    if len(sys.argv)>1:
        debug = 'debug' in sys.argv[1:]
    print(f"debug: {debug}")
    output_path = None
    if debug:
        output_path = '/tmp/'
    run_determine_change_job(output_path)
