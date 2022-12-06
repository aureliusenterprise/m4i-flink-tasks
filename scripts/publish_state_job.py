import json
import logging
import sys
import os
import time
import traceback


from m4i_atlas_core import ConfigStore, Entity

from config import config
from credentials import credentials
from m4i_flink_tasks.operation.PublishStateLocal import PublishStateLocal
from m4i_flink_tasks.synchronize_app_search import make_elastic_connection

# from set_environment import set_env

config_store = ConfigStore.get_instance()

from kafka import KafkaProducer
from pyflink.common.serialization import (JsonRowSerializationSchema,
                                          SimpleStringSchema)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import (FlinkKafkaConsumer,
                                           FlinkKafkaProducer)
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.common.typeinfo import Types

from m4i_flink_tasks.DeadLetterBoxMessage import DeadLetterBoxMesage

class PublishState(MapFunction,PublishStateLocal):
    bootstrap_server_hostname=None
    bootstrap_server_port=None
    dead_lettter_box_topic = "deadletterbox"
    producer = None
    config_store = None
    cnt = 0
    cnt_res = 0

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
        logging.info(f"recevied events PublishState: {self.cnt}")

        try:
            res = self.map_local(kafka_notification)
            logging.info("received result: "+repr(res))
            self.cnt_res = self.cnt_res+1
            logging.info(f"submitted event count PublishState: {self.cnt_res}")
            return res
        except Exception as e:
            logging.error("Exception during processing:")
            logging.error(repr(e))

            exc_info = sys.exc_info()
            e1 = (''.join(traceback.format_exception(*exc_info)))

            event = DeadLetterBoxMesage(timestamp=time.time(), original_notification=kafka_notification, job="publish_state", description = (e1),
                                        exception_class = type(e).__name__, remark= None)
            logging.error("this goes into dead letter box: ")
            logging.error(repr(event))

            retry = 0
            while retry < 10:
                try:
                    producer_ = self.get_deadletter()
                    producer_.send(topic=self.dead_lettter_box_topic, value=event.to_json())
                    return
                except Exception as e2:
                    logging.error("error dumping data into deadletter topic "+repr(e2))
                    retry = retry + 1
# end of class PublishState


def run_publish_state_job():

    env = StreamExecutionEnvironment.get_execution_environment()
    # set_env(env)
    env.set_parallelism(1)

    path = os.path.dirname(__file__)

    # download JARs
    kafka_jar = "file:///" + path + "/../flink_jars/flink-connector-kafka-1.15.1.jar"
    kafka_client = "file:///" + path + "/../flink_jars/kafka-clients-2.2.1.jar"

    env.add_jars(kafka_jar, kafka_client)

    bootstrap_server_hostname = config.get("kafka.bootstrap.server.hostname")
    bootstrap_server_port = config.get("kafka.bootstrap.server.port")
    source_topic_name = config.get("enriched.events.topic.name")
    sink_topic_name = source_topic_name+"_SAVED"
    kafka_consumer_group_id = config.get("kafka.consumer.group.id")

    kafka_source = FlinkKafkaConsumer(topics = source_topic_name,
                                      properties={'bootstrap.servers': f"{bootstrap_server_hostname}:{bootstrap_server_port}",
                                                  'group.id': kafka_consumer_group_id+"_publish_state_job",
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

    data_stream = env.add_source(kafka_source).name("publish state source")
    data_stream = data_stream.map(PublishState(), Types.STRING()).name("publish_state mapping").filter(lambda notif: notif)
    data_stream.add_sink(FlinkKafkaProducer(topic=sink_topic_name,
        producer_config={"bootstrap.servers": f"{bootstrap_server_hostname}:{bootstrap_server_port}","max.request.size": "14999999", 'group.id': kafka_consumer_group_id+"_publish_state_job2"},
        serialization_schema=SimpleStringSchema())).name("write_to_kafka publish_state_job")

    env.execute("publish_state_to_elastic_search")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")
    run_publish_state_job()
