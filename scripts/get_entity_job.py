import logging
import sys
import os
import time
import traceback

from kafka import KafkaProducer

from config import config
from credentials import credentials
from m4i_atlas_core import ConfigStore
from m4i_flink_tasks.operation.GetEntityLocal import GetEntityLocal

from m4i_flink_tasks.DeadLetterBoxMessage import DeadLetterBoxMesage
store = ConfigStore.get_instance()

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.common.typeinfo import Types


class GetEntity(MapFunction,GetEntityLocal):
    bootstrap_server_hostname=None
    bootstrap_server_port=None
    dead_lettter_box_topic = "deadletterbox"
    producer = None
    store = None
    cnt_res = 0
    cnt_rec = 0

    def open(self, runtime_context: RuntimeContext):
        store.load({**config, **credentials})

        self.bootstrap_server_hostname, self.bootstrap_server_port =  store.get_many("kafka.bootstrap.server.hostname", "kafka.bootstrap.server.port")
        self.dead_lettter_box_topic = store.get("exception.events.topic.name")


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
        self.cnt_rec = self.cnt_rec + 1
        logging.info(f"recevied events GetEntity: {self.cnt_rec}")

        try:
            res = self.map_local(kafka_notification)
            logging.info("received result: "+repr(res))
            self.cnt_res = self.cnt_res+1
            logging.info(f"submitted event count GetEntity: {self.cnt_res}")
            return res
        except Exception as e:
            logging.error("Exception during processing:")
            logging.error(repr(e))

            exc_info = sys.exc_info()
            e1 = (''.join(traceback.format_exception(*exc_info)))

            event = DeadLetterBoxMesage(timestamp=time.time(), original_notification=repr(kafka_notification), job="get_entity", description = (e1),
                                        exception_class = type(e).__name__, remark= None)
            logging.error("this goes into dead letter box: ")
            logging.error(repr(event))

            retry = 0
            while retry < 2:
                try:
                    producer_ = self.get_deadletter()
                    producer_.send(topic=self.dead_lettter_box_topic, value=event.to_json())
                    return
                except Exception as e2:
                    logging.error("error dumping data into deadletter topic "+repr(e2))
                    retry = retry + 1


def run_get_entity_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    #set_env(env)
    env.set_parallelism(1)

    path = os.path.dirname(__file__)

    # download JARs
    kafka_jar = "file:///" + path + "/../flink_jars/flink-connector-kafka-1.15.1.jar"
    kafka_client = "file:///" + path + "/../flink_jars/kafka-clients-2.2.1.jar"

    env.add_jars(kafka_jar, kafka_client)

    bootstrap_server_hostname = config.get("kafka.bootstrap.server.hostname")
    bootstrap_server_port = config.get("kafka.bootstrap.server.port")
    source_topic_name = config.get("atlas.audit.events.topic.name")
    sink_topic_name = config.get("enriched.events.topic.name")
    kafka_consumer_group_id = config.get("kafka.consumer.group.id")

    kafka_source = FlinkKafkaConsumer(topics=source_topic_name,
                                      properties={'bootstrap.servers': f"{bootstrap_server_hostname}:{bootstrap_server_port}",
                                                  'group.id': kafka_consumer_group_id+"_get_entity_job",
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

    data_stream = env.add_source(kafka_source).name("consuming atlas events run_get_entity")

    data_stream = data_stream.map(GetEntity(), Types.STRING()).name("retrieve entity from atlas run_get_entity").filter(lambda notif: notif)

    data_stream.add_sink(FlinkKafkaProducer(topic=sink_topic_name,
        producer_config={"bootstrap.servers": f"{bootstrap_server_hostname}:{bootstrap_server_port}","max.request.size": "14999999", 'group.id': kafka_consumer_group_id+"_get_entity_job2"},
        serialization_schema=SimpleStringSchema())).name("write_to_kafka run_get_entity")

    env.execute("get_atlas_entity")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")
    run_get_entity_job()
