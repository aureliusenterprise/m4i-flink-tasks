import logging
import os
import sys
import time
import traceback

from kafka import KafkaProducer
from m4i_atlas_core import ConfigStore
from pyflink.common.serialization import Encoder, SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import (FileSink,
                                                       OutputFileConfig,
                                                       RollingPolicy)
from pyflink.datastream.connectors.kafka import (FlinkKafkaConsumer,
                                                 FlinkKafkaProducer)
from pyflink.datastream.functions import FlatMapFunction, MapFunction

from m4i_flink_tasks import DeadLetterBoxMesage
from m4i_flink_tasks.operation.DetermineChangeLocal import DetermineChangeLocal

store = ConfigStore.get_instance()


def retry(func: Retryable, retries: int = 1, *args, **kwargs):
    for _ in range(retries):
        try:
            func(*args, **kwargs)
            break
        except Exception as e:
            logging.error(f"error in {func.__name__}: {repr(e)}")


class DetermineChange(MapFunction, DetermineChangeLocal):

    def __init__(self):
        bootstrap_server_hostname, bootstrap_server_port = store.get_many(
            "kafka.bootstrap.server.hostname",
            "kafka.bootstrap.server.port",
            all_required=True
        )

        self.kafka_producer = KafkaProducer(
            bootstrap_servers=f"{bootstrap_server_hostname}:{bootstrap_server_port}",
            value_serializer=str.encode,
            request_timeout_ms=1000,
            api_version=(2, 0, 2),
            retries=1,
            linger_ms=1000
        )

        self.dead_lettter_box_topic = store.get(
            "exception.events.topic.name",
            default="deadletterbox"
        )
    # END __init__

    def send_deadletter_message(self, event: DeadLetterBoxMesage):
        self.kafka_producer.send(
            topic=self.dead_lettter_box_topic,
            value=event.to_json()
        )

    def handle_error(self, e: Exception, kafka_notification: str):
        logging.error(f"Exception during processing: {repr(e)}")

        exc_info = sys.exc_info()
        formatted_exception = ''.join(traceback.format_exception(*exc_info))

        event = DeadLetterBoxMesage(
            timestamp=time.time(),
            original_notification=kafka_notification,
            job="determine_change",
            description=formatted_exception,
            exception_class=type(e).__name__,
            remark=None
        )

        logging.error(f"this goes into dead letter box: {repr(event)}")

        for _ in range(2):
            try:
                self.send_deadletter_message(event)
                break
            except Exception as e2:
                logging.error(
                    f"error dumping data into deadletter topic: {repr(e2)}")

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

            event = DeadLetterBoxMesage(timestamp=time.time(), original_notification=kafka_notification, job="determine_change", description=(e1),
                                        exception_class=type(e).__name__, remark=None)
            logging.error("this goes into dead letter box: ")
            logging.error(repr(event))

            retry = 0
            while retry < 2:
                try:
                    producer_ = self.get_deadletter()
                    producer_.send(
                        topic=self.dead_lettter_box_topic, value=event.to_json())
                    return
                except Exception as e2:
                    logging.error(
                        "error dumping data into deadletter topic "+repr(e2))
                    retry = retry + 1
# end of class DetermineChange


class GetResultDetermineChange(FlatMapFunction):
    cnt = 0

    def flat_map(self, input_list):
        for element in input_list:
            self.cnt = self.cnt+1
            logging.info(
                f"submitted event count GetResultDetermineChange: {self.cnt}")
            yield element


def run_determine_change_job(output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    path = os.path.dirname(__file__)

    # download JARs
    kafka_jar = "file:///" + path + "/../flink_jars/flink-connector-kafka-1.15.1.jar"
    kafka_client = "file:///" + path + "/../flink_jars/kafka-clients-2.2.1.jar"

    env.add_jars(kafka_jar, kafka_client)

    bootstrap_server_hostname = config.get("kafka.bootstrap.server.hostname")
    bootstrap_server_port = config.get("kafka.bootstrap.server.port")
    source_topic_name = config.get("enriched.events.topic.name")+"_SAVED"
    sink_topic_name = config.get("determined.events.topic.name")
    kafka_consumer_group_id = config.get("kafka.consumer.group.id")

    kafka_source = FlinkKafkaConsumer(topics=source_topic_name,
                                      properties={'bootstrap.servers':  f"{bootstrap_server_hostname}:{bootstrap_server_port}",
                                                  'group.id': kafka_consumer_group_id+"_determine_change_job",
                                                  "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
                                                  "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"},
                                      deserialization_schema=SimpleStringSchema())
    if kafka_source == None:
        logging.warning("kafka source is empty")
        logging.warning(
            f"bootstrap_servers: {bootstrap_server_hostname}:{bootstrap_server_port}")
        logging.warning(f"group.id: {kafka_consumer_group_id}")
        logging.warning(f"topcis: {source_topic_name}")
        raise Exception("kafka source is empty")
    kafka_source.set_commit_offsets_on_checkpoints(
        True).set_start_from_latest()

    data_stream = env.add_source(kafka_source).name(
        "consuming enriched atlas events determine_change")

    data_stream = data_stream.map(DetermineChange(), Types.LIST(
        element_type_info=Types.STRING())).name("determine change").filter(lambda notif: notif)

    data_stream = data_stream.flat_map(GetResultDetermineChange(
    ), Types.STRING()).name("parse change determine_change")

    data_stream.add_sink(FlinkKafkaProducer(topic=sink_topic_name,
                                            producer_config={"bootstrap.servers": f"{bootstrap_server_hostname}:{bootstrap_server_port}",
                                                             "max.request.size": "14999999", 'group.id': kafka_consumer_group_id+"_determine_change_job2"},
                                            serialization_schema=SimpleStringSchema())).name("write_to_kafka_sink determine_change")

    # define the sink
    if output_path is not None:
        data_stream.add_sink(
            sink_func=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("determine_change_result_")
                .with_part_suffix(".ext")
                .build()
            )
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )

    env.execute("determine_change")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")
    logging.debug(sys.argv)

    debug = False
    if len(sys.argv) > 1:
        debug = 'debug' in sys.argv[1:]

    logging.debug(f"debug: {debug}")

    output_path = None

    if debug:
        output_path = '/tmp/'

    run_determine_change_job(output_path)
# END MAIN
