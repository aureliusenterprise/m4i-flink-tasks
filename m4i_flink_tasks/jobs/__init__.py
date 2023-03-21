import logging
import sys
import time
import traceback

from kafka import KafkaProducer
from m4i_atlas_core import ConfigStore, retry_decorator
from pyflink.common.serialization import Encoder, SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import (DataStream, SinkFunction, SourceFunction,
                                StreamExecutionEnvironment)
from pyflink.datastream.connectors.file_system import (FileSink,
                                                       OutputFileConfig,
                                                       RollingPolicy)
from pyflink.datastream.connectors.kafka import (FlinkKafkaConsumer,
                                                 FlinkKafkaProducer)
from pyflink.datastream.functions import FlatMapFunction, MapFunction

from m4i_flink_tasks import DeadLetterBoxMesage
from m4i_flink_tasks.operation.DetermineChangeLocal import DetermineChangeLocal

store = ConfigStore.get_instance()


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

    @retry_decorator(retries=2)
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
            remark=""
        )

        logging.error(f"this goes into dead letter box: {repr(event)}")

        try:
            self.send_deadletter_message(event)
        except Exception as e:
            logging.error(
                f"error dumping data into deadletter topic: {repr(e)}"
            )
        # END TRY
    # END handle_error

    def map(self, kafka_notification: str):
        try:
            res = self.map_local(kafka_notification)
            return res
        except Exception as e:
            self.handle_error(e, kafka_notification)
        # END TRY
    # END map
# end of class DetermineChange


class GetResultDetermineChange(FlatMapFunction):
    def flat_map(self, input_list):
        for element in input_list:
            yield element
        # END LOO
    # END flat_map
# END GetResultDetermineChange


def create_kafka_source():
    bootstrap_server_hostname, bootstrap_server_port, kafka_consumer_group_id, source_topic_name = store.get_many(
        "kafka.bootstrap.server.hostname",
        "kafka.bootstrap.server.port",
        "kafka.consumer.group.id",
        "enriched.events.topic.name",
        all_required=True
    )

    consumer_properties = {
        "bootstrap.servers": f"{bootstrap_server_hostname}:{bootstrap_server_port}",
        "group.id": kafka_consumer_group_id
    }

    consumer = FlinkKafkaConsumer(
        topics=source_topic_name,
        properties=consumer_properties,
        deserialization_schema=SimpleStringSchema()
    )

    (
        consumer
        .set_commit_offsets_on_checkpoints(True)
        .set_start_from_latest()
    )

    return consumer
# END create_kafka_source


def create_kafka_sink():
    bootstrap_server_hostname, bootstrap_server_port, sink_topic_name = store.get_many(
        "kafka.bootstrap.server.hostname",
        "kafka.bootstrap.server.port",
        "determined.events.topic.name",
        all_required=True
    )

    producer_properties = {
        "bootstrap.servers": f"{bootstrap_server_hostname}:{bootstrap_server_port}",
        "max.request.size": "14999999",
    }

    return FlinkKafkaProducer(
        topic=sink_topic_name,
        producer_config=producer_properties,
        serialization_schema=SimpleStringSchema(),
    )
# END create_kafka_sink


def create_file_sink():
    sink = (
        FileSink.for_row_format(
            base_path=output_path,
            encoder=Encoder.simple_string_encoder()
        )
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix("determine_change_result_")
            .with_part_suffix(".ext")
            .build()
        )
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .build()
    )

    return sink
# END create_file_sink


class DetermineChangePipeline:

    def __init__(self, stream: DataStream):
        self.changes = (
            stream
            .map(DetermineChange(), Types.LIST(element_type_info=Types.STRING()))
            .name("determine changes")
        )

        self.parsed_changes = (
            self.changes
            .filter(lambda notif: notif)
            .flat_map(GetResultDetermineChange(), Types.STRING())
            .name("parse changes")
        )
    # END __init__

# END DetermineChangePipeline


def create_stream_execution_environment():
    env = StreamExecutionEnvironment.get_execution_environment()

    # PLACEHOLDER CONFIG KEY
    parallelism = store.get("determine.changes.parallelism", default=1)
    env.set_parallelism(parallelism)

    # PLACEHOLDER CONFIG KEY
    dependencies = store.get("determine.changes.dependencies", default=[])
    env.add_jars(*dependencies)

    return env
# END create_stream_execution_environment


class DetermineChangeJob:
    def __init__(self, event_source: SourceFunction = None, event_sink: SinkFunction = None):
        self.env = create_stream_execution_environment()

        self.source = (
            self.env
            .add_source(event_source)
            .name("changes_source")
        )

        pipeline = DetermineChangePipeline(self.source).parsed_changes.add_sink()

        self.sink = (
            pipeline.parsed_changes
            .add_sink(event_sink)
            .name("output changes")
        )
    # END __init__
# END DetermineChangeJob


def create_determine_change_job(output_path: str):

    env = create_stream_execution_environment()

    kafka_source = create_kafka_source()
    kafka_sink = create_kafka_sink()

    stream = DetermineChangePipeline(env, kafka_source)

    # If output_path is None, sink to Kafka. Else, sink to file.
    if output_path is None:

        kafka_sink = create_kafka_sink()

        (
            stream.parsed_changes
            .add_sink(kafka_sink)
            .name("write_to_kafka_sink determine_change")
        )
    else:
        file_sink = create_file_sink()

        (
            stream.parsed_changes
            .add_sink(sink_func=file_sink)
            .name("write_to_file_sink determine_change")
        )
    # END IF

    return env
# END create_determine_change_job
