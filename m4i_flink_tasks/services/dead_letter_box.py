import sys
import time
import traceback

from kafka import KafkaProducer
from m4i_atlas_core import ConfigStore, retry_decorator

from ..model import DeadLetterBoxMesage

store = ConfigStore.get_instance()


class DeadLetterBoxService:

    def __init__(self) -> None:
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

    def handle_error(self, error: Exception, payload: str):
        exc_info = sys.exc_info()
        formatted_exception = ''.join(traceback.format_exception(*exc_info))

        event = DeadLetterBoxMesage(
            timestamp=time.time(),
            original_notification=payload,
            job="determine_change",
            description=formatted_exception,
            exception_class=type(error).__name__,
            remark=""
        )

        self.send(event)
    # END handle_error

    @retry_decorator(retries=2)
    def send(self, event: DeadLetterBoxMesage):
        self.kafka_producer.send(
            topic=self.dead_lettter_box_topic,
            value=event.to_json()
        )
    # END send

# END DeadLetterBoxService
