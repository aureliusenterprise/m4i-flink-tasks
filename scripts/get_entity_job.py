import asyncio
import json
import logging
import sys
import os

from pyflink.common.typeinfo import Types

from m4i_atlas_core import AtlasChangeMessage, ConfigStore, EntityAuditAction, get_entity_by_guid, get_keycloak_token
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import MapFunction, RuntimeContext
from kafka import KafkaProducer
import time
import os
from m4i_flink_tasks.DeadLetterBoxMessage import DeadLetterBoxMesage
from config import config
from credentials import credentials
import traceback
from  aiohttp.client_exceptions import ClientResponseError
# from set_environment import set_env

store = ConfigStore.get_instance()


class GetEntity(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        store.load({**config, **credentials})

    def map(self, kafka_notification: str):
        async def get_entity(kafka_notification):

            logging.warning(repr(kafka_notification))
            kafka_notification = AtlasChangeMessage.from_json(kafka_notification)
            access_token = get_keycloak_token()
            logging.warning(access_token)

            if kafka_notification.message.operation_type in [EntityAuditAction.ENTITY_CREATE, EntityAuditAction.ENTITY_UPDATE]:
                entity_guid = kafka_notification.message.entity.guid
                await get_entity_by_guid.cache.clear()
                event_entity = await get_entity_by_guid(guid=entity_guid, ignore_relationships=False, access_token=access_token)
                # event_entity = await get_entity_by_guid(guid=entity_guid, ignore_relationships=False)
                if not event_entity:
                    raise Exception(f"No entity could be retreived from Atlas with guid {entity_guid}")

                logging.warning(repr(kafka_notification))
                logging.warning(repr(event_entity))
                kafka_notification_json = json.loads(kafka_notification.to_json())
                entity_json = json.loads(event_entity.to_json())

                logging.warning(json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : entity_json}))
                return json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : entity_json})

            if kafka_notification.message.operation_type == EntityAuditAction.ENTITY_DELETE:
                kafka_notification_json = json.loads(kafka_notification.to_json())
                logging.warning(json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : {}}))
                return json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : {}})


        # END func
        try:
            return asyncio.run(get_entity(kafka_notification))

        except ClientResponseError as e:

            bootstrap_server_hostname, bootstrap_server_port =  store.get_many("kafka.bootstrap.server.hostname", "kafka.bootstrap.server.port")

            exc_info = sys.exc_info()
            e = (''.join(traceback.format_exception(*exc_info)))

            event = DeadLetterBoxMesage(timestamp=time.time(), original_notification=kafka_notification.to_json(), job="get_entity", description = (e))
            logging.warning("this goes into dead letter box: ")
            logging.warning(repr(event))

            producer = KafkaProducer(
                bootstrap_servers=  f"{bootstrap_server_hostname}:{bootstrap_server_port}",
                value_serializer=str.encode,
                request_timeout_ms = 1000,
                api_version = (2,0,2),
                retries = 1,
                linger_ms = 1000
            )

            dead_lettter_box_topic = store.get("exception.events.topic.name")

            producer.send(topic=dead_lettter_box_topic, value=event.to_json())



def run_get_entity_job():

    env = StreamExecutionEnvironment.get_execution_environment()
    #set_env(env)
    env.set_parallelism(1)

    path = os.path.dirname(__file__)

    # download JARs
    kafka_jar = f"file:///" + path + "/../flink_jars/flink-connector-kafka-1.15.1.jar"
    kafka_client = f"file:///" + path + "/../flink_jars/kafka-clients-2.2.1.jar"

    bootstrap_server_hostname = config.get("kafka.bootstrap.server.hostname")
    bootstrap_server_port = config.get("kafka.bootstrap.server.port")
    source_topic_name = config.get("atlas.audit.events.topic.name")
    sink_topic_name = config.get("enriched.events.topic.name")
    kafka_consumer_group_id = config.get("kafka.consumer.group.id")

    env.add_jars(kafka_jar, kafka_client)

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


    data_stream = env.add_source(kafka_source).name(f"consuming atlas events")

    data_stream = data_stream.map(GetEntity(), Types.STRING()).name("retrieve entity from atlas").filter(lambda notif: notif)

    data_stream.print()

    data_stream.add_sink(FlinkKafkaProducer(topic=sink_topic_name,
        producer_config={"bootstrap.servers": f"{bootstrap_server_hostname}:{bootstrap_server_port}","max.request.size": "14999999", 'group.id': kafka_consumer_group_id},
        serialization_schema=SimpleStringSchema())).name("write_to_kafka")


    env.execute("get_atlas_entity")

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")
    run_get_entity_job()
