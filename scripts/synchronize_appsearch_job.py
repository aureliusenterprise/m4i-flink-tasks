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
from m4i_flink_tasks.operation.AlternativeSynchronizeAppsearchLocal import SynchronizeAppsearchLocal

import time
from kafka import KafkaProducer
import traceback
from elastic_enterprise_search import EnterpriseSearch, AppSearch
# from set_environment import set_env
import jsonpickle
import uuid
import datetime




class SynchronizeAppsearch(MapFunction,SynchronizeAppsearchLocal):
    bootstrap_server_hostname=None
    bootstrap_server_port=None
    dead_lettter_box_topic = None
    producer = None

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
        try:
            res = asyncio.run(self.map_local(kafka_notification))
            logging.info("received result: "+repr(res))
            return res
        except Exception as e:
            logging.error("Exception during processing:")
            logging.error(repr(e))

            exc_info = sys.exc_info()
            e = (''.join(traceback.format_exception(*exc_info)))

            event = DeadLetterBoxMesage(timestamp=time.time(), original_notification=kafka_notification, job="synchronize_appsearch", description = (e))
            logging.error("this goes into dead letter box: ")
            logging.error(repr(event))

            retry = 0
            while retry <2:
                try:
                    producer = self.get_deadletter()
                    producer.send(topic=self.dead_lettter_box_topic, value=event.to_json())
                    return
                except Exception as e2:
                    logging.error("error dumping data into deadletter topic "+repr(e2))
                retry = retry + 1
                    

    # def open(self, runtime_context: RuntimeContext):
    #     global app_search
    #     config_store.load({**config, **credentials})
    #     app_search = get_app_search()



    # def map(self, kafka_notification: str):
    #     try:    

    #         result = None

    #         operation_list = []
    #         logging.warning(kafka_notification)
    #         entity_message = EntityMessage.from_json((kafka_notification))

    #         input_entity = entity_message.new_value
            


    #         # Charif: This if-statement does not match our new approach..

    #         if entity_message.direct_change == False:
    #             logging.warning("This message is a consequence of an indirect change. No further action is taken.")
    #             # return
    #             pass

    #         if entity_message.changed_attributes != []:
    #             logging.info("handle updated attributes.")
    #             for update_attribute in entity_message.changed_attributes:
    #                 if update_attribute in input_entity.attributes.unmapped_attributes.keys():

    #                     value = input_entity.attributes.unmapped_attributes[update_attribute]
    #                     operation_list.append(UpdateLocalAttributeProcessor(name="update attribute", key=update_attribute, value=value))



    #                 if update_attribute == "name":
    #                     pass
    #             seq = Sequence(name="update attribute", steps = operation_list)
    #             spec = jsonpickle.encode(seq) 

    #             oc = OperationChange(propagate=False, propagate_down=False, operation = json.loads(spec))
    #             logging.warning("Operation Change has been created")

    #             oe = OperationEvent(id=str(uuid.uuid4()), 
    #                                 creation_time=int(datetime.datetime.now().timestamp()*1000),
    #                                 entity_guid=input_entity.guid,
    #                                 changes=[oc])
    #             logging.warning("Operation event has been created")
                
    #             result = json.dumps(json.loads(oe.to_json()))

    #         return result



       


    #     except Exception as e:

    #         logging.warning("The Kafka notification received could not be handled.")


    #         exc_info = sys.exc_info()
    #         e = (''.join(traceback.format_exception(*exc_info)))

    #         event = DeadLetterBoxMesage(timestamp=time.time(), original_notification=kafka_notification, job="determine_change", description = (e))
    #         bootstrap_server_hostname, bootstrap_server_port =  config_store.get_many("kafka.bootstrap.server.hostname", "kafka.bootstrap.server.port")
    #         producer = KafkaProducer(
    #             bootstrap_servers=  f"{bootstrap_server_hostname}:{bootstrap_server_port}",
    #             value_serializer=str.encode,
    #             request_timeout_ms = 1000,
    #             api_version = (2,0,2),
    #             retries = 1,
    #             linger_ms = 1000
    #         )
    #         dead_lettter_box_topic = config_store.get("exception.events.topic.name")
    #         producer.send(topic=dead_lettter_box_topic, value=event.to_json())

class GetResult(FlatMapFunction):

    def flat_map(self, input_list):
        for element in input_list:
            yield element



def synchronize_app_search():

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
                                                  'group.id': kafka_consumer_group_id,
                                                  "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
                                                  "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer"},
                                      deserialization_schema=SimpleStringSchema()).set_commit_offsets_on_checkpoints(True).set_start_from_latest()

    data_stream = env.add_source(kafka_source).name("consuming determined change events")

    data_stream = data_stream.map(SynchronizeAppsearch(), Types.LIST(element_type_info = Types.STRING())).name("synchronize appsearch").filter(lambda notif: notif)

    data_stream = data_stream.flat_map(GetResult(), Types.STRING()).name("parse change")

    data_stream.print()

    data_stream.add_sink(FlinkKafkaProducer(topic = sink_topic_name,
        producer_config={"bootstrap.servers": f"{bootstrap_server_hostname}:{bootstrap_server_port}","max.request.size": "14999999", 'group.id': kafka_consumer_group_id},
        serialization_schema=SimpleStringSchema())).name("write_to_kafka_sink")


    env.execute("synchronize app search")



if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")



    synchronize_app_search()

