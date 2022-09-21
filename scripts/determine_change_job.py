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
from pyflink.datastream.functions import FlatMapFunction

store = m4i_ConfigStore.get_instance()

inserted_attributes = []
changed_attributes = []
deleted_attributes = []

inserted_relationships = {}
changed_relationships = {}
deleted_relationships = {}


def drop_columns(df : pd.DataFrame, drop_params: str):
    """This function returns the input dataframe without the columns corresponding to the input parameters"""

    if drop_params == "attributes":
        return df.loc[:,~df.columns.str.startswith('attributes')]

    if drop_params == "relationsghipAttributes":
        return df.loc[:,~df.columns.str.startswith('attributes')]

    else:
        return df

def delete_list_values_from_dict(input_dict: dict):
    dict_keys = copy(list(input_dict.keys()))
    for key in dict_keys:
        if type(input_dict[key]) == list:
            del input_dict[key]
    return input_dict

def delete_null_values_from_dict(input_dict: dict):
    dict_keys = copy(list(input_dict.keys()))
    for key in dict_keys:
        if input_dict[key] == None:
            del input_dict[key]
    return input_dict

def get_attributes_df(atlas_entity: dict, column: str):
    atlas_entity = pd.DataFrame.from_dict(atlas_entity, orient = "index").transpose()
    attributes = pd.json_normalize(atlas_entity[column].tolist())
    return attributes


def get_flat_df(atlas_entity: dict) -> pd.DataFrame:
    """This function returns a flat dataframe corresponding to json datastructue of the Atlas entity."""
    atlas_entity = pd.DataFrame.from_dict(atlas_entity, orient = "index").transpose()

    attributes = pd.json_normalize(atlas_entity['attributes'].tolist()).add_prefix('attributes.')
    relationship_attributes = pd.json_normalize(atlas_entity['relationshipAttributes'].tolist()).add_prefix('relationshipAttributes.')

    atlas_entity = atlas_entity.drop(columns=["attributes","relationshipAttributes"])

    atlas_entity = pd.concat([atlas_entity, attributes, relationship_attributes], axis = 1)
    return atlas_entity

def is_direct_change(entity_guid: str) -> bool:
    """This function determines whether the kafka notification belong to a direct entity change or an indirect change."""
    access_token = get_keycloak_token()
    entity_audit =  asyncio.run(get_entity_audit(entity_guid = entity_guid, access_token = access_token))
    if entity_audit:
        atlas_entiy = Entity.from_json(re.search(r"{.*}", entity_audit.details).group(0))
        return atlas_entiy.relationship_attributes != None
    else:
        return True


def remove_prefix(input_string, prefix):
    if input_string.startswith(prefix):
        return input_string[len(prefix):]
    else:
        return input_string


def remove_prefix_from_attributes(attribute_set, prefix):
    result = []
    for attribute in attribute_set:
        result.append(remove_prefix(attribute, prefix))

    return result

def get_non_matching_fields(current_entity, previous_entity):
    comparison = current_entity.iloc[0].eq(previous_entity.iloc[0])
    changed_attributes = comparison[comparison==False].index.to_list()

    for changed_attribute in copy(changed_attributes):
        if type(current_entity[changed_attribute].iloc[0])==list and type(previous_entity[changed_attribute].iloc[0])==list:

            list_is_idential = True
            for element in (current_entity[changed_attribute].iloc[0]):
                if element not in ((previous_entity[changed_attribute].iloc[0])):
                    list_is_idential = False

            if list_is_idential:
                changed_attributes.remove(changed_attribute)

    return set(changed_attributes)

def get_added_relationships(current_entity, previous_entity):
    comparison = current_entity.iloc[0].eq(previous_entity.iloc[0])
    changed_attributes = comparison[comparison==False].index.to_list()

    result  = dict()
    for changed_attribute in copy(changed_attributes):
        element_list = []
        if type(current_entity[changed_attribute].iloc[0])==list and type(previous_entity[changed_attribute].iloc[0])==list:
            list_is_idential = True
            for element in (current_entity[changed_attribute].iloc[0]):
                if element not in (previous_entity[changed_attribute].iloc[0]):
                    list_is_idential = False
                    element_list.append(element)

            if list_is_idential:
                changed_attributes.remove(changed_attribute)
            else:
                result[changed_attribute] = element_list

    return (result)

def get_deleted_relationships(current_entity, previous_entity):
    comparison = current_entity.iloc[0].eq(previous_entity.iloc[0])
    changed_attributes = comparison[comparison==False].index.to_list()

    result  = dict()
    for changed_attribute in copy(changed_attributes):
        element_list = []
        if type(current_entity[changed_attribute].iloc[0])==list and type(previous_entity[changed_attribute].iloc[0])==list:
            list_is_idential = True
            for element in (previous_entity[changed_attribute].iloc[0]):
                if element not in (current_entity[changed_attribute].iloc[0]):
                    list_is_idential = False
                    element_list.append(element)

            if list_is_idential:
                changed_attributes.remove(changed_attribute)
            else:
                result[changed_attribute] = element_list

    return  (result)

def get_changed_fields(current_entity_df, previous_entity_df):
    result = []
    non_matching_fields = get_non_matching_fields(current_entity_df, previous_entity_df)
    for field in non_matching_fields:
        if (previous_entity_df[field].iloc[0] != [] or previous_entity_df[field].iloc[0] != None) and (current_entity_df[field].iloc[0] != [] or current_entity_df[field].iloc[0]  != None):
            result.append(field)
    return list(set(result))

def get_added_fields(current_entity_df, previous_entity_df):
    result = []
    non_matching_fields = get_non_matching_fields(current_entity_df, previous_entity_df)
    for field in non_matching_fields:
        if (previous_entity_df[field].iloc[0]  == [] or previous_entity_df[field].iloc[0]  == None) and (current_entity_df[field].iloc[0]  != [] or current_entity_df[field].iloc[0]  != None):
            result.append(field)
    return list(set(result))

def get_deleted_fields(current_entity_df, previous_entity_df):
    result = []
    non_matching_fields = get_non_matching_fields(current_entity_df, previous_entity_df)
    for field in non_matching_fields:
        if (previous_entity_df[field].iloc[0]  != [] or previous_entity_df[field].iloc[0]  != None) and (current_entity_df[field].iloc[0]  == [] or current_entity_df[field].iloc[0]  == None):
            result.append(field)
    return list(set(result))



class DetermineChangeLocal():
    elastic_search_index = None
    elastic = None

    def open_local(self):
        store.load({**config, **credentials})
        self.elastic_search_index = store.get("elastic.search.index")
        self.elastic = make_elastic_connection()

# elastic = make_elastic_connection()
    def get_previous_atlas_entity(self, atlas_entity_parsed, msg_creation_time):
        entity_guid = atlas_entity_parsed.guid
        
        query = {
            "bool": {
                "filter": [
                {
                    "match": {
                    "body.guid.keyword": entity_guid
                    }
                },
                {
                    "range": {
                    "msg_creation_time": {
                        "lt": msg_creation_time
                    }
                    }
                }
                ]
            }
        }

        sort = {
            "msg_creation_time": {"numeric_type" : "long", "order": "desc"}
        }

        retry = 0
        while retry<3:
            try:
                # there is still potential to improve by using search_templayes instead of index search
                result = self.elastic.search(index = self.elastic_search_index, query = query, sort = sort, size = 1)

                if result["hits"]["total"]["value"] >= 1:
                    return result["hits"]["hits"][0]["_source"]["body"]
                if result["hits"]["total"]["value"] == 0:
                    return None
            except Exception as e:
                logging.warning("failed to retrieve document")
                logging.warning(str(e))
                try:
                    self.elastic = make_elastic_connection()
                except:
                    pass
            retry = retry + 1

    def map_local(self, kafka_notification: str):
        logging.warning(repr(kafka_notification))

        kafka_notification_json = json.loads(kafka_notification)
        msg_creation_time = kafka_notification_json.get("msg_creation_time")
     
        	    # check whether notification or entity is missing
        if not kafka_notification_json.get("kafka_notification") or not kafka_notification_json.get("atlas_entity"):
            logging.warning("The Kafka notification received could not be handled due to unexpected notification structure.")
            guid = kafka_notification_json.get("guid","not available")
            raise Exception(f"event with GUID {guid} does not have a kafka notification and or an atlas entity attribute.")
     
        atlas_kafka_notification_json = kafka_notification_json["kafka_notification"]
        atlas_kafka_notification = AtlasChangeMessage.from_json(json.dumps(atlas_kafka_notification_json))
     
        atlas_entity_json = kafka_notification_json["atlas_entity"]
        atlas_entity_parsed = Entity.from_json(json.dumps(atlas_entity_json))
     
        	# DELETE operation
        if atlas_kafka_notification.message.operation_type == EntityAuditAction.ENTITY_DELETE:
            logging.warning("The Kafka notification received belongs to an entity delete audit.")
     
            atlas_entity_json["attributes"] = delete_list_values_from_dict(atlas_entity_json["attributes"])
            atlas_entity_json["attributes"] = delete_null_values_from_dict(atlas_entity_json["attributes"])
     
            atlas_entity_change_message = EntityMessage(
                                type_name = atlas_entity_parsed.type_name,
                                qualified_name = atlas_entity_parsed.attributes.unmapped_attributes["qualifiedName"],
                                guid = atlas_entity_parsed.guid,
                                msg_creation_time = msg_creation_time,
                                old_value = atlas_entity_parsed,
                                new_value = {},
                                original_event_type = atlas_kafka_notification.message.operation_type,
                                direct_change = is_direct_change(atlas_entity_parsed.guid),
                                event_type = "EntityDeleted",
                     
                                inserted_attributes = [],
                                changed_attributes = [],
                                deleted_attributes = list((atlas_entity_json["attributes"]).keys()),
                     
                                inserted_relationships = {},
                                changed_relationships = {},
                                deleted_relationships = (atlas_entity_json["relationshipAttributes"])
                     
                            )
            return [json.dumps(json.loads(atlas_entity_change_message.to_json()))]
     
        # CREATE operation
        if atlas_kafka_notification.message.operation_type == EntityAuditAction.ENTITY_CREATE:
            logging.warning("The Kafka notification received belongs to an entity create audit.")
            atlas_entity_json["attributes"] = delete_list_values_from_dict(atlas_entity_json["attributes"])
            atlas_entity_json["attributes"] = delete_null_values_from_dict(atlas_entity_json["attributes"])
     
            atlas_entity_change_message = EntityMessage(
                                type_name = atlas_entity_parsed.type_name,
                                qualified_name = atlas_entity_parsed.attributes.unmapped_attributes["qualifiedName"],
                                guid = atlas_entity_parsed.guid,
                                msg_creation_time = msg_creation_time,
                                old_value = {},
                                new_value = atlas_entity_parsed,
                                original_event_type = atlas_kafka_notification.message.operation_type,
                                direct_change = is_direct_change(atlas_entity_parsed.guid),
                                event_type = "EntityCreated",
                     
                                inserted_attributes = list((atlas_entity_json["attributes"]).keys()),
                                changed_attributes = [],
                                deleted_attributes = [],
                     
                                inserted_relationships = (atlas_entity_json["relationshipAttributes"]),
                                changed_relationships = {},
                                deleted_relationships = {}
                     
                            )
            return [json.dumps(json.loads(atlas_entity_change_message.to_json()))]
     
        # UPDATE operation
        if atlas_kafka_notification.message.operation_type == EntityAuditAction.ENTITY_UPDATE:
            logging.warning("The Kafka notification received belongs to an entity update audit.")
            previous_atlas_entity_json = self.get_previous_atlas_entity(atlas_entity_parsed, msg_creation_time)
            # this is not good.... need a way to handle individual states even if they have the same updatetime
            if previous_atlas_entity_json==None or not previous_atlas_entity_json:
                logging.warning("The Kafka notification received could not be handled due to missing corresponding entity document in the audit database in elastic search.")
                return
            logging.warning("Previous entity found.")
            previous_entity_parsed = Entity.from_json(json.dumps(previous_atlas_entity_json))
     
            previous_atlas_entity_json["attributes"] = delete_list_values_from_dict(previous_atlas_entity_json["attributes"])
            atlas_entity_json["attributes"] = delete_list_values_from_dict(atlas_entity_json["attributes"])
     
            previous_entity_attributes = get_attributes_df(previous_atlas_entity_json, "attributes")
            current_entity_attributes = get_attributes_df(atlas_entity_json, "attributes")
     
            previous_entity_relationships = get_attributes_df(previous_atlas_entity_json, "relationshipAttributes")
            current_entity_relationships = get_attributes_df(atlas_entity_json, "relationshipAttributes")
     
            inserted_attributes = get_added_fields(current_entity_attributes, previous_entity_attributes)
            changed_attributes = get_changed_fields(current_entity_attributes, previous_entity_attributes)
            deleted_attributes = get_deleted_fields(current_entity_attributes, previous_entity_attributes)
     
     
            inserted_relationships = get_added_relationships(current_entity_relationships, previous_entity_relationships)
            changed_relationships = {}
            deleted_relationships = get_deleted_relationships(current_entity_relationships, previous_entity_relationships)
     
            logging.warning("Determine audit category.")
     
            if sum([len(inserted_attributes), len(changed_attributes), len(deleted_attributes), len(inserted_relationships), len(changed_relationships), len(deleted_relationships)])==0:
                logging.warning("No audit could be determined.")
                return
     
            result = []
     
            if sum([len(inserted_attributes), len(changed_attributes), len(deleted_attributes)])>0:
                event_type = "EntityAttributeAudit"
     
                atlas_entity_change_message = EntityMessage(
                                            type_name = atlas_entity_parsed.type_name,
                                            qualified_name = atlas_entity_parsed.attributes.unmapped_attributes["qualifiedName"],
                                            guid = atlas_entity_parsed.guid,
                                            msg_creation_time = msg_creation_time,
                                            old_value = previous_entity_parsed,
                                            new_value = atlas_entity_parsed,
                                            original_event_type = atlas_kafka_notification.message.operation_type,
                                            direct_change = is_direct_change(atlas_entity_parsed.guid),
                                            event_type = event_type,
                                 
                                            inserted_attributes = inserted_attributes,
                                            changed_attributes = changed_attributes,
                                            deleted_attributes = deleted_attributes,
                                 
                                            inserted_relationships = {},
                                            changed_relationships = {},
                                            deleted_relationships = {}
                                 
                                            )
     
                result.append(json.dumps(json.loads(atlas_entity_change_message.to_json())))
     
     
            if sum([len(inserted_relationships), len(changed_relationships), len(deleted_relationships)])>0:
                event_type = "EntityRelationshipAudit"
     
                atlas_entity_change_message = EntityMessage(
                type_name = atlas_entity_parsed.type_name,
                qualified_name = atlas_entity_parsed.attributes.unmapped_attributes["qualifiedName"],
                guid = atlas_entity_parsed.guid,
                msg_creation_time = msg_creation_time,
                old_value = previous_entity_parsed,
                new_value = atlas_entity_parsed,
                original_event_type = atlas_kafka_notification.message.operation_type,
                direct_change = is_direct_change(atlas_entity_parsed.guid),
                event_type = event_type,
     
                inserted_attributes = [],
                changed_attributes = [],
                deleted_attributes = [],
     
                inserted_relationships = inserted_relationships,
                changed_relationships = changed_relationships,
                deleted_relationships = deleted_relationships
     
                )
     
                result.append(json.dumps(json.loads(atlas_entity_change_message.to_json())))
     
     
            logging.warning("audit catergory determined.")
     
            return result
     
        logging.error(f"unknown event type: {atlas_kafka_notification.message.operation_type}")
        return
# end of class DetermineChangeLocal


class DetermineChange(MapFunction,DetermineChangeLocal):
    bootstrap_server_hostname=None
    bootstrap_server_port=None

    def open(self, runtime_context: RuntimeContext):
        store.load({**config, **credentials})
        self.bootstrap_server_hostname, self.bootstrap_server_port =  store.get_many("kafka.bootstrap.server.hostname", "kafka.bootstrap.server.port")
        self.dead_lettter_box_topic = store.get("exception.events.topic.name")

        self.open_local()

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

    # def map_local(self, kafka_notification: str):
    #     kafka_notification_json = json.loads(kafka_notification)
    #     return kafka_notification_json

    def map(self, kafka_notification: str):
        try:
            res = self.map_local(kafka_notification)
            logging.info("received result: "+repr(res))
            return res
        except Exception as e:
            logging.error("Exception during processing:")
            logging.error(repr(e))

            exc_info = sys.exc_info()
            e = (''.join(traceback.format_exception(*exc_info)))

            event = DeadLetterBoxMesage(timestamp=time.time(), original_notification=kafka_notification, job="determine_change", description = (e))
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
# end of class DetermineChange



class GetResult(FlatMapFunction):

    def flat_map(self, input_list):
        for element in input_list:
            yield element



def determine_change():


    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    path = os.path.dirname(__file__)

    # download JARs
    kafka_jar = "file:///" + path + "/../flink_jars/flink-connector-kafka-1.15.1.jar"
    kafka_client = "file:///" + path + "/../flink_jars/kafka-clients-2.2.1.jar"


    env.add_jars(kafka_jar, kafka_client)

    bootstrap_server_hostname = config.get("kafka.bootstrap.server.hostname")
    bootstrap_server_port = config.get("kafka.bootstrap.server.port")
    source_topic_name = config.get("enriched.events.topic.name")
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

    data_stream = env.add_source(kafka_source).name(f"consuming enriched atlas events")

    data_stream = data_stream.map(DetermineChange(), Types.LIST(element_type_info = Types.STRING())).name("determine change").filter(lambda notif: notif)

    data_stream = data_stream.flat_map(GetResult(), Types.STRING()).name("parse change")

    data_stream.print()

    data_stream.add_sink(FlinkKafkaProducer(topic = sink_topic_name,
        producer_config={"bootstrap.servers": f"{bootstrap_server_hostname}:{bootstrap_server_port}","max.request.size": "14999999", 'group.id': kafka_consumer_group_id},
        serialization_schema=SimpleStringSchema())).name("write_to_kafka_sink")

    env.execute("determine_change")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO, format="%(message)s")



    determine_change()
