import json
import logging
import asyncio
import re
import pandas as pd
from copy import copy
from m4i_flink_tasks.synchronize_app_search import make_elastic_connection
from m4i_flink_tasks import EntityMessage
from m4i_atlas_core import Entity, AtlasChangeMessage, EntityAuditAction
from m4i_atlas_core import get_keycloak_token
from m4i_atlas_core import get_entity_audit


class DetermineChangeLocal():
    elastic_search_index = None
    elastic = None
    access_token = None

    def open_local(self, config, credentials, store):
        self.store = store
        self.store.load({**config, **credentials})
        self.elastic_search_index = store.get("elastic.search.index")
        self.elastic = make_elastic_connection()

    def get_access_token(self):
        if self.access_token==None:
            try:
                self.access_token = get_keycloak_token()
            except:
                pass
        return self.access_token

    def delete_list_values_from_dict(self,input_dict: dict):
        dict_keys = copy(list(input_dict.keys()))
        for key in dict_keys:
            if type(input_dict[key]) == list:
                del input_dict[key]
        return input_dict

    def delete_null_values_from_dict(self,input_dict: dict):
        dict_keys = copy(list(input_dict.keys()))
        for key in dict_keys:
            if input_dict[key] == None:
                del input_dict[key]
        return input_dict

    def get_attributes_df(self, atlas_entity: dict, column: str):
        atlas_entity = pd.DataFrame.from_dict(atlas_entity, orient = "index").transpose()
        attributes = pd.json_normalize(atlas_entity[column].tolist())
        return attributes

    def is_direct_change(self,entity_guid: str) -> bool:
        """This function determines whether the kafka notification belong to a direct entity change or an indirect change."""
        retry = 0
        while retry < 3:
            try:
                access__token = self.get_access_token()
                logging.info(f"access tokenL: {access__token}")
                entity_audit =  asyncio.run(get_entity_audit(entity_guid = entity_guid, access_token = access__token))
                if entity_audit:
                    atlas_entiy = Entity.from_json(re.search(r"{.*}", entity_audit.details).group(0))
                    logging.info(f"derived atlas_entity relationship attributes : {atlas_entiy.relationship_attributes}")
                    return atlas_entiy.relationship_attributes != None
                else:
                    return True
            except Exception as e:
                logging.error("failed to retrieve entity audit from atlas - retry")
                logging.error(str(e))
                self.access_token = None
                retry = retry+1
        raise Exception(f"Failed to lookup entity audit for entity guid {entity_guid}")

    def get_added_relationships(self, current_entity, previous_entity):
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
    
    def get_deleted_relationships(self,current_entity, previous_entity):
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

    def get_non_matching_fields(self, current_entity, previous_entity):
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
    
    def get_changed_fields(self,current_entity_df, previous_entity_df):
        result = []
        non_matching_fields = self.get_non_matching_fields(current_entity_df, previous_entity_df)
        for field in non_matching_fields:
            if (previous_entity_df[field].iloc[0] != [] or previous_entity_df[field].iloc[0] != None) and (current_entity_df[field].iloc[0] != [] or current_entity_df[field].iloc[0]  != None):
                result.append(field)
        return list(set(result))
    
    def get_added_fields(self,current_entity_df, previous_entity_df):
        result = []
        non_matching_fields = self.get_non_matching_fields(current_entity_df, previous_entity_df)
        for field in non_matching_fields:
            if (previous_entity_df[field].iloc[0]  == [] or previous_entity_df[field].iloc[0]  == None) and (current_entity_df[field].iloc[0]  != [] or current_entity_df[field].iloc[0]  != None):
                result.append(field)
        return list(set(result))
    
    def get_deleted_fields(self,current_entity_df, previous_entity_df):
        result = []
        non_matching_fields = self.get_non_matching_fields(current_entity_df, previous_entity_df)
        for field in non_matching_fields:
            if (previous_entity_df[field].iloc[0]  != [] or previous_entity_df[field].iloc[0]  != None) and (current_entity_df[field].iloc[0]  == [] or current_entity_df[field].iloc[0]  == None):
                result.append(field)
        return list(set(result))
        

     
    def get_previous_atlas_entity(self, entity_guid, msg_creation_time):
        
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
                    "msgCreationTime": {
                        "lt": msg_creation_time
                    }
                    }
                }
                ]
            }
        }

        sort = {
            "msgCreationTime": {"numeric_type" : "long", "order": "desc"}
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
        msg_creation_time = kafka_notification_json.get("kafka_notification").get("msgCreationTime")
     
        	    # check whether notification or entity is missing
        if not kafka_notification_json.get("kafka_notification") or (not kafka_notification_json.get("atlas_entity") and kafka_notification_json.get("atlas_entity")!={}):
            
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

            entity_guid = kafka_notification_json["kafka_notification"]["message"]["entity"]["guid"]

            atlas_entity_json = self.get_previous_atlas_entity(entity_guid, msg_creation_time)
     
            atlas_entity_json["attributes"] = self.delete_list_values_from_dict(atlas_entity_json["attributes"])
            atlas_entity_json["attributes"] = self.delete_null_values_from_dict(atlas_entity_json["attributes"])
     
            atlas_entity_change_message = EntityMessage(
                                type_name = atlas_entity_parsed.type_name,
                                qualified_name = atlas_entity_parsed.attributes.unmapped_attributes["qualifiedName"],
                                guid = atlas_entity_parsed.guid,
                                msg_creation_time = msg_creation_time,
                                old_value = atlas_entity_parsed,
                                new_value = {},
                                original_event_type = atlas_kafka_notification.message.operation_type,
                                direct_change = self.is_direct_change(atlas_entity_parsed.guid),
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
            atlas_entity_json["attributes"] = self.delete_list_values_from_dict(atlas_entity_json["attributes"])
            atlas_entity_json["attributes"] = self.delete_null_values_from_dict(atlas_entity_json["attributes"])
     
            atlas_entity_change_message = EntityMessage(
                                type_name = atlas_entity_parsed.type_name,
                                qualified_name = atlas_entity_parsed.attributes.unmapped_attributes["qualifiedName"],
                                guid = atlas_entity_parsed.guid,
                                msg_creation_time = msg_creation_time,
                                old_value = {},
                                new_value = atlas_entity_parsed,
                                original_event_type = atlas_kafka_notification.message.operation_type,
                                direct_change = self.is_direct_change(atlas_entity_parsed.guid),
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
            previous_atlas_entity_json = self.get_previous_atlas_entity(atlas_entity_parsed.guid, msg_creation_time)
            # this is not good.... need a way to handle individual states even if they have the same updatetime
            if previous_atlas_entity_json==None or not previous_atlas_entity_json:
                logging.warning("The Kafka notification received could not be handled due to missing corresponding entity document in the audit database in elastic search.")
                return
            logging.warning("Previous entity found.")
            previous_entity_parsed = Entity.from_json(json.dumps(previous_atlas_entity_json))
     
            previous_atlas_entity_json["attributes"] = self.delete_list_values_from_dict(previous_atlas_entity_json["attributes"])
            atlas_entity_json["attributes"] = self.delete_list_values_from_dict(atlas_entity_json["attributes"])
     
            previous_entity_attributes = self.get_attributes_df(previous_atlas_entity_json, "attributes")
            current_entity_attributes = self.get_attributes_df(atlas_entity_json, "attributes")
     
            previous_entity_relationships = self.get_attributes_df(previous_atlas_entity_json, "relationshipAttributes")
            current_entity_relationships = self.get_attributes_df(atlas_entity_json, "relationshipAttributes")
     
            inserted_attributes = self.get_added_fields(current_entity_attributes, previous_entity_attributes)
            changed_attributes = self.get_changed_fields(current_entity_attributes, previous_entity_attributes)
            deleted_attributes = self.get_deleted_fields(current_entity_attributes, previous_entity_attributes)
     
     
            inserted_relationships = self.get_added_relationships(current_entity_relationships, previous_entity_relationships)
            changed_relationships = {}
            deleted_relationships = self.get_deleted_relationships(current_entity_relationships, previous_entity_relationships)
     
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
                                            direct_change = self.is_direct_change(atlas_entity_parsed.guid),
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
                direct_change = self.is_direct_change(atlas_entity_parsed.guid),
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
