import json
import logging
import jsonpickle
import uuid
import datetime

from m4i_flink_tasks import EntityMessage
from m4i_flink_tasks.operation import UpdateLocalAttributeProcessor, OperationEvent
from m4i_flink_tasks.operation import OperationChange, Sequence,CreateLocalEntityProcessor,DeleteLocalAttributeProcessor
from elastic_enterprise_search import EnterpriseSearch, AppSearch

class SynchronizeAppsearchLocal(object):
    app_search = None
    elastic_base_endpoint = None
    elastic_user = None
    elastic_passwd = None
    
    
    def open_local(self, config, credentials, config_store):
        config_store.load({**config, **credentials})
        (
            self.elastic_base_endpoint,
            self.elastic_user,
            self.elastic_passwd
        ) = config_store.get_many(
            "elastic.enterprise.search.endpoint",
            "elastic.user",
            "elastic.passwd"
        )
        self.app_search = self.get_app_search()


    def get_app_search(self):
        if self.app_search == None:
            self.app_search = AppSearch(
                hosts=self.elastic_base_endpoint,
                basic_auth=(self.elastic_user, self.elastic_passwd)
                )
        return self.app_search


    def map_local(self, kafka_notification: str):
        result = None

        change_list=[]        
        logging.warning(kafka_notification)
        entity_message = EntityMessage.from_json((kafka_notification))

        input_entity = entity_message.new_value
        
        # Charif: This if-statement does not match our new approach..
        if entity_message.direct_change == False:
            logging.warning("This message is a consequence of an indirect change. No further action is taken.")
            return
            #pass
        local_operation_list = []
        if entity_message["originalEventType"]=="ENTITY_CREATE":
            local_operation_list.append(CreateLocalEntityProcessor(name=f"create entity with guid {input_entity.guid} of type {input_entity.type_name}", 
                                                                      entity_guid = input_entity.guid,
                                                                      entity_type = input_entity.type_name))
        
        if entity_message["originalEventType"] in ["ENTITY_UPDATE","ENTITY_CREATE"]:
            if entity_message.inserted_attributes != []:
                logging.info("handle inserted attributes.")
                for insert_attribute in entity_message.inserted_attributes:
                     if insert_attribute in input_entity.attributes.unmapped_attributes.keys():

                        value = input_entity.attributes.unmapped_attributes[insert_attribute]
                        local_operation_list.append(UpdateLocalAttributeProcessor(name=f"insert attribute {insert_attribute}", key=insert_attribute, value=value))
            
            if entity_message.changed_attributes != []:
                logging.info("handle updated attributes.")
                for update_attribute in entity_message.changed_attributes:
                    if update_attribute in input_entity.attributes.keys():
    
                        value = input_entity.attributes.unmapped_attributes[update_attribute]
                        local_operation_list.append(UpdateLocalAttributeProcessor(name=f"update attribute {update_attribute}", key=update_attribute, value=value))

            if entity_message.deleted_attributes != []:
                logging.info("handle deleted attributes.")
                for delete_attribute in entity_message.deleted_attributes:
                    local_operation_list.append(DeleteLocalAttributeProcessor(name=f"delete attribute {delete_attribute}", key=delete_attribute, value=value))

            if len(local_operation_list)>0:
                seq = Sequence(name="update and inser attributes", steps = local_operation_list)
                spec = jsonpickle.encode(seq) 

                oc = OperationChange(propagate=False, propagate_down=False, operation = json.loads(spec))
                logging.warning("Operation Change has been created")
                change_list.append(oc)
        if len(change_list)>0:
            oe = OperationEvent(id=str(uuid.uuid4()), 
                                creation_time=int(datetime.datetime.now().timestamp()*1000),
                                entity_guid=input_entity.guid,
                                changes=change_list)
            logging.warning("Operation event has been created")
            
            result = json.dumps(json.loads(oe.to_json()))

        return result
            
# end of class SynchronizeAppsearchLocal