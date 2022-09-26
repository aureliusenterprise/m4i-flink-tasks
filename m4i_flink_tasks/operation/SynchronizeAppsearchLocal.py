import json
import logging
import jsonpickle
import uuid
import datetime
from .parameters import *
from m4i_flink_tasks import EntityMessage
from m4i_flink_tasks.operation.core_operation import UpdateLocalAttributeProcessor, UpdateListEntryProcessor, DeletePrefixFromList, InsertPrefixToList
from m4i_flink_tasks.operation.OperationEvent import OperationEvent, OperationChange
from m4i_flink_tasks.operation.core_operation import Sequence,CreateLocalEntityProcessor,DeleteLocalAttributeProcessor
from m4i_atlas_core import EntityAuditAction
from elastic_enterprise_search import EnterpriseSearch, AppSearch
from scripts.init.app_search_engine_setup import engines

from m4i_flink_tasks.synchronize_app_search import *

class SynchronizeAppsearchLocal(object):
    app_search = None
    elastic_base_endpoint = None
    elastic_user = None
    elastic_passwd = None
    schema_names = None
    engine_name = None




    
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
        self.schema_names = engines[0]['schema'].keys()
        self.engine_name = config_store.get("elastic.app.search.engine.name")
        # "operations.appsearch.engine.name" 



    def get_app_search(self):
        if self.app_search == None:
            self.app_search = AppSearch(
                hosts=self.elastic_base_endpoint,
                basic_auth=(self.elastic_user, self.elastic_passwd)
                )
        return self.app_search


    async def map_local(self, kafka_notification: str):
        result = None

        change_list=[]   
        propagated_change_list = []   # This is a list of ditionaries {id: change}  

        logging.warning(kafka_notification)
        entity_message = EntityMessage.from_json((kafka_notification))

        input_entity = entity_message.new_value
        old_input_entity = entity_message.old_value
        
        # Charif: This if-statement does not match our new approach..
        if entity_message.direct_change == False:
            logging.warning("This message is a consequence of an indirect change. No further action is taken.")
            # return
            pass
            
        local_operation_list = []
        propagated_operation_downwards_list = []
        propagated_operation_upwards_list = []

        if entity_message.original_event_type==EntityAuditAction.ENTITY_CREATE:
            local_operation_list.append(CreateLocalEntityProcessor(name=f"create entity with guid {input_entity.guid} of type {input_entity.type_name}", 
                                                                      entity_guid = input_entity.guid,
                                                                      entity_type = input_entity.type_name))
        
        if entity_message.original_event_type in [EntityAuditAction.ENTITY_CREATE,EntityAuditAction.ENTITY_IMPORT_CREATE,
                                                  EntityAuditAction.ENTITY_UPDATE,EntityAuditAction.ENTITY_IMPORT_UPDATE ]:
            if entity_message.inserted_attributes != []:
                logging.info("handle inserted attributes.")
                for insert_attribute in entity_message.inserted_attributes:
                     if ((insert_attribute in input_entity.attributes.unmapped_attributes.keys()) and
                         (insert_attribute.lower() in self.schema_names)):

                        value = input_entity.attributes.unmapped_attributes[insert_attribute]
                        local_operation_list.append(UpdateLocalAttributeProcessor(name=f"insert attribute {insert_attribute}", key=insert_attribute.lower(), value=value))
            
            if entity_message.changed_attributes != []:
                logging.info("handle updated attributes.")
                for update_attribute in entity_message.changed_attributes:
                    if ((update_attribute in input_entity.attributes.unmapped_attributes.keys()) and
                         (update_attribute.lower() in self.schema_names)):
    
                        value = input_entity.attributes.unmapped_attributes[update_attribute]
                        old_value = old_input_entity.attributes.unmapped_attributes[update_attribute]

                        local_operation_list.append(UpdateLocalAttributeProcessor(name=f"update attribute {update_attribute}", key=update_attribute.lower(), value=value))

                        if name == update_attribute:

                            propagated_operation_downwards_list.append(UpdateListEntryProcessor(name=f"update breadcrumb name {update_attribute}", key=breadcrumb_name, old_value=old_value, new_value=value))
                            super_types = await get_super_types_names(input_entity.type_name)
                            m4isourcetype = get_m4i_source_types(super_types)
                            derived_guid, derived_type = get_relevant_hierarchy_entity_fields(m4isourcetype)

                            propagated_operation_downwards_list.append(UpdateListEntryProcessor(name=f"update attribute {update_attribute}", key=derived_type, old_value=old_value, new_value=value))


            if entity_message.deleted_attributes != []:
                logging.info("handle deleted attributes.")
                for delete_attribute in entity_message.deleted_attributes:
                    if delete_attribute.lower() in self.schema_names:
                        local_operation_list.append(DeleteLocalAttributeProcessor(name=f"delete attribute {delete_attribute}", key=delete_attribute.lower(), value=value))

            
            if entity_message.deleted_relationships != {}:
                logging.warning("handle deleted relationships.")
                for key, deleted_relationships_ in entity_message.deleted_relationships.items():
                    if deleted_relationships_ == []:
                        continue

                    for deleted_relationship in deleted_relationships_:
                        super_types = await get_super_types_names(input_entity.type_name)
                        m4isourcetype = get_m4i_source_types(super_types)

                        if await is_parent_child_relationship(m4isourcetype, key, deleted_relationship):
                            parent_entity_guid, child_entity_guid = get_parent_child_entity_guid(input_entity.guid, input_entity.type_name, key, deleted_relationship)

                    
                            if input_entity.guid == child_entity_guid:

                                propagated_operation_downwards_list.append(DeletePrefixFromList(name=f"update breadcrumb guid", key="breadcrumbguid", index = -1,  incremental=True)) # Charif: use more descriptive names here
                                propagated_operation_downwards_list.append(DeletePrefixFromList(name=f"update breadcrumb name", key="breadcrumbname", index = -1,  incremental=True))
                                propagated_operation_downwards_list.append(DeletePrefixFromList(name=f"update breadcrumb type", key="breadcrumbtype", index = -1,  incremental=True))

                            

                logging.warning("deleted relationships handled.")

           
            if entity_message.inserted_relationships != {}:
                logging.warning("handle inserted relationships.")
                for key, inserted_relationships_ in entity_message.inserted_relationships.items():

                    if inserted_relationships_ == []:
                        continue

                    for inserted_relationship in inserted_relationships_:

                        if await is_parent_child_relationship(input_entity.type_name, key, inserted_relationship):
                            parent_entity_guid, child_entity_guid = await get_parent_child_entity_guid(input_entity.guid, input_entity.type_name, key, inserted_relationship)

                    
                            if input_entity.guid == child_entity_guid:
                                breadcrumbguid_prefix = get_prefix(parent_entity_guid, "breadcrumbguid", "guid", self.app_search)
                                breadcrumbname_prefix = get_prefix(parent_entity_guid, "breadcrumbname", "name",  self.app_search)
                                breadcrumbtype_prefix = get_prefix(parent_entity_guid, "breadcrumbtype", "typename",  self.app_search)


                                propagated_operation_downwards_list.append(InsertPrefixToList(name=f"update breadcrumb guid", key="breadcrumbguid", input_list=breadcrumbguid_prefix)) # Charif: use more descriptive names here
                                propagated_operation_downwards_list.append(InsertPrefixToList(name=f"update breadcrumb name", key="breadcrumbname", input_list=breadcrumbname_prefix))
                                propagated_operation_downwards_list.append(InsertPrefixToList(name=f"update breadcrumb type", key="breadcrumbtype", input_list=breadcrumbtype_prefix))




                logging.warning("inserted relationships handled.")

            if len(propagated_operation_downwards_list)>0:
                seq = Sequence(name="update and inser attributes", steps = propagated_operation_downwards_list)
                spec = jsonpickle.encode(seq) 

                oc = OperationChange(propagate=True, propagate_down=True, operation = json.loads(spec))
                logging.warning("Operation Change has been created")
                change_list.append(oc)


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


