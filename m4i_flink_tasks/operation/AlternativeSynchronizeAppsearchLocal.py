import json
import logging
#from aenum import unique
import jsonpickle
import uuid
import datetime
from m4i_flink_tasks.parameters import *
from m4i_flink_tasks import EntityMessage
from m4i_flink_tasks.operation.core_operation import InsertPrefixToList,DeletePrefixFromList,DeleteLocalAttributeProcessor, UpdateListEntryBasedOnUniqueValueList, DeleteListEntryBasedOnUniqueValueList, AbstractProcessor
from m4i_flink_tasks.operation.core_operation import CreateLocalEntityProcessor, DeleteEntityOperator,UpdateLocalAttributeProcessor, AddElementToListProcessor
from m4i_flink_tasks.operation.OperationEvent import OperationEvent, OperationChange
from m4i_flink_tasks.operation.core_operation import Sequence,CreateLocalEntityProcessor,DeleteLocalAttributeProcessor
from m4i_atlas_core import EntityAuditAction
from elastic_enterprise_search import EnterpriseSearch, AppSearch
from m4i_flink_tasks.synchronize_app_search.elastic import delete_document
from scripts.init.app_search_engine_setup import engines
from typing import Optional 
from m4i_flink_tasks.synchronize_app_search import get_m4i_source_types, get_super_types_names, get_relevant_hierarchy_entity_fields, is_parent_child_relationship, get_parent_child_entity_guid, get_document


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
    
    def insert_person_relationship(self, input_entity, inserted_relationship, local_operations_dict:dict):
        logging.info("start insert_person_relationship")
        local_operation_person = []
        # check whether the inserted relationship is a Person related relationship
        person_guid = input_entity.guid
        person_name = input_entity.attributes.unmapped_attributes["name"]
        operation_event_guid = inserted_relationship["guid"]
        if inserted_relationship["typeName"]=="m4i_person":
            person_guid = inserted_relationship['guid']
            person_name = inserted_relationship['displayText']
            operation_event_guid = input_entity.guid

        operation = [AddElementToListProcessor(name="update attribute derivedperson",
                                                        key="derivedperson", value=person_name),
                     AddElementToListProcessor(name="update attribute derivedpersonguid", 
                                                        key="derivedpersonguid", value=person_guid)]

                                                                
  
        if operation_event_guid not in local_operations_dict.keys():
            local_operations_dict[operation_event_guid] = operation
        else:
            local_operations_dict[operation_event_guid].extend(operation)

        logging.info("end insert_person_relationship")
        return local_operation_person


    def delete_person_relationship(self,input_entity, deleted_relationship, local_operations_dict):
        logging.info("start deleted_person_relationship")
        local_operation_person = []
        # check whether the inserted relationship is a Person related relationship
        operation_event_guid = deleted_relationship["guid"]
        person_guid = input_entity.guid
        if deleted_relationship["typeName"]=="m4i_person":
            operation_event_guid = input_entity.guid
            person_guid = deleted_relationship["guid"]
        # create local operations

        operation = [DeleteListEntryBasedOnUniqueValueList(name="delete attribute derivedperson", 
                                                           unique_list_key = derived_person_guid, 
                                                           target_list_key=derived_person, 
                                                           unique_value=person_guid),
                     DeleteListEntryBasedOnUniqueValueList(name="delete attribute derivedpersonguid", 
                                                           unique_list_key = derived_person_guid, 
                                                           target_list_key=derived_person_guid, 
                                                           unique_value=person_guid)
        ]

     
        if operation_event_guid not in local_operations_dict.keys():
            local_operations_dict[operation_event_guid] = operation
        else:
            local_operations_dict[operation_event_guid].extend(operation)
        logging.info("end deleted_person_relationship")
        return local_operations_dict


    async def handle_deleted_hierarchical_relationships(self, entity_message: EntityMessage,  local_operations_dict : dict, propagated_operation_downwards_operations_dict : dict, propagated_operation_upwards_operations_dict: dict):
        """This function defines all operators required to handle the deleted relationships."""
        input_entity = entity_message.new_value
        if entity_message.deleted_relationships != {}:
            logging.warning("handle deleted relationships.")
            for key, deleted_relationships_ in entity_message.deleted_relationships.items():
                # key,deleted_relationships_ = (list(entity_message.deleted_relationships.items()))[0]
                if deleted_relationships_ == [] or (not deleted_relationships_):
                    continue

                # iterate over all deleted relationships
                for deleted_relationship in deleted_relationships_:
                    # deleted_relationship = deleted_relationships_[0]
                    super_types = await get_super_types_names(input_entity.type_name)
                    logging.info(f"super_types {super_types}")
                    m4isourcetype = get_m4i_source_types(super_types)
                    logging.info(f"m4isourcetype {m4isourcetype}")
                    if len(m4isourcetype) > 0:
                        m4isourcetype = m4isourcetype[0]
                    derived_guid, derived_type = get_relevant_hierarchy_entity_fields(m4isourcetype)
                    logging.info(f"derived_guid {derived_guid}")
                    logging.info(f"derived_type {derived_type}")
                
                    # check whether the relationship is a hierarchical relationship
                    if await is_parent_child_relationship(m4isourcetype, key, deleted_relationship):
                        local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict = await self.handle_deleted_hierarchical_relationship(entity_message, key, deleted_relationship, derived_guid, local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict)
             

                    elif deleted_relationship["typeName"]=="m4i_person" or input_entity.type_name=="m4i_person":
                        local_operations_dict = self.delete_person_relationship(input_entity, deleted_relationship, local_operations_dict)

        return local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict   

    
    async def handle_deleted_hierarchical_relationship(self, entity_message: EntityMessage, key :str, deleted_relationship : list, derived_guid: str, local_operations_dict : dict, propagated_operation_downwards_operations_dict : dict, propagated_operation_upwards_operations_dict: dict):
        """This function defines all operators required to handle the deleted relationship."""

        local_operation_list = []
        operation = []
        propagated_operation_downwards_list = []

        input_entity = entity_message.new_value

        input_entity_guid = entity_message.guid

        logging.warning("handle deleted relationships.")

        parent_entity_guid, child_entity_guid = await get_parent_child_entity_guid(input_entity.guid, input_entity.type_name, key, deleted_relationship)
        operation_event_guid = child_entity_guid # validate whether this goes right in all cases.

        # breadcrumb updates -> relevant for child entity 
        propagated_operation_downwards_list.append(DeletePrefixFromList(name="update breadcrumb name", key="breadcrumbname", guid_key="breadcrumbguid" , first_guid_to_keep=child_entity_guid))
        propagated_operation_downwards_list.append(DeletePrefixFromList(name="update breadcrumb type", key="breadcrumbtype", guid_key="breadcrumbguid" , first_guid_to_keep=child_entity_guid))
        propagated_operation_downwards_list.append(DeletePrefixFromList(name="update breadcrumb guid", key="breadcrumbguid", guid_key="breadcrumbguid" , first_guid_to_keep=child_entity_guid))
        
        # delete parent guid -> relevant for child 
        local_operation_list.append(DeleteLocalAttributeProcessor(name=f"delete attribute {parent_guid}", key=parent_guid))
        
        # delete derived entity guid -> relevant for child
        if derived_guid in conceptual_hierarchical_derived_entity_guid_fields_list:
            index = conceptual_hierarchical_derived_entity_guid_fields_list.index(derived_guid)
            to_be_deleted_derived_guid_fields = conceptual_hierarchical_derived_entity_guid_fields_list[:index+1]   


        if derived_guid in technical_hierarchical_derived_entity_guid_fields_list:
            index = technical_hierarchical_derived_entity_guid_fields_list.index(derived_guid)
            to_be_deleted_derived_guid_fields = technical_hierarchical_derived_entity_guid_fields_list[:index+1]   

        for to_be_deleted_derived_guid_field in to_be_deleted_derived_guid_fields:

            propagated_operation_downwards_list.append(DeleteLocalAttributeProcessor(name=f"delete derived entity field: {to_be_deleted_derived_guid_field}", key = to_be_deleted_derived_guid_field))
            propagated_operation_downwards_list.append(DeleteLocalAttributeProcessor(name=f"delete derived entity field: {hierarchical_derived_entity_fields_mapping[to_be_deleted_derived_guid_field]}", key = hierarchical_derived_entity_fields_mapping[to_be_deleted_derived_guid_field]))

        
        
        
        if operation_event_guid not in local_operations_dict.keys():
            local_operations_dict[operation_event_guid] = local_operation_list
        else:
            local_operations_dict[operation_event_guid].extend(local_operation_list)


        if operation_event_guid not in propagated_operation_downwards_operations_dict.keys():
            propagated_operation_downwards_operations_dict[operation_event_guid] = propagated_operation_downwards_list
        else:
            propagated_operation_downwards_operations_dict[operation_event_guid].extend(propagated_operation_downwards_list)


        return local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict
    
    async def handle_inserted_hierarchical_relationships(self, entity_message: EntityMessage,  local_operations_dict : dict, propagated_operation_downwards_operations_dict : dict, propagated_operation_upwards_operations_dict: dict):
        """This function defines all operators required to handle the inserted relationships."""
        input_entity = entity_message.new_value
        if entity_message.inserted_relationships != {}:
            logging.warning("handle inserted relationships.")
            for key, inserted_relationships_ in entity_message.inserted_relationships.items():
                # key,inserted_relationships_ = (list(entity_message.inserted_relationships.items()))[0]
                if inserted_relationships_ == [] or (not inserted_relationships_):
                # if inserted_relationships_ == [] or :
                    continue

                for inserted_relationship in inserted_relationships_ :
                    if await is_parent_child_relationship(input_entity.type_name, key, inserted_relationship):
                        local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict = await self.handle_inserted_hierarchical_relationship(entity_message, key, inserted_relationship, local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict)
                    elif inserted_relationship["typeName"]=="m4i_person" or input_entity.type_name=="m4i_person":
                        local_operations_dict = self.insert_person_relationship(input_entity, inserted_relationship, local_operations_dict)

        return local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict   



    async def handle_inserted_hierarchical_relationship(self, entity_message: EntityMessage, key :str, inserted_relationship, local_operations_dict : dict, propagated_operation_downwards_operations_dict : dict, propagated_operation_upwards_operations_dict: dict):
        """This function defines all operators required to handle the inserted hierarchical relationship."""

        local_operation_list = []
        propagated_operation_downwards_list = []

        input_entity = entity_message.new_value
        
        parent_entity_guid, child_entity_guid = await get_parent_child_entity_guid(input_entity.guid, input_entity.type_name, key, inserted_relationship)
        operation_event_guid = child_entity_guid # validate whether this goes right in all cases.

        parent_entity_document = get_document(parent_entity_guid, self.app_search)

        if not parent_entity_document:
            logging.warning(f"no parent entity found corresponding to guid {parent_entity_guid}")
            raise Exception(f"no parent entity found corresponding to guid {parent_entity_guid}. This entity should be created, but is not created.")

        if input_entity.guid == parent_entity_guid:
            super_types = await get_super_types_names(input_entity.type_name)

        else:

            parent_entity_type = inserted_relationship["typeName"]
            super_types = await get_super_types_names(parent_entity_type)
            

        m4isourcetype = get_m4i_source_types(super_types)

        if len(m4isourcetype) > 0:
            m4isourcetype = m4isourcetype[0]
        derived_guid, derived_type = get_relevant_hierarchy_entity_fields(m4isourcetype)

        # derived entity fields -> relevant for child entity

        if derived_guid in conceptual_hierarchical_derived_entity_guid_fields_list:
            index = conceptual_hierarchical_derived_entity_guid_fields_list.index(derived_guid)
            to_be_inserted_derived_guid_fields = conceptual_hierarchical_derived_entity_guid_fields_list[:index] 


        if derived_guid in technical_hierarchical_derived_entity_guid_fields_list:
            index = technical_hierarchical_derived_entity_guid_fields_list.index(derived_guid)
            to_be_inserted_derived_guid_fields = technical_hierarchical_derived_entity_guid_fields_list[:index]   

        for to_be_inserted_derived_guid_field in to_be_inserted_derived_guid_fields:

            propagated_operation_downwards_list.append(UpdateLocalAttributeProcessor(name=f"insert derived entity field {to_be_inserted_derived_guid_field}", key = to_be_inserted_derived_guid_field, value = parent_entity_document[to_be_inserted_derived_guid_field]))
            propagated_operation_downwards_list.append(UpdateLocalAttributeProcessor(name=f"insert derived entity field {hierarchical_derived_entity_fields_mapping[to_be_inserted_derived_guid_field]}", key = hierarchical_derived_entity_fields_mapping[to_be_inserted_derived_guid_field], value = parent_entity_document[hierarchical_derived_entity_fields_mapping[to_be_inserted_derived_guid_field]]))

        # define parent guid -> relevant for child 
        local_operation_list.append(UpdateLocalAttributeProcessor(name=f"insert attribute {parent_guid}", key=parent_guid, value=parent_entity_guid))

        local_operation_list.append(UpdateLocalAttributeProcessor(name=f"insert attribute {derived_guid}", key=derived_guid, value=parent_entity_document[derived_guid] + [parent_entity_guid]))
        
        local_operation_list.append(UpdateLocalAttributeProcessor(name=f"insert attribute {derived_type}", key=derived_type, value=parent_entity_document[derived_type] + [parent_entity_document[name]])) # Charif: Validate whether this will work for nested structures!!

        # breadcrumb updates -> relevant for child entity 
        breadcrumbguid_prefix = parent_entity_document["breadcrumbguid"] + [parent_entity_document["guid"]]
        breadcrumbname_prefix = parent_entity_document["breadcrumbname"] + [parent_entity_document["name"]]
        breadcrumbtype_prefix = parent_entity_document["breadcrumbtype"] + [parent_entity_document["typename"]]
        
        propagated_operation_downwards_list.append(InsertPrefixToList(name="update breadcrumb guid", key="breadcrumbguid", input_list=breadcrumbguid_prefix)) 
        propagated_operation_downwards_list.append(InsertPrefixToList(name="update breadcrumb name", key="breadcrumbname", input_list=breadcrumbname_prefix))
        propagated_operation_downwards_list.append(InsertPrefixToList(name="update breadcrumb type", key="breadcrumbtype", input_list=breadcrumbtype_prefix))



        if operation_event_guid not in local_operations_dict.keys():
            local_operations_dict[operation_event_guid] = local_operation_list
        else:
            local_operations_dict[operation_event_guid].extend(local_operation_list)


        if operation_event_guid not in propagated_operation_downwards_operations_dict.keys():
            propagated_operation_downwards_operations_dict[operation_event_guid] = propagated_operation_downwards_list
        else:
            propagated_operation_downwards_operations_dict[operation_event_guid].extend(propagated_operation_downwards_list)


        return local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict

        # end of handling an inserted hierarchical relationship

    def add_list_to_dict(self, input_dict: dict, input_guid: str, input):
        
        if isinstance(input, AbstractProcessor):
            input_list = [input]

        if input_guid not in input_dict.keys():

            input_dict[input_guid] = input_list
        else:
            input_dict[input_guid].extend(input_list)

        return input_dict

    async def map_local(self, kafka_notification: str):
        result = []

        change_list=[]   
        other_change_list = []
        propagated_change_list = []   

        logging.warning(kafka_notification)
        entity_message = EntityMessage.from_json((kafka_notification))

        input_entity = entity_message.new_value
        old_input_entity = entity_message.old_value
        
        if entity_message.direct_change == False:
            logging.info("This message is a consequence of an indirect change. No further action is taken.")
            return
            # pass
            
        operation_event_guid = entity_message.guid

        local_operation_list = []
        # other_operations = {} 
        propagated_operation_downwards_list = []
        propagated_operation_upwards_list = []

        create_local_operation_dict = {}
        delete_local_operation_dict = {}


        other_operations = {}

        propagated_operation_downwards_operations_dict = {}
        propagated_operation_upwards_operations_dict = {}
        local_operations_dict = {}

        if entity_message.original_event_type==EntityAuditAction.ENTITY_DELETE:
            logging.warning("start handling deleted relationships.")
            delete_local_operation_dict[input_entity.guid] = [(DeleteEntityOperator(name=f"delete entity with guid {operation_event_guid}"))]

            if entity_message.deleted_relationships != {}:
                local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict = await self.handle_deleted_hierarchical_relationships( entity_message,  local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict)
                
            logging.warning("deleted relationships handled.")
           #TODO handle propagation of removed relationships:
           # deleting an entity with an attribute and a domain related to it requires to 
           # propagate changes to related attributes, by removing parent and removing breadcrumb prefix
           # check also for delete of person, which requires a whole different set of lcoal changes
           # local_changes = self.delete_person_entity(entity)








        if entity_message.original_event_type==EntityAuditAction.ENTITY_CREATE:
            create_local_operation_dict[input_entity.guid] = [CreateLocalEntityProcessor(name=f"create entity with guid {input_entity.guid} of type {input_entity.type_name}", 
                                                                      entity_guid = input_entity.guid,
                                                                      entity_type = input_entity.type_name,
                                                                      entity_name = input_entity.attributes.unmapped_attributes['name'],
                                                                      entity_qualifiedname = entity_message.qualified_name)]
            # local_operations_dict = self.add_list_to_dict(local_operations_dict, input_entity.guid, create_operation)
        
        if entity_message.original_event_type in [EntityAuditAction.ENTITY_CREATE,EntityAuditAction.ENTITY_IMPORT_CREATE,
                                                  EntityAuditAction.ENTITY_UPDATE,EntityAuditAction.ENTITY_IMPORT_UPDATE ]:
            # handle inserted attributes
            if entity_message.inserted_attributes != []:
                logging.info("handle inserted attributes.")
                for insert_attribute in entity_message.inserted_attributes:
                     if ((insert_attribute in input_entity.attributes.unmapped_attributes.keys()) and
                         (insert_attribute.lower() in self.schema_names)):

                        value = input_entity.attributes.unmapped_attributes[insert_attribute]
                        # local_operation_list.append(UpdateLocalAttributeProcessor(name=f"insert attribute {insert_attribute}", key=insert_attribute.lower(), value=value))

                        local_operations_dict = self.add_list_to_dict(local_operations_dict, input_entity.guid, UpdateLocalAttributeProcessor(name=f"insert attribute {insert_attribute}", key=insert_attribute.lower(), value=value))

            # end of handle inserted attributes
            # handle changed attributes
            if entity_message.changed_attributes != []:
                logging.info("handle updated attributes.")
                for update_attribute in entity_message.changed_attributes:
                    if ((update_attribute in input_entity.attributes.unmapped_attributes.keys()) and
                         (update_attribute.lower() in self.schema_names)):
    
                        value = input_entity.attributes.unmapped_attributes[update_attribute]
                        old_value = old_input_entity.attributes.unmapped_attributes[update_attribute]

                        # local_operation_list.append(UpdateLocalAttributeProcessor(name=f"update attribute {update_attribute}", key=update_attribute.lower(), value=value))

                        local_operations_dict = self.add_list_to_dict(local_operations_dict, input_entity.guid, UpdateLocalAttributeProcessor(name=f"update attribute {update_attribute}", key=update_attribute.lower(), value=value))


                        if name == update_attribute:

                            propagated_operation_downwards_operations_dict = self.add_list_to_dict(propagated_operation_downwards_operations_dict, operation_event_guid, UpdateListEntryBasedOnUniqueValueList(name="update breadcrumb name", unique_list_key=breadcrumb_guid, target_list_key=breadcrumb_name, unique_value=input_entity.guid, target_value= value))

                            super_types = await get_super_types_names(input_entity.type_name)
                            m4isourcetype = get_m4i_source_types(super_types)
                            if len(m4isourcetype) > 0:
                                m4isourcetype = m4isourcetype[0]
                            derived_guid, derived_type = get_relevant_hierarchy_entity_fields(m4isourcetype)

                            propagated_operation_downwards_operations_dict = self.add_list_to_dict(propagated_operation_downwards_operations_dict, operation_event_guid, UpdateListEntryBasedOnUniqueValueList(name="update derived entity field", unique_list_key=derived_guid, target_list_key=derived_type, unique_value=input_entity.guid, target_value= value))

            # end of handle change attributes

            # handle deleted attributes
            if entity_message.deleted_attributes != []:
                logging.info("handle deleted attributes.")
                for delete_attribute in entity_message.deleted_attributes:
                    if delete_attribute.lower() in self.schema_names:
                        # local_operation_list.append(DeleteLocalAttributeProcessor(name=f"delete attribute {delete_attribute}", key=delete_attribute.lower(), value=value))
                        local_operations_dict = self.add_list_to_dict(local_operations_dict, operation_event_guid, DeleteLocalAttributeProcessor(name=f"delete attribute {delete_attribute}", key=delete_attribute.lower(), value=value))

            # end of handle deleted attributes
            
            # handle deleted_relationships
            if entity_message.deleted_relationships != {}:
                local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict = await self.handle_deleted_hierarchical_relationships( entity_message,  local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict)
                
                logging.warning("deleted relationships handled.")
            # end of handle delete relationships
           
            if entity_message.inserted_relationships != {}:
                local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict  = await self.handle_inserted_hierarchical_relationships(entity_message,  local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict)

                logging.warning("inserted relationships handled.")
                # end of handling inserted relationship
        # Charif: Using this indent might cause problems later: please reconsider

        entity_guid_list = list(create_local_operation_dict.keys()) + list(local_operations_dict.keys()) + list(delete_local_operation_dict.keys()) + list(other_operations.keys()) + list(propagated_operation_downwards_operations_dict.keys()) + list(propagated_operation_upwards_operations_dict.keys())
        entity_guid_list = list(set(entity_guid_list))
        
        for entity_guid in entity_guid_list:
            local_steps = create_local_operation_dict.get(entity_guid, []) + local_operations_dict.get(entity_guid, []) + delete_local_operation_dict.get(entity_guid, [])
            if len(local_steps)>0:
                seq = Sequence(name="local operaions", steps = local_steps)
                spec = jsonpickle.encode(seq) 
                oc = OperationChange(propagate=False, propagate_down=False, operation = json.loads(spec))
                logging.warning("Operation that executes local operations has been created")
                change_list.append(oc)

            propagated_steps = propagated_operation_downwards_operations_dict.get(entity_guid, []) 
            if len(propagated_steps)>0:
                seq = Sequence(name="create entity", steps = propagated_steps)
                spec = jsonpickle.encode(seq) 
                oc = OperationChange(propagate=True, propagate_down=True, operation = json.loads(spec))
                logging.warning("Operation that propagates relevant operations downwards has been created")
                change_list.append(oc)

            propagated_steps = propagated_operation_upwards_operations_dict.get(entity_guid, [])
            if len(propagated_steps)>0:
                seq = Sequence(name="create entity", steps = propagated_steps)
                spec = jsonpickle.encode(seq) 
                oc = OperationChange(propagate=True, propagate_down=False, operation = json.loads(spec))
                logging.warning("Operation that propagates relevant operations upwards has been created")
                change_list.append(oc)

            if len(change_list)>0:
                oe = OperationEvent(id=str(uuid.uuid4()), 
                                    creation_time=int(datetime.datetime.now().timestamp()*1000),
                                    entity_guid=entity_guid,
                                    changes=change_list)
                result.append(json.dumps(json.loads(oe.to_json())))
            logging.warning("Operation event has been created")

            
        return result
            
# end of class SynchronizeAppsearchLocal


