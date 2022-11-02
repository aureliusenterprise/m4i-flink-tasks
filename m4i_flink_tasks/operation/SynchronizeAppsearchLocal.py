import datetime
import json
import logging
import uuid
from typing import Optional

import jsonpickle
from elastic_enterprise_search import AppSearch, EnterpriseSearch
from m4i_atlas_core import Entity, EntityAuditAction

from m4i_flink_tasks import EntityMessage
from m4i_flink_tasks.operation.core_operation import (
    AbstractProcessor, AddElementToListProcessor, CreateLocalEntityProcessor,
    Delete_Hierarchical_Relationship, DeleteEntityOperator,
    DeleteListEntryBasedOnUniqueValueList, DeleteLocalAttributeProcessor,
    DeletePrefixFromList, Insert_Hierarchical_Relationship, InsertPrefixToList,
    Sequence, UpdateListEntryBasedOnUniqueValueList,
    UpdateLocalAttributeProcessor)
from m4i_flink_tasks.operation.OperationEvent import (OperationChange,
                                                      OperationEvent)
from m4i_flink_tasks.parameters import *
from m4i_flink_tasks.synchronize_app_search import (
    get_attribute_field_guid, get_document, get_m4i_source_types,
    get_parent_child_entity_guid, get_relevant_hierarchy_entity_fields,
    get_super_types_names, is_attribute_field_relationship,
    is_parent_child_relationship)
from m4i_flink_tasks.synchronize_app_search.elastic import delete_document
from scripts.init.app_search_engine_setup import engines


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
        #TODO select the engine based on the name rather than the index. The index may change
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

        relationsip_type = inserted_relationship["relationshipType"]

        person_guid = input_entity.guid
        person_name = input_entity.attributes.unmapped_attributes["name"]
        operation_event_guid = inserted_relationship["guid"] # this is the conceptual entity that has a reference with the person.
        # operation_event_name = inserted_relationship["displayText"]


        if inserted_relationship["typeName"]=="m4i_person":
            person_guid = inserted_relationship['guid']
            person_name = inserted_relationship['displayText']
            operation_event_guid = input_entity.guid
            # operation_event_name = input_entity.attributes.unmapped_attributes['name']

        local_operation_person.extend([AddElementToListProcessor(name="update attribute derivedperson",
                                                        key="derivedperson", value=person_name),
                                       AddElementToListProcessor(name="update attribute derivedpersonguid",
                                                        key="derivedpersonguid", value=person_guid)])
        # if (relationsip_type == data_attribute_business_owner_assignment or relationsip_type == data_entity_business_owner_assignment):
        #     # business owner
        #     local_operation_person.extend([UpdateLocalAttributeProcessor(name="update attribute dervieddataownerguid",
        #                                                 key="deriveddataownerguid", value=person_guid)])
        # if (relationsip_type == data_attribute_steward_assignment or relationsip_type == data_entity_steward_assignment):
        #     # steward
        #     local_operation_person.extend([UpdateLocalAttributeProcessor(name="update attribute deriveddatastewardguid",
        #                                                 key="deriveddatastewardguid", value=person_guid)])
        # if (relationsip_type == domain_lead_assignment):
        #     # domainlead
        #     local_operation_person.extend([UpdateLocalAttributeProcessor(name="update attribute deriveddomainleadguid",
        #                                                 key="deriveddomainleadguid", value=person_guid)])
        if operation_event_guid not in local_operations_dict.keys():
            local_operations_dict[operation_event_guid] = local_operation_person
        else:
            local_operations_dict[operation_event_guid].extend(local_operation_person)

        logging.info("end insert_person_relationship")
        return local_operations_dict


    def delete_person_relationship(self,input_entity : Entity, deleted_relationship, local_operations_dict):
        logging.info("start deleted_person_relationship")
        local_operation_person = []
        # check whether the inserted relationship is a Person related relationship

        relationsip_type = deleted_relationship["relationshipType"]

        operation_event_guid = deleted_relationship["guid"]
        person_guid = input_entity.guid

        if deleted_relationship["typeName"]=="m4i_person":
            operation_event_guid = input_entity.guid
            person_guid = deleted_relationship["guid"]
        # create local operations

        local_operation_person = [DeleteListEntryBasedOnUniqueValueList(name="delete attribute derivedperson",
                                                           unique_list_key = derived_person_guid,
                                                           target_list_key=derived_person,
                                                           unique_value=person_guid),
                     DeleteListEntryBasedOnUniqueValueList(name="delete attribute derivedpersonguid",
                                                           unique_list_key = derived_person_guid,
                                                           target_list_key=derived_person_guid,
                                                           unique_value=person_guid)
        ]

        # if (relationsip_type == data_attribute_business_owner_assignment or relationsip_type == data_entity_business_owner_assignment):
        #     # business owner
        #     local_operation_person.extend([DeleteLocalAttributeProcessor(name="delete attribute dervieddataownerguid", key="deriveddataownerguid")])

        # if (relationsip_type == data_attribute_steward_assignment or relationsip_type == data_entity_steward_assignment):
        #     # steward
        #     local_operation_person.extend([DeleteLocalAttributeProcessor(name="delete attribute deriveddatastewardguid",key="deriveddatastewardguid")])

        # if (relationsip_type == domain_lead_assignment):
        #     # domainlead
        #     local_operation_person.extend([DeleteLocalAttributeProcessor(name="delete attribute deriveddomainleadguid",key="deriveddomainleadguid")])

        #     local_operation_person.extend([AddElementToListProcessor(name="update attribute deriveddomainleadguid",
        #                                                 key="deriveddomainleadguid", value=person_guid)])


        if operation_event_guid not in local_operations_dict.keys():
            local_operations_dict[operation_event_guid] = local_operation_person
        else:
            local_operations_dict[operation_event_guid].extend(local_operation_person)
        logging.info("end deleted_person_relationship")
        return local_operations_dict


    def handle_deleted_relationships(self, entity_message: EntityMessage,  local_operations_dict : dict, propagated_operation_downwards_operations_dict : dict, propagated_operation_upwards_operations_dict: dict):
        """This function defines all operators required to handle the deleted relationships."""
        input_entity = entity_message.new_value
        if input_entity == {}:
            input_entity = entity_message.old_value

        if entity_message.deleted_relationships != {}:
            logging.warning("handle deleted relationships.")
            for key, deleted_relationships_ in entity_message.deleted_relationships.items():
                # key,deleted_relationships_ = (list(entity_message.deleted_relationships.items()))[0]
                if deleted_relationships_ == [] or (not deleted_relationships_):
                    continue

                # iterate over all deleted relationships
                for deleted_relationship in deleted_relationships_:
                    # deleted_relationship = deleted_relationships_[0]
                    super_types = get_super_types_names(input_entity.type_name)
                    logging.info(f"super_types {super_types}")
                    m4isourcetype = get_m4i_source_types(super_types)
                    logging.info(f"m4isourcetype {m4isourcetype}")
                    #TODO what is this strange rule
                    if len(m4isourcetype) > 0:
                        m4isourcetype = m4isourcetype[0]
                    derived_guid, derived_type = get_relevant_hierarchy_entity_fields(m4isourcetype)
                    logging.info(f"derived_guid {derived_guid}")
                    logging.info(f"derived_type {derived_type}")

                    # check whether the relationship is a hierarchical relationship
                    if is_parent_child_relationship(m4isourcetype, key, deleted_relationship):
                        local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict = self.handle_deleted_hierarchical_relationship(entity_message, key, deleted_relationship, derived_guid, local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict)


                    elif deleted_relationship["typeName"]=="m4i_person" or input_entity.type_name=="m4i_person":
                        local_operations_dict = self.delete_person_relationship(input_entity, deleted_relationship, local_operations_dict)

                    elif is_attribute_field_relationship(input_entity.type_name, deleted_relationship):
                        local_operations_dict = self.delete_attribute_field_relationship(input_entity, deleted_relationship, local_operations_dict)

        return local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict


    def delete_attribute_field_relationship(self, input_entity: Entity, deleted_relationship: dict, local_operations_dict : dict):
        """This function determines all the operations necessary to delete a relationship from a data attribute to a field."""

        data_attribute_guid, field_guid = get_attribute_field_guid(input_entity, deleted_relationship)
        local_operations_data_attribute = [DeleteListEntryBasedOnUniqueValueList(name="delete data field",
                                            unique_list_key = derived_field_guid,
                                            target_list_key = derived_field,
                                            unique_value = field_guid),
                                        DeleteListEntryBasedOnUniqueValueList(name="delete data field guid",
                                            unique_list_key = derived_field_guid,
                                            target_list_key = derived_field_guid,
                                            unique_value = field_guid)]

        local_operations_field = [DeleteListEntryBasedOnUniqueValueList(name="delete data attribute",
                                            unique_list_key = derived_data_attribute_guid,
                                            target_list_key = derived_data_attribute,
                                            unique_value = data_attribute_guid),
                                 DeleteListEntryBasedOnUniqueValueList(name="delete data attribute guid",
                                            unique_list_key = derived_data_attribute_guid,
                                            target_list_key = derived_data_attribute_guid,
                                            unique_value = data_attribute_guid)]

        local_operations_dict = self.add_list_to_dict(local_operations_dict, data_attribute_guid, local_operations_data_attribute)
        local_operations_dict = self.add_list_to_dict(local_operations_dict, field_guid, local_operations_field)

        return local_operations_dict


    def handle_deleted_hierarchical_relationship(self, entity_message: EntityMessage, key :str, deleted_relationship : list, derived_guid: str, local_operations_dict : dict, propagated_operation_downwards_operations_dict : dict, propagated_operation_upwards_operations_dict: dict):
        """This function defines all operators required to handle the deleted relationship."""

        local_operation_list = []
        operation = []
        propagated_operation_downwards_list = []

        input_entity = entity_message.new_value

        input_entity_guid = entity_message.guid

        logging.warning("handle deleted relationships.")

        parent_entity_guid, child_entity_guid = get_parent_child_entity_guid(input_entity.guid, input_entity.type_name, key, deleted_relationship)

        # Charif: The following two lines are not as discussed: I put it here for testing purposes
        # operation_event_guid = child_entity_guid # validate whether this goes right in all cases.
        # propagated_operation_downwards_list.append(Delete_Hierarchical_Relationship(name="delete hierarchical relationship", parent_entity_guid=parent_entity_guid, child_entity_guid=child_entity_guid, current_entity_guid=child_entity_guid, derived_guid=derived_guid))


        operation_event_guid = input_entity_guid
        propagated_operation_downwards_list.append(Delete_Hierarchical_Relationship(name="delete hierarchical relationship", parent_entity_guid=parent_entity_guid, child_entity_guid=child_entity_guid, current_entity_guid=input_entity_guid, derived_guid=derived_guid))

        if operation_event_guid not in local_operations_dict.keys():
            local_operations_dict[operation_event_guid] = local_operation_list
        else:
            local_operations_dict[operation_event_guid].extend(local_operation_list)


        if operation_event_guid not in propagated_operation_downwards_operations_dict.keys():
            propagated_operation_downwards_operations_dict[operation_event_guid] = propagated_operation_downwards_list
        else:
            propagated_operation_downwards_operations_dict[operation_event_guid].extend(propagated_operation_downwards_list)


        return local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict


    def handle_inserted_relationships(self, entity_message: EntityMessage,  local_operations_dict : dict, propagated_operation_downwards_operations_dict : dict, propagated_operation_upwards_operations_dict: dict):
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
                    if is_parent_child_relationship(input_entity.type_name, key, inserted_relationship):
                        local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict = self.handle_inserted_hierarchical_relationship(entity_message, key, inserted_relationship, local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict)
                    elif inserted_relationship["typeName"]=="m4i_person" or input_entity.type_name=="m4i_person":
                        local_operations_dict = self.insert_person_relationship(input_entity, inserted_relationship, local_operations_dict)

                    elif is_attribute_field_relationship(input_entity.type_name, inserted_relationship):
                        local_operations_dict = self.insert_attribute_field_relationship(input_entity, inserted_relationship, local_operations_dict)

        return local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict


    def insert_attribute_field_relationship(self, input_entity: Entity, inserted_relationship: dict, local_operations_dict : dict):
        """This function determines all the operations necessary to delete a relationship from a data attribute to a field."""

        data_attribute_guid, field_guid = get_attribute_field_guid(input_entity, inserted_relationship)
        if data_attribute_guid == input_entity.guid:
            data_attribute_name = input_entity.attributes.unmapped_attributes[name]
            field_name = inserted_relationship["displayText"]

        else:
            data_attribute_name = inserted_relationship["displayText"]
            field_name = input_entity.attributes.unmapped_attributes[name]

        local_operations_data_attribute = [AddElementToListProcessor(name="insert data field guid",
                                            key = derived_field_guid,
                                            value = field_guid),
                                            AddElementToListProcessor(name="insert data field",
                                            key = derived_field,
                                            value = field_name)]

        local_operations_field = [AddElementToListProcessor(name="insert data attribute guid",
                                    key = derived_data_attribute_guid,
                                    value = data_attribute_guid),
                                    AddElementToListProcessor(name="insert data attribute",
                                    key = derived_data_attribute,
                                    value = data_attribute_name)]

        local_operations_dict = self.add_list_to_dict(local_operations_dict, data_attribute_guid, local_operations_data_attribute)
        local_operations_dict = self.add_list_to_dict(local_operations_dict, field_guid, local_operations_field)

        return local_operations_dict


    def handle_inserted_hierarchical_relationship(self, entity_message: EntityMessage, key :str, inserted_relationship, local_operations_dict : dict, propagated_operation_downwards_operations_dict : dict, propagated_operation_upwards_operations_dict: dict):
        """This function defines all operators required to handle the inserted hierarchical relationship."""

        local_operation_list = []
        propagated_operation_downwards_list = []

        input_entity = entity_message.new_value


        parent_entity_guid, child_entity_guid = get_parent_child_entity_guid(input_entity.guid, input_entity.type_name, key, inserted_relationship)
        # Charif: The following two lines are not as discussed: I put it here for testing purposes
        # operation_event_guid = child_entity_guid
        # propagated_operation_downwards_list.append(Insert_Hierarchical_Relationship(name="insert hierarchical relationship", parent_entity_guid=parent_entity_guid,child_entity_guid=child_entity_guid, current_entity_guid=child_entity_guid))


        operation_event_guid = input_entity.guid
        propagated_operation_downwards_list.append(Insert_Hierarchical_Relationship(name="insert hierarchical relationship", parent_entity_guid=parent_entity_guid,child_entity_guid=child_entity_guid, current_entity_guid=input_entity.guid))

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

        else:
            input_list = input

        if input_guid not in input_dict.keys():

            input_dict[input_guid] = input_list
        else:
            input_dict[input_guid].extend(input_list)

        return input_dict


    def map_local(self, kafka_notification: str):
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
            delete_local_operation_dict[entity_message.guid] = [(DeleteEntityOperator(name=f"delete entity with guid {entity_message.guid}"))]

            if entity_message.deleted_relationships != {}:
                local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict = self.handle_deleted_relationships( entity_message,  local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict)
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

                        local_operations_dict = self.add_list_to_dict(local_operations_dict, input_entity.guid,
                                                UpdateLocalAttributeProcessor(name=f"insert attribute {insert_attribute}", key=insert_attribute.lower(), value=value))
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

                        local_operations_dict = self.add_list_to_dict(local_operations_dict, input_entity.guid,
                                             UpdateLocalAttributeProcessor(name=f"update attribute {update_attribute}", key=update_attribute.lower(), value=value))

                        if "name" == update_attribute:
                            propagated_operation_downwards_operations_dict = self.add_list_to_dict(propagated_operation_downwards_operations_dict, operation_event_guid, UpdateListEntryBasedOnUniqueValueList(name="update breadcrumb name", unique_list_key=breadcrumb_guid, target_list_key=breadcrumb_name, unique_value=input_entity.guid, target_value= value))

                            super_types = get_super_types_names(input_entity.type_name)
                            m4isourcetype = get_m4i_source_types(super_types)
                            if len(m4isourcetype) > 0:
                                m4isourcetype = m4isourcetype[0]
                            derived_guid, derived_type = get_relevant_hierarchy_entity_fields(m4isourcetype)

                            propagated_operation_downwards_operations_dict = self.add_list_to_dict(propagated_operation_downwards_operations_dict, operation_event_guid,
                                                 UpdateListEntryBasedOnUniqueValueList(name="update derived entity field", unique_list_key=derived_guid,
                                                                    target_list_key=derived_type, unique_value=input_entity.guid, target_value= value))
            # end of handle change attributes

            # handle deleted attributes
            if entity_message.deleted_attributes != []:
                logging.info("handle deleted attributes.")
                for delete_attribute in entity_message.deleted_attributes:
                    if delete_attribute.lower() in self.schema_names:
                        # local_operation_list.append(DeleteLocalAttributeProcessor(name=f"delete attribute {delete_attribute}", key=delete_attribute.lower(), value=value))
                        local_operations_dict = self.add_list_to_dict(local_operations_dict, operation_event_guid,
                                                DeleteLocalAttributeProcessor(name=f"delete attribute {delete_attribute}", key=delete_attribute.lower()))
            # end of handle deleted attributes

            # handle deleted_relationships
            if entity_message.deleted_relationships != {}:
                local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict =  self.handle_deleted_relationships( entity_message,  local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict)

                logging.warning("deleted relationships handled.")
            # end of handle delete relationships

            if entity_message.inserted_relationships != {}:
                local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict  =  self.handle_inserted_relationships(entity_message,  local_operations_dict, propagated_operation_downwards_operations_dict, propagated_operation_upwards_operations_dict)

                logging.warning("inserted relationships handled.")
                # end of handling inserted relationship
        # Charif: Using this indent might cause problems later: please reconsider

        entity_guid_list = list(create_local_operation_dict.keys()) + list(local_operations_dict.keys()) + list(delete_local_operation_dict.keys()) + list(other_operations.keys()) + list(propagated_operation_downwards_operations_dict.keys()) + list(propagated_operation_upwards_operations_dict.keys())
        entity_guid_list = list(set(entity_guid_list))

        for entity_guid in entity_guid_list:
            change_list = []
            create_entity_steps = create_local_operation_dict.get(entity_guid, [])
            if len(create_entity_steps)>0:
                seq = Sequence(name="local operations create", steps = create_entity_steps)
                spec = jsonpickle.encode(seq)
                oc = OperationChange(propagate=False, propagate_down=False, operation = json.loads(spec))
                logging.warning(f"Operation that creates entity document in app search. {len(create_entity_steps)}")
                change_list.append(oc)

            propagated_steps = propagated_operation_downwards_operations_dict.get(entity_guid, [])
            if len(propagated_steps)>0:
                seq = Sequence(name="propagated downwards operation", steps = propagated_steps)
                spec = jsonpickle.encode(seq)
                oc = OperationChange(propagate=True, propagate_down=True, operation = json.loads(spec))
                logging.warning(f"Operation that propagates relevant operations downwards has been created {len(propagated_steps)}")
                change_list.append(oc)

            propagated_steps = propagated_operation_upwards_operations_dict.get(entity_guid, [])
            if len(propagated_steps)>0:
                seq = Sequence(name="propagated upwards operation", steps = propagated_steps)
                spec = jsonpickle.encode(seq)
                oc = OperationChange(propagate=True, propagate_down=False, operation = json.loads(spec))
                logging.warning(f"Operation that propagates relevant operations upwards has been created {len(propagated_steps)}")
                change_list.append(oc)

            local_steps = local_operations_dict.get(entity_guid, [])
            if len(local_steps)>0:
                seq = Sequence(name="local operations", steps = local_steps)
                spec = jsonpickle.encode(seq)
                oc = OperationChange(propagate=False, propagate_down=False, operation = json.loads(spec))
                logging.warning(f"Operation that executes local operations has been created {len(local_steps)}")
                change_list.append(oc)

            delete_entity_steps =  delete_local_operation_dict.get(entity_guid, [])
            if len(delete_entity_steps)>0:
                seq = Sequence(name="local operations", steps = delete_entity_steps)
                spec = jsonpickle.encode(seq)
                oc = OperationChange(propagate=False, propagate_down=False, operation = json.loads(spec))
                logging.warning(f"Operation that deletes entity document from app search. {len(delete_entity_steps)}")
                change_list.append(oc)

            if len(change_list)>0:
                oe = OperationEvent(id=str(uuid.uuid4()),
                                    creation_time=int(datetime.datetime.now().timestamp()*1000),
                                    entity_guid=entity_guid,
                                    changes=change_list)
                result.append(json.dumps(json.loads(oe.to_json())))
            logging.warning(f"Operation event has been created {len(change_list)}")

        return result

# end of class SynchronizeAppsearchLocal


