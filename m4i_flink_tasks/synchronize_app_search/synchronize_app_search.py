import asyncio
import json
# from itertools import pairwise
import logging
from copy import copy  # add this to dependencies
from typing import Callable, Dict, List, Optional, Union

from elastic_enterprise_search import AppSearch
from m4i_atlas_core import (ConfigStore, Entity, EntityDef, get_keycloak_token,
                            get_type_def)
from m4i_atlas_core.entities.atlas.core.relationship.Relationship import \
    Relationship

from m4i_flink_tasks.parameters import *

from .AppSearchDocument import AppSearchDocument
from .elastic import (get_child_entity_docs, get_document, get_documents,
                      send_query)
from .HierarchyMapping import hierarchy_mapping
from m4i_atlas_core import (AtlasChangeMessage, EntityAuditAction,
                            get_entity_by_guid, get_keycloak_token)

ActionHandler = Callable[[Optional[Union[Entity, Relationship]]], None]
logger = logging.getLogger(__name__)

config_store = ConfigStore.get_instance()
update_attributes = [definition, email]

# engine_name = config_store.get("elastic.app.search.engine.name")
# logging.warning(engine_name)

updated_docs: Dict[str, dict] = dict()
breadcrumb_dict: Dict[str, list] = dict()
derived_entity_dict: Dict[str, list] = dict()
docs_dict: Dict[str, dict] = dict()


async def get_super_types(input_type: str) -> List[EntityDef]:
    """This function returns all supertypes of the input type given"""
    access_token = get_keycloak_token()
    entity_def = await get_type_def(input_type, access_token=access_token)
    logging.info(f"entity_def {entity_def}")
    if len(entity_def.super_types) == 0:
        return [entity_def]

    requests = [
        get_super_types(super_type)
        for super_type in entity_def.super_types
    ]
    responses = await asyncio.gather(*requests)

    super_types = [
        super_type
        for response in responses
        for super_type in response
    ]

    return [entity_def, *super_types]
# END get_super_types

async def get_super_types_names(input_type: str) -> List[str]:
    """This function returns all supertype names of the input type given with the given type included."""
    super_types = await get_super_types(input_type)
    logging.info(f"supertypenames: {super_types}")
    return  [super_type.name for super_type in super_types]

def get_source_type(super_types : list) -> SourceType:
    """This function returns the source type, either Business or Technical, based on the supertypes provided as input."""
    if data_domain in super_types or data_entity in super_types or data_attribute in super_types:
        return SourceType.BUSINESS.value

    return SourceType.TECHNICAL.value


def fill_in_dq_scores(schema_keys: list, input_document : dict) -> dict:
    """This function fills in and sets all dq scores to zero in the given document and returns the  updated document."""
    for key in schema_keys:
        if key.startswith("dqscore"):
            input_document[key] = 0
    return input_document

def get_parent_type(input_entity_type: str) -> str:
    """This function get the parent type of the given entity type"""
    return hierarchy_mapping.get(input_entity_type)

def define_breadcrumb(input_document: dict, parent_entity_guid : str, app_search: AppSearch) -> dict:
    """This function defines a breadcrumb by inheriting it from its parent entity and returns the updated document"""
    if not parent_entity_guid:
        return input_document
    parent_entity_document = get_document(parent_entity_guid, app_search)
    if parent_entity_document:
        input_document["breadcrumbguid"] = parent_entity_document["breadcrumbguid"] + \
            [parent_entity_guid]
        input_document["breadcrumbname"] = parent_entity_document["breadcrumbname"] + \
            [parent_entity_document["name"]]
        input_document["breadcrumbtype"] = parent_entity_document["breadcrumbtype"] + \
            [parent_entity_document["typename"]]
    else:
        logging.warning("No corresponding document is found in elastic app belonging to parent entity guid.")

    return input_document


def insert_prefix_to_breadcrumbs_of_child_entities(input_document : dict, child_entity_documents : List[dict]) -> List[dict]:
    """This function updates the breadcrumb of all child entity documents and returns the updated documents in case of an inserted relationship."""
    for child_document in child_entity_documents:
        if input_document[guid] not in child_document["breadcrumbguid"]:
            child_document["breadcrumbguids"] = input_document["breadcrumbguids"] + child_document["breadcrumbguid"]

        if input_document["name"] not in child_document["breadcrumbname"]:
            child_document["breadcrumbname"] = input_document["breadcrumbname"] + child_document["breadcrumbname"]

        if input_document["typename"] not in child_document["breadcrumbtype"]:
            child_document["breadcrumbtype"] = input_document["breadcrumbtype"] + child_document["breadcrumbtype"]

    return child_entity_documents



def get_m4i_source_types(super_types : list) -> list:
    """This function returns the m4i_source_types in the list of given super types."""
    #TODO here are other types missing like data_quality , governance quality and potentially others
    source_types = [data_domain, data_entity,
                    data_attribute, field, dataset, collection, system, person]
    return list(filter(lambda super_type: super_type in source_types, super_types))


async def is_parent_child_relationship(input_document : dict, relationship_key :str, input_relationship : dict):
    """This function determines whether the entity belonging to the input document and the entity corresponding to the end point of the relationship are a parent child pair."""
    super_types = await get_super_types_names(input_relationship["typeName"])
    target_entity_source_types = get_m4i_source_types(super_types)

    if relationship_key.startswith("child") or relationship_key.startswith("parent"):
        return True

    for current_entity_source_type in input_document["m4isourcetype"]:
        for target_entity_source_type in target_entity_source_types:
            if hierarchy_mapping.get(current_entity_source_type) == target_entity_source_type or hierarchy_mapping.get(target_entity_source_type) == current_entity_source_type:
                return True

    return False

# async def is_attribute_field_relationship(doc, inserted_relationship):
#     """This function determines whether the relationship is an attribute field relationship."""
#     super_types = await get_super_types(inserted_relationship["typeName"])
#     target_entity_source_types = get_source_types(
#         super_types + [inserted_relationship["typeName"]])
#     if field in (target_entity_source_types) and data_attribute in doc["m4isourcetype"]:
#         return True
#     if data_attribute in (target_entity_source_types) and field in doc["m4isourcetype"]:
#         return True
#     return False



async def is_parent_child_relationship(input_type : str, relationship_key :str, input_relationship : dict):
    """This function determines whether the entity belonging to the input entity type and the entity corresponding to the end point of the relationship are a parent child pair."""
    super_types = await get_super_types_names(input_relationship["typeName"])
    target_entity_source_types = get_m4i_source_types(super_types)

    if relationship_key.startswith("child") or relationship_key.startswith("parent"):
        return True

    m4i_source_types = await get_super_types_names(input_type)
    
    for current_entity_source_type in m4i_source_types:
        for target_entity_source_type in target_entity_source_types:
            if hierarchy_mapping.get(current_entity_source_type) == target_entity_source_type or hierarchy_mapping.get(target_entity_source_type) == current_entity_source_type:
                return True

    return False

async def is_parent_relationship(input_type : str, relationship_key :str, input_relationship : dict):
    """This function determines whether the entity belonging to the input entity type and the entity corresponding to the end point of the relationship are a parent child pair."""
    super_types = await get_super_types_names(input_relationship["typeName"])
    target_entity_source_types = get_m4i_source_types(super_types)

    if relationship_key.startswith("parent"):
        return True

    m4i_source_types = await get_super_types_names(input_type)
    
    for current_entity_source_type in m4i_source_types:
        for target_entity_source_type in target_entity_source_types:
            if hierarchy_mapping.get(current_entity_source_type) == target_entity_source_type:
                return True

    return False

async def get_all_parent_entity_guids(event_entity: Dict, app_search: AppSearch):
    result = []
    entity_type = event_entity["typeName"]
    for key, input_relationships_ in event_entity["relationshipAttributes"]:
        for input_relationship in input_relationships_ :
            if await is_parent_relationship(entity_type, key, input_relationship):
                super_types = await get_super_types_names(input_relationship["typeName"])
                entity_type_name = get_m4i_source_types(super_types)
                parent_data = get_document(input_relationship[guid], app_search)
                parent_breadcrumb_type = get_hierarchy(parent_data)
                if not parent_breadcrumb_type:
                    parent_breadcrumb_type = entity_type_name

                result.append((input_relationship[guid], parent_breadcrumb_type))

    return result 

def entity_has_parent(input_data: Dict ) ->bool:
        return input_data[parent_guid]

def get_breadcrumb_length(input_data: Dict) -> int:
    return len(input_data[breadcrumb_guid])


def get_hierarchy(input_data: Dict) -> Optional[str]:
    if len(input_data[breadcrumb_type]) > 0 :
        return input_data[breadcrumb_type][0]

    else:
        return None

async def define_parent_entity_document(input_data, app_search, parent_entities):
    """This function defines which proper parent of the child entity should inherit from in acse the child has several parent entities assigned to it, e.g, (data domain A -> data entity C) and  (data entity B -> data entity C)"""
    conceptual_hierarchy_order = [data_domain, data_entity, data_attribute]
    technical_hierarchy_order = [system, collection, dataset, field]

    if len(parent_entities) == 0:
            raise Exception(f"Entity with guid {input_data[guid]} does not have any parent entities")

    if len(parent_entities) == 1:
        return parent_entities[0]

    parent_entity_guid,  parent_entity_type_name = parent_entities[0]



    for index in range(1, len(parent_entities)):
        potential_parent_entity_guid, potential_parent_entity_type_name = parent_entities[index]

        if parent_entity_type_name in conceptual_hierarchy_order and potential_parent_entity_guid in conceptual_hierarchy_order:

            if conceptual_hierarchy_order.index(potential_parent_entity_guid) < conceptual_hierarchy_order.index(parent_entity_type_name):
                parent_entity_guid = potential_parent_entity_guid
                parent_entity_type_name = potential_parent_entity_type_name

            if conceptual_hierarchy_order.index(potential_parent_entity_guid) == conceptual_hierarchy_order.index(parent_entity_type_name):
                if get_breadcrumb_length(parent_entity_type_name) < get_breadcrumb_length(potential_parent_entity_guid):
                    parent_entity_guid = potential_parent_entity_guid
                    parent_entity_type_name = potential_parent_entity_type_name

        elif parent_entity_type_name in technical_hierarchy_order and potential_parent_entity_guid in technical_hierarchy_order:
            if technical_hierarchy_order.index(potential_parent_entity_guid) < technical_hierarchy_order.index(parent_entity_type_name):
                parent_entity_guid = potential_parent_entity_guid
                parent_entity_type_name = potential_parent_entity_type_name

            if technical_hierarchy_order.index(potential_parent_entity_guid) == technical_hierarchy_order.index(parent_entity_type_name):
                if get_breadcrumb_length(parent_entity_type_name) < get_breadcrumb_length(potential_parent_entity_guid):
                    parent_entity_guid = potential_parent_entity_guid
                    parent_entity_type_name = potential_parent_entity_type_name

        else:
            raise Exception(f"hierarchical relationships with unexpected types: ({potential_parent_entity_guid}, {potential_parent_entity_type_name}) and ({parent_entity_guid}, {parent_entity_type_name})")


    return get_document(parent_entity_guid, app_search=app_search)




async def is_attribute_field_relationship(input_type : str, input_relationship : dict):
    """This function determines whether the relationship is an attribute field relationship."""
    super_types = await get_super_types_names(input_type)
    source_entity_source_types = get_m4i_source_types(super_types)

    super_types = await get_super_types_names(input_relationship["typeName"])
    target_entity_source_types = get_m4i_source_types(super_types)

    if field in (target_entity_source_types) and data_attribute in source_entity_source_types:
        return True
    if data_attribute in (target_entity_source_types) and field in source_entity_source_types:
        return True
    return False


# def get_attribute_field_guid(input_document : dict, input_relationship : dict):
#     """This function returns respectively the guid of a data attribute and a field."""
#     if data_attribute in input_document["m4isourcetype"]:
#         return input_document[guid], input_relationship[guid]
#     else:
#         return input_relationship[guid], input_document[guid]

async def get_attribute_field_guid(input_entity : Entity, input_relationship : dict):
    """This function returns respectively the guid of a data attribute and a field."""
    super_types = await get_super_types_names(input_entity.type_name)
    source_entity_source_types = get_m4i_source_types(super_types)

    super_types = await get_super_types_names(input_relationship["typeName"])
    target_entity_source_types = get_m4i_source_types(super_types)

    if data_attribute in source_entity_source_types:
        return input_entity.guid, input_relationship[guid]
    else:
        return input_relationship[guid], input_entity.guid


def define_derived_entity_attribute_field_fields(data_attribute_document: dict, field_document : dict) -> List[dict]:
    """This function defines the derived entity fields for the documents provided which should correspond to a data attribute and field.
    The documents updated are returned as a result."""

    data_attribute_document["derivedfieldguid"] = [field_document[guid]]
    data_attribute_document["derivedfield"] = [field_document[name]]

    field_document["deriveddataattributeguid"] = [data_attribute_document[guid]]
    field_document["deriveddataattribute"] = [data_attribute_document[name]]

    return data_attribute_document, field_document

def delete_derived_entity_attribute_field_fields(data_attribute_document: dict, field_document: dict) -> List[dict]:
    """This function deleted the derived entity fields for the documents provided which should correspond to a data attribute and field.
    The documents updated are returned as a result."""

    # This can be done using a generic function that takes a document and a field as input and resets the field...
    # Implement this function once the schema is formalized

    data_attribute_document["derivedfieldguid"] = []
    data_attribute_document["derivedfield"] = []

    field_document["deriveddataattributeguid"] = []
    field_document["deriveddataattribute"] = []

    return data_attribute_document, field_document

# async def get_parent_child_entity_guid(input_document, key, input_relationship):
#     """This function determines the hierarchy between the input entities and rerturns the guids ordered: parent_guid, child_guid"""
#     super_types = await get_super_types_names(input_relationship["typeName"])
#     target_entity_source_types = get_m4i_source_types(super_types)

#     target_entity_guid = input_relationship[guid]
#     input_entity_guid = input_document[guid]

#     if set(target_entity_source_types) == set(input_document["m4isourcetype"]):
#         if key.startswith("child"):
#             return input_entity_guid, target_entity_guid
#         if key.startswith("parent"):
#             return target_entity_guid, input_entity_guid
#         else:
#             logging.warning("The parent and child entity could not be determined of a relatonship that is classified as a parent-child relationship.")

#     for current_entity_source_type in input_document["m4isourcetype"]:
#         for target_entity_source_type in target_entity_source_types:
#             if hierarchy_mapping.get(current_entity_source_type) == target_entity_source_type:
#                 return target_entity_guid, input_entity_guid

#             if hierarchy_mapping.get(target_entity_source_type) == current_entity_source_type:
#                 return input_entity_guid, target_entity_guid

#     logging.warning("The parent and child entity could not be determined of a relatonship that is classified as a parent-child relationship.")


async def get_parent_child_entity_guid(input_entity_guid, input_entity_typename, key, input_relationship):
    """This function determines the hierarchy between the input entities and rerturns the guids ordered: parent_guid, child_guid"""
    super_types = await get_super_types_names(input_relationship["typeName"])
    target_entity_source_types = get_m4i_source_types(super_types)

    super_types = await get_super_types_names(input_entity_typename)
    source_entity_source_types = get_m4i_source_types(super_types)

    target_entity_guid = input_relationship[guid]

    if set(target_entity_source_types) == set(source_entity_source_types):
        if key.startswith("child"):
            return input_entity_guid, target_entity_guid
        if key.startswith("parent"):
            return target_entity_guid, input_entity_guid
        else:
            logging.warning("The parent and child entity could not be determined of a relatonship that is classified as a parent-child relationship.")

    for current_entity_source_type in source_entity_source_types:
        for target_entity_source_type in target_entity_source_types:
            if hierarchy_mapping.get(current_entity_source_type) == target_entity_source_type:
                return target_entity_guid, input_entity_guid

            if hierarchy_mapping.get(target_entity_source_type) == current_entity_source_type:
                return input_entity_guid, target_entity_guid

    logging.warning("The parent and child entity could not be determined of a relatonship that is classified as a parent-child relationship.")

def get_prefix(parent_entity_guid, first_key : str, second_key: str, app_search: AppSearch):
    parent_entity_document = get_document(parent_entity_guid, app_search)
    return parent_entity_document[first_key] + [parent_entity_document[second_key]]

    

def insert_prefix_to_breadcrumbs_of_child_entities(input_document : dict, child_entity_documents : List[dict]) -> List[dict]:
    """This function updates the breadcrumb of all child entity documents and returns the updated documents in case of an inserted relationship."""
    for child_document in child_entity_documents:
        if input_document[guid] not in child_document["breadcrumbguid"]:
            child_document["breadcrumbguids"] = input_document["breadcrumbguids"] + child_document["breadcrumbguid"]

        if input_document["name"] not in child_document["breadcrumbname"]:
            child_document["breadcrumbname"] = input_document["breadcrumbname"] + child_document["breadcrumbname"]

        if input_document["typename"] not in child_document["breadcrumbtype"]:
            child_document["breadcrumbtype"] = input_document["breadcrumbtype"] + child_document["breadcrumbtype"]

    return child_entity_documents

def update_name_in_breadcrumbs(new_input_entity_name: str, input_document : dict, app_search : AppSearch, updated_documents : List[dict]) -> List[dict]:
    """This function synchronizes updated name of an entity in all breadcrumbs inheriting from this entity."""
    document_entity_name = input_document[name]
    document_entity_guid = input_document[guid]

    engine_name = config_store.get("elastic.app.search.engine.name")

    body = {
        "query":"",
        "filters":{
            breadcrumb_guid:[
                document_entity_guid
            ]
        }
    }


    breadcrumb_guid_list = send_query(app_search=app_search, body=body)

    if len(breadcrumb_guid_list) == 0:
        return updated_documents

    retrieved_documents = []

    for document_guid in copy(breadcrumb_guid_list):
        if document_guid in updated_documents.keys():
            breadcrumb_guid_list.remove(document_guid)
            retrieved_documents.append(updated_documents[document_guid])

    retrieved_documents = retrieved_documents + (get_documents(app_search=app_search, engine_name=engine_name, entity_guid_list=breadcrumb_guid_list))

    for retrieved_document in retrieved_documents:
        if breadcrumb_guid in retrieved_document.keys() and document_entity_guid in retrieved_document[breadcrumb_guid]:
            if breadcrumb_name in retrieved_document.keys():
                index = retrieved_document[breadcrumb_guid].index(document_entity_guid)
                logging.warning("updating breadcrumb name")
                retrieved_document[breadcrumb_name][index] = new_input_entity_name
                logging.warning("updating documents returned by function: update_name_in_breadcrumbs")
                updated_documents[retrieved_document[guid]] = retrieved_document
                logging.warning(json.dumps(retrieved_document))


    return updated_documents


def get_relevant_hierarchy_entity_fields(input_entity_data_type : str):
    derived_type = None
    derived_guid = None
    if input_entity_data_type == data_domain:
        derived_type = derived_data_domain
        derived_guid = derived_data_domain_guid

    elif input_entity_data_type == data_entity:
        derived_type = derived_data_entity
        derived_guid = derived_data_entity_guid

    elif input_entity_data_type == data_attribute:
        derived_type = derived_data_attribute
        derived_guid = derived_data_attribute_guid

    elif input_entity_data_type == system:
        derived_type = derived_system
        derived_guid = derived_system_guid

    elif input_entity_data_type == collection:
        derived_type = derived_collection
        derived_guid = derived_collection_guid

    elif input_entity_data_type == dataset:
        derived_type = derived_dataset
        derived_guid = derived_dataset_guid

    elif input_entity_data_type == field:
        derived_type = derived_field
        derived_guid = derived_field_guid

    elif input_entity_data_type == person:
        derived_type = derived_person
        derived_guid = derived_person_guid

    return derived_guid, derived_type

   


def update_name_in_derived_entity_fields(new_input_entity_name: str, input_document : dict, app_search: AppSearch, updated_documents : List[dict]) -> List[dict]:
    """This function inserts newly defined name or an updated name to all documents inheriting this name"""

    engine_name = config_store.get("elastic.app.search.engine.name")

    document_entity_name = input_document[name]
    document_entity_guid = input_document[guid]

    input_entity_data_type = input_document["typename"]
    # This is the old approach. An alternative approach is to loop over all document fiels having "derived"as prefix. Adjusting this has the lowest priorty.
    if input_entity_data_type == data_domain:
        derived_types = [derived_data_domain]
        derived_guid = [derived_data_domain_guid]

    elif input_entity_data_type == data_entity:
        derived_types = [derived_data_entity, derived_entity_names]
        derived_guid = [derived_data_entity_guid, derived_entity_guids]

    elif input_entity_data_type == data_attribute:
        derived_types = [derived_data_attribute]
        derived_guid = [derived_data_attribute_guid]

    elif input_entity_data_type == system:
        derived_types = [derived_system]
        derived_guid = [derived_system_guid]

    elif input_entity_data_type == collection:
        derived_types = [derived_collection]
        derived_guid = [derived_collection_guid]

    elif input_entity_data_type == dataset:
        derived_types = [derived_dataset, derived_dataset_names]
        derived_guid = [derived_dataset_guid, derived_dataset_guids]

    elif input_entity_data_type == field:
        derived_types = [derived_field]
        derived_guid = [derived_field_guid]

    elif input_entity_data_type == person:
        derived_types = [derived_person]
        derived_guid = [derived_person_guid]

    body = {
        "query":"",
        "filters":{
            "any" :
            [
                {derived_data_domain_guid:      [document_entity_guid]},
                {derived_data_entity_guid:      [document_entity_guid]},
                {derived_entity_guids:          [document_entity_guid]},
                {derived_data_attribute_guid:   [document_entity_guid]},
                {derived_system_guid:           [document_entity_guid]},
                {derived_collection_guid:       [document_entity_guid]},
                {derived_dataset_guid:          [document_entity_guid]},
                {derived_dataset_guids:         [document_entity_guid]},
                {derived_field_guid:            [document_entity_guid]},
                {derived_person_guid:           [document_entity_guid]}
            ]
        }
    }

    derived_entity_guid_list = send_query(app_search=app_search, body=body)

    if len(derived_entity_guid_list) == 0:
        return updated_documents


    retrieved_documents = []
    # check whether documents are already updated 
    for document_guid in copy(derived_entity_guid_list):
        if document_guid in updated_documents.keys():
            derived_entity_guid_list.remove(document_guid)
            retrieved_documents.append(updated_documents[document_guid])

    retrieved_documents = retrieved_documents + (get_documents(app_search=app_search, engine_name=engine_name, entity_guid_list=derived_entity_guid_list))

    for retrieved_document in retrieved_documents:
        logging.warning("start value:")
        logging.warning(json.dumps(retrieved_document))

        for index in range(len(derived_types)):
            derived_type_field = derived_types[index]
            derived_guid_field = derived_guid[index]

            if derived_guid_field in retrieved_document.keys() and isinstance(retrieved_document[derived_guid_field], list) and document_entity_guid in retrieved_document[derived_guid_field]:
                entity_guid_index = retrieved_document[derived_guid_field].index(
                    document_entity_guid)

            if derived_type_field in retrieved_document.keys() and isinstance(retrieved_document[derived_type_field], list):
                entity_name_index = retrieved_document[derived_type_field].index(
                    document_entity_name)

                if(entity_guid_index == entity_name_index):
                    retrieved_document[derived_type_field][entity_name_index] = new_input_entity_name
                    updated_documents[retrieved_document[guid]] = retrieved_document

                else:
                    logging.warning(
                        f"The entity guid index does not match the entity name index.")
        logging.warning("result:")
        logging.warning(json.dumps(retrieved_document))
    return updated_documents

# Until here is checked

# this function is probably incorrect
def delete_prefix_from_breadcrumbs_of_child_entities(input_document : dict, child_entity_documents : List[dict]) -> List[dict]:
    """This function updates the breadcrumb of all child entity documents and returns the updated documents in case of a deleted relationship."""
    for child_document in child_entity_documents:
        if input_document[guid] in child_document["breadcrumbguid"]:
            guid_index = child_document["breadcrumbguid"].index(input_document[guid])
            child_document["breadcrumbguid"] = child_document["breadcrumbguid"][guid_index::]

        if input_document["name"] in child_document["breadcrumbname"]:
            child_document["breadcrumbname"] = child_document["breadcrumbname"][guid_index::]

        if input_document["typename"] in child_document["breadcrumbtype"]:
            child_document["breadcrumbtype"] = child_document["breadcrumbtype"][guid_index::]

    return child_entity_documents


def update_derived_entity_fields_of_child_entities(input_document :  dict, child_entity_documents : List[dict]) -> List[dict]:
    """This function updates the derived entity fields of all child entity documents and returns the updated documents"""
    for child_document in child_entity_documents:
        for key in input_document:
            if key.startswith("derived"):
                child_document[key] = input_document[key]

    return child_entity_documents


def delete_derived_entities(input_document : dict, parent_entity_guid : str, app_search : AppSearch) -> dict:
    parent_entity_doc = get_document(parent_entity_guid, app_search)
    for key in parent_entity_doc:
        if key.startswith("derived") and parent_entity_doc.get(key) == input_document.get(key):
            if type(input_document[key]) == list:
                input_document[key] = []
            else:
                input_document[key] = None
    return input_document


def update_derived_entities(input_document : dict, parent_entity_guid : str, app_search : AppSearch) -> dict:
    parent_entity_doc = get_document(parent_entity_guid, app_search)
    for key in parent_entity_doc:
        if key.startswith("derived") and parent_entity_doc.get(key):
            input_document[key] = parent_entity_doc[key]
    return input_document


def is_governance_role_relationship(key : str) -> bool:
    """This function updates the derived entity fields of all child entity documents and returns the updated documents"""
    return key == "domainLead" or key == "businessOwner" or key == "dataSteward"


def delete_parent_guid(input_document : dict) -> dict:
    """This function deletes the parent guid of the given document."""
    input_document["parentguid"] = None
    return input_document


def delete_breadcrumb(input_document : dict) -> dict:
    """This function deleted a breadcrumb of a child entity given that relationship to its parent is deleted."""
    input_document["breadcrumbguid"] = []
    input_document["breadcrumbname"] = []
    input_document["breadcrumbtype"] = []

    return input_document


def define_parent_guid(input_document : dict, parent_entity_guid : str) -> dict:
    """This function defines the parent guid of the entity corresponsing to the document given."""
    input_document["parentguid"] = parent_entity_guid
    return input_document


######

def update_governance_role_derived_entity_fields(input_document : dict, key : str, input_entity : Entity):
    """This function updates the derived entity fields of all child entity documents and returns the updated documents"""
    if is_governance_role_relationship(key=key):
        if input_document["typename"] == data_domain:

            for relationship_attributes in input_entity.relationship_attributes.get("domainLead"):
                input_document["deriveddomainleadguid"] = relationship_attributes[guid]
                input_document["derivedpersonguid"] = [(input_document["deriveddomainleadguid"])]

        if input_document["typename"] == data_entity or input_document["typename"] == data_attribute:

            for relationship_attributes in input_entity.relationship_attributes.get("businessOwner"):
                input_document["deriveddataownerguid"] = input_entity.relationship_attributes.get(
                    "businessOwner")[guid]
                input_document["deriveddatastewardguid"] = input_entity.relationship_attributes.get("dataSteward")[
                    guid]
                input_document["derivedpersonguid"] = [
                    (input_document["deriveddataownerguid"], input_document["deriveddatastewardguid"])]

        return input_document

def insert_governance_role_derived_entity_fields(input_document : dict, key : str, input_relationship : dict):
    """This function updates the derived entity fields of all child entity documents and returns the updated documents"""
    if is_governance_role_relationship(key=key):
        if input_document["typename"] == data_domain and key == "domainLead":

            input_document["deriveddomainleadguid"].append(input_relationship[guid])
            input_document["derivedpersonguid"].append(input_relationship[guid])

        if (input_document["typename"] == data_entity or input_document["typename"] == data_attribute) and key == "businessOwner":

            input_document["deriveddatastewardguid"].append(input_relationship[guid])
            input_document["derivedpersonguid"].append(input_relationship[guid])

        if (input_document["typename"] == data_entity or input_document["typename"] == data_attribute) and key == "dataOwner":

            input_document["deriveddataownerguid"].append(input_relationship[guid])
            input_document["derivedpersonguid"].append(input_relationship[guid])

        return input_document

def delete_governance_role_derived_entity_fields(input_document : dict, key : str, input_relationship : dict):
    """This function updates the derived entity fields of all child entity documents and returns the updated documents"""
    if is_governance_role_relationship(key=key):
        if input_document["typename"] == data_domain and key == "domainLead":
            if input_relationship[guid] in input_document["deriveddomainleadguid"]:
                input_document["deriveddomainleadguid"].remove(input_relationship[guid])

            if input_relationship[guid] in input_document["derivedpersonguid"]:
                input_document["derivedpersonguid"].remove(input_relationship[guid])


        if (input_document["typename"] == data_entity or input_document["typename"] == data_attribute) and key == "businessOwner":

            if input_relationship[guid] in input_document["deriveddatastewardguid"]:
                input_document["deriveddatastewardguid"].remove(input_relationship[guid])

            if input_relationship[guid] in input_document["derivedpersonguid"]:
                input_document["derivedpersonguid"].remove(input_relationship[guid])


        if (input_document["typename"] == data_entity or input_document["typename"] == data_attribute) and key == "dataOwner":

            if input_relationship[guid] in input_document["deriveddataownerguid"]:
                input_document["deriveddataownerguid"].remove(input_relationship[guid])

            if input_relationship[guid] in input_document["derivedpersonguid"]:
                input_document["derivedpersonguid"].remove(input_relationship[guid])

        return input_document





async def handle_inserted_relationships(entity_message, new_input_entity, inserted_relationships, app_search, doc=None):
    updated_docs: Dict[str, dict] = dict()
    engine_name = config_store.get("elastic.app.search.engine.name")
    schema_keys = sorted(
        list(app_search.get_schema(engine_name=engine_name).keys()))
    input_entity_guid = new_input_entity.guid
    parent_child_dict = dict()

    if not doc:
        doc = get_document(input_entity_guid, app_search)

    if not doc:
        logging.warning(
            f"Updated entity having guid {new_input_entity.guid} does not have a corresponding app search document.")
        return updated_docs

    for key, inserted_relationships_ in inserted_relationships.items():

        if inserted_relationships_ == []:
            continue
        for inserted_relationship in inserted_relationships_:

            if await is_parent_child_relationship(doc, key, inserted_relationship):
                parent_entity_guid, child_entity_guid = await get_parent_child_entity_guid(
                    doc, key, inserted_relationship)
                if input_entity_guid == child_entity_guid:
                    doc = define_breadcrumb(
                        doc, parent_entity_guid, app_search)
                    doc = define_parent_guid(doc, parent_entity_guid)
                    doc = update_derived_entities(
                        doc, parent_entity_guid, app_search)

                child_docs = get_child_entity_docs(
                    input_entity_guid, app_search, engine_name)
                child_docs = insert_prefix_to_breadcrumbs_of_child_entities(
                    doc, child_docs)
                child_docs = update_derived_entity_fields_of_child_entities(
                    doc, child_docs)

                for child_doc in child_docs:
                    updated_docs[child_doc[guid]] = child_doc

            if is_governance_role_relationship(key):
                doc = insert_governance_role_derived_entity_fields(
                    doc, key, inserted_relationship)
                child_docs = get_child_entity_docs(
                    input_entity_guid, app_search, engine_name)
                child_docs = update_derived_entity_fields_of_child_entities(
                    doc, child_docs)

                for child_doc in child_docs:
                    updated_docs[child_doc[guid]] = child_doc

            if await is_attribute_field_relationship(doc, inserted_relationship):

                attribute_guid, field_guid = get_attribute_field_guid(
                    doc, inserted_relationship)

                data_attribute_doc, field_doc = get_documents(app_search, engine_name, [attribute_guid, field_guid])

                docs = define_derived_entity_attribute_field_fields(data_attribute_doc, field_doc)
                for doc_ in docs:
                    updated_docs[doc_[guid]] = doc_

        updated_docs[input_entity_guid] = doc

    return updated_docs


async def handle_deleted_relationships(entity_message, input_entity, deleted_relationships, app_search, doc=None):
    updated_docs: Dict[str, dict] = dict()
    engine_name = config_store.get("elastic.app.search.engine.name")
    schema_keys = sorted(
        list(app_search.get_schema(engine_name=engine_name).keys()))

    input_entity_guid = input_entity.guid
    parent_child_dict = dict()

    if not doc:
        doc = get_document(input_entity_guid, app_search)

    if not doc:
        logging.warning(
            f"Updated entity having guid {input_entity.guid} does not have a corresponding app search document.")
        return updated_docs

    for key, deleted_relationships_ in deleted_relationships.items():

        if deleted_relationships_ == []:
            continue
        for deleted_relationship in deleted_relationships_:

            if await is_parent_child_relationship(doc, key, deleted_relationship):
                parent_entity_guid, child_entity_guid = get_parent_child_entity_guid(
                    doc, key, deleted_relationship)
                if input_entity_guid == child_entity_guid:
                    doc = delete_breadcrumb(
                        doc)
                    doc = delete_parent_guid(doc)
                    doc = delete_derived_entities(
                        doc, parent_entity_guid, app_search)

                child_docs = get_child_entity_docs(
                    input_entity_guid, app_search, engine_name)
                child_docs = delete_prefix_from_breadcrumbs_of_child_entities(
                    doc, child_docs)
                child_docs = update_derived_entity_fields_of_child_entities(
                    doc, child_docs)

                for child_doc in child_docs:
                    updated_docs[child_doc[guid]] = child_doc

            if is_governance_role_relationship(key):
                doc = update_governance_role_derived_entity_fields(
                    doc, key, deleted_relationship)
                child_docs = get_child_entity_docs(
                    input_entity_guid, app_search, engine_name)
                child_docs = update_derived_entity_fields_of_child_entities(
                    doc, child_docs)

                for child_doc in child_docs:
                    updated_docs[child_doc[guid]] = child_doc

            if is_attribute_field_relationship(doc, deleted_relationship):

                attribute_guid, field_guid = get_attribute_field_guid(
                    doc, deleted_relationship)
                data_attribute_document, field_document = get_documents(app_search, engine_name, [attribute_guid, field_guid])
                docs = delete_derived_entity_attribute_field_fields(
                    data_attribute_document, field_document)
                for doc_ in docs:
                    updated_docs[doc_[guid]] = doc_

        updated_docs[input_entity_guid] = doc

    return updated_docs



def handle_updated_attributes(entity_message, input_entity, updated_attributes, app_search: AppSearch, doc=None) -> dict:
    """This function updates the document for the relevant set of inserted or updated attributes in the Apache Atlas entity.
    This function returns a dictionary with all updated documents as output with the following structure: guid -> app search document"""
    engine_name = config_store.get("elastic.app.search.engine.name")
    updated_docs: Dict[str, dict] = dict()
    logging.warning(engine_name)
    schema_keys = sorted(
        list(app_search.get_schema(engine_name=engine_name).keys()))
    input_entity_guid = input_entity.guid

    if not doc:
        doc = get_document(input_entity_guid, app_search)

    if not doc:
        logging.warning(
            f"Updated entity having guid {input_entity.guid} does not have a corresponding app search document.")
        return updated_docs

    for update_attribute in updated_attributes:
        logging.warning("start update attribute")

        if update_attribute in input_entity.attributes.unmapped_attributes.keys() and update_attribute in schema_keys and update_attribute in update_attributes:
            doc[update_attribute] = input_entity.attributes.unmapped_attributes[update_attribute]

    if name in updated_attributes and name in input_entity.attributes.unmapped_attributes.keys() and name in schema_keys:
        logging.warning("start update name.")
        if input_entity.attributes.unmapped_attributes[name] != doc.get(name):
            input_entity_name = input_entity.attributes.unmapped_attributes[name]
            logging.warning("start breadcrumb update.")
            updated_docs = update_name_in_breadcrumbs(
                input_entity_name, doc, app_search, updated_docs)

            logging.warning("start dervived name update.")
            updated_docs = update_name_in_derived_entity_fields(
                input_entity_name, doc,  app_search, updated_docs)
            doc[name] = input_entity_name

    updated_docs[input_entity_guid] = doc
    return updated_docs


def handle_deleted_attributes(entity_message, input_entity, deleted_attributes, app_search, doc=None) -> dict:
    """This function updates the document for the relevant set ofdeleted attributes in the Apache Atlas entity.
    This function returns a dictionary with all updated documents as output with the following structure: guid -> app search document"""
    engine_name = config_store.get("elastic.app.search.engine.name")
    updated_docs: Dict[str, dict] = dict()
    schema_keys = sorted(
        list(app_search.get_schema(engine_name=engine_name).keys()))
    input_entity_guid = input_entity.guid

    if not doc:
        doc = get_document(input_entity_guid, app_search)

    if not doc:
        logging.warning(
            f"Updated entity having guid {input_entity.guid} does not have a corresponding app search document.")
        return updated_docs

    for deleted_attribute in deleted_attributes:

        if deleted_attribute in input_entity.attributes.unmapped_attributes.keys() and deleted_attribute in schema_keys and deleted_attribute in update_attributes:
            doc[deleted_attribute] = input_entity.attributes.unmapped_attributes[deleted_attribute]

    if name in deleted_attribute and name in input_entity.attributes.unmapped_attributes.keys() and name in schema_keys:

        if input_entity.attributes.unmapped_attributes[name] != doc.get(name):
            input_entity_name = input_entity.qualified_name

            updated_docs = update_name_in_breadcrumbs(
                input_entity_name, doc, app_search, updated_docs)
            updated_docs = update_name_in_derived_entity_fields(
                input_entity_name, doc,  app_search, updated_docs)
            doc[name] = input_entity_name

    updated_docs[input_entity_guid] = doc
    return updated_docs


async def create_document(input_entity : Entity) -> dict:
    """This function creates a new app search document corresponding tp the entity belonging to the input entity message.
    The output document has the standard fields that could be infered directly from the entity message filled in.
    The dq scores are all equal to zero"""
    super_types = await get_super_types_names(input_entity.type_name)
    app_search_document = AppSearchDocument(id=input_entity.guid,
        guid = input_entity.guid,
        sourcetype = asyncio.run(get_source_type(super_types)),
        referenceablequalifiedname = input_entity.attributes.unmapped_attributes["qualifiedName"],
        typename = input_entity.type_name,
        m4isourcetype = get_m4i_source_types(super_types),
        supertypenames = super_types,
        name = input_entity.attributes.unmapped_attributes.get(name),
        definition =  input_entity.attributes.unmapped_attributes.get(definition),
        email = input_entity.attributes.unmapped_attributes.get(email)
    )

    return json.loads(app_search_document.to_json())

    # new_doc = define_breadcrumb(new_doc, entity_message, app_search)
    # new_doc = define_derived_entity_fields(new_doc, entity_message, app_search)





def get_parent_entity_guid(input_entity : Entity):
    "This function returns the guid of the parent entity of the input entity given."
    entity_relationships = input_entity.relationship_attributes
    for key in entity_relationships.keys():
        if key.startswith("parent"):
            return entity_relationships[key]["guid"]

    parent_type = get_parent_type(input_entity.type_name)

    for key, val in entity_relationships.items():
        if val != [] and val[0].get("typeName") == parent_type:
            if len(val) > 1:
                logging.warning(f"several parent entities are found for the input entit: {input_entity}.")
                # The code should never reach this part!
            else:
                return val[0]["guid"]


def get_parent_guids(input_entity_guid):
    input_entity_guid
    pass

