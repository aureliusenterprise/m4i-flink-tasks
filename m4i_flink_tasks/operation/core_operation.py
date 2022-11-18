import asyncio
import datetime
import json
import logging
import math
import sys
#from datetime import datetime, timedelta
import traceback
from abc import ABC
from telnetlib import SE
from typing import Dict, List, Optional

import jsonpickle
import numpy as np
import pandas as pd
from elastic_enterprise_search import AppSearch, EnterpriseSearch
from m4i_atlas_core import Entity, EntityAuditAction

from m4i_flink_tasks.parameters import *
from m4i_flink_tasks.synchronize_app_search import (
    get_attribute_field_guid, get_document, get_m4i_source_types,
    get_parent_child_entity_guid, get_relevant_hierarchy_entity_fields,
    get_super_types_names, is_attribute_field_relationship,
    is_parent_child_relationship)
from m4i_flink_tasks.synchronize_app_search.elastic import delete_document

from ..synchronize_app_search import (get_direct_child_entity_docs,
                                      get_m4i_source_types, get_source_type,
                                      get_super_types_names,
                                      make_elastic_app_search_connect,
                                      update_dq_score_fields)
from ..synchronize_app_search.AppSearchDocument import AppSearchDocument


class AbstractProcessor(ABC):
    """All processors of the workflow inherit from this AbstractProcessor class.
    The only purpose of this class is to distinguish classes which are intended to be processors
    from other arbitrary classes. Each processor must have a method process

    Parameters
    ----------
        Name of the processor
    """
    def __init__(self, name:str):
        self.name = name
        assert(name != None and len(name)>0)
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:
        return {}
    # end of process

    def transform(self, input_data:Dict, app_search: AppSearch):
        return self

# end of class AbstractProcessor



class Sequence(AbstractProcessor):
    """Sequence process step executes sequentially a list of process steps, where the putput of one process step
    is added to the input of the next process steps.

    Parameters
    ----------
    name :str
        Name of the processor
    steps :List
        List of process steps to be executed.
    """
    def __init__(self, name:str, steps:List):
        super().__init__(name)
        self.steps = steps
        print(type(self.steps[0]))
        assert(self.steps!=None and isinstance(self.steps, list) and len(self.steps)>0)
        #assert(isinstance(self.steps[0],AbstractProcessor))
    # end of __init__

    def process(self, input_data: Dict, app_search: AppSearch) -> Dict:
        #logging = input_data["logging"]
        input_data_ = input_data
        for step in self.steps:
            #logging.append({"ts":str(datetime.datetime.now()),
            #                "class":"sequence",
            #                "step_name":step.name})
            #print(f"====LOG {str(datetime.datetime.now())},sequence,{step.name}")
            input_data_ = step.process(input_data_, app_search)
        return input_data_
    # end of process

    def transform(self, input_data: Dict, app_search: AppSearch):
        result = []
        for step in self.steps:
            result.append(step.transform(input_data, app_search))

        return Sequence(name=self.name, steps=result)
    # end of transform
# end of class Sequence

class WorkflowEngine:
    """Workflow engine creates an object of the agent specification and triggers the process method of the object.
    The resulting data of the processing are returned as a dictionary.

    Parameters
    ----------
    specification :str
        specification of a hunting agent. The hunting agent object is serialized as a string using jsonpickle.
    """
    def __init__(self, specification:str):
        self.specification = jsonpickle.decode(specification)
    # end of __init__

    def run(self, input_data:Dict, app_search: AppSearch) -> Dict:
        data = self.specification.process(input_data, app_search)
        return data
    # end of run

    def transform(self, input_data:Dict, app_search: AppSearch) -> Dict:
        specification = self.specification.transform(input_data, app_search)
        return specification
    # end of transform
# end of class WorkflowEngine


class CreateLocalEntityProcessor(AbstractProcessor):
    """CreateLocalEntityProcessor is a processot to create an entity with
        basic values and attributes.

    Parameters
    ----------
    name :str
        Name of the processor
    """
    def __init__(self,
                name:str,
                entity_type:str,
                entity_guid:str,
                entity_name:str,
                entity_qualifiedname:str):
        super().__init__(name)
        self.entity_guid = entity_guid
        self.entity_type = entity_type
        self.entity_name = entity_name
        self.entity_qualifiedname = entity_qualifiedname
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:
        super_types = get_super_types_names(self.entity_type)
        app_search_document = AppSearchDocument(id=self.entity_guid,
            guid = self.entity_guid,
            sourcetype = get_source_type(super_types),
            typename = self.entity_type,
            m4isourcetype = get_m4i_source_types(super_types),
            supertypenames = super_types,
            name=self.entity_name,
            referenceablequalifiedname=self.entity_qualifiedname
        )
        return json.loads(app_search_document.to_json())
    # end of process

# end of class CreateLocalEntityProcessor

class UpdateListEntryBasedOnUniqueValueList(AbstractProcessor):
    """UpdateListEntryBasedOnUniqueValueList is a processot to update a single key
        in the data.

    Parameters
    ----------
    name :str
        Name of the processor
    """

    def __init__(self,
                name:str,
                unique_list_key:str,
                target_list_key:str,
                unique_value:str,
                target_value:str):

        super().__init__(name)
        self.unqiue_list_key = unique_list_key
        self.target_list_key = target_list_key
        self.unqiue_value = unique_value
        self.target_value = target_value
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:
        # Charif: Remark .. The first time when this operator is called for a breadcrumb name update, the provided breadcrumb will not be found
        if self.unqiue_value in input_data[self.unqiue_list_key]:
            index = input_data[self.unqiue_list_key].index(self.unqiue_value)
            input_data = InsertElementInList(name=self.name, key=self.target_list_key, index = index, value=self.target_value).process(input_data, app_search)

        return input_data
    # end of process

# end of class UpdateLocalAttributeProcessor

class DeleteListEntryBasedOnUniqueValueList(AbstractProcessor):
    """DeleteListEntryBasedOnUniqueValueList is a processot
        in the data.

    Parameters
    ----------
    name :str
        Name of the processor
    """

    def __init__(self,
                name:str,
                unique_list_key:str,
                target_list_key:str,
                unique_value:str):

        super().__init__(name)
        self.unqiue_list_key = unique_list_key
        self.target_list_key = target_list_key
        self.unqiue_value = unique_value
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:

        index = input_data[self.unqiue_list_key].index(self.unqiue_value)
        input_data = DeleteElementFromList(name=self.name, key=self.target_list_key, index = index).process(input_data, app_search)

        return input_data
    # end of process

# end of class UpdateLocalAttributeProcessor






class UpdateLocalAttributeProcessor(AbstractProcessor):
    """UpdateLocalAttributeProcessor is a processot to update a single key
        in the data.

    Parameters
    ----------
    name :str
        Name of the processor
    """
    def __init__(self,
                name:str,
                key:str,
                value):
        super().__init__(name)
        self.key = key
        self.value = value
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:
       input_data[self.key] = self.value
       return input_data
    # end of process

# end of class UpdateLocalAttributeProcessor

class AddElementToListProcessor(AbstractProcessor):
    """UpdateLocalAttributeProcessor is a processot to update a single key
        in the data.

    Parameters
    ----------
    name :str
        Name of the processor
    """
    def __init__(self,
                name:str,
                key:str,
                value):
        super().__init__(name)
        self.key = key
        self.value = value
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:

        if isinstance(input_data[self.key], list):
            input_data[self.key].append(self.value)
        return input_data


    # end of process

# end of class AddElementToListProcessor



class DeleteLocalAttributeProcessor(AbstractProcessor):
    """DeleteLocalAttributeProcessor is a processot to delete the value of a
        single key in the data.

    Parameters
    ----------
    name :str
        Name of the processor
    """
    def __init__(self,
                name:str,
                key:str):
        super().__init__(name)
        self.key = key
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:
        if type(input_data[self.key]) == list:
            input_data[self.key] = []
        else:
            input_data[self.key] = None
        return input_data
    # end of process

# end of class DeleteLocalAttributeProcessor


class UpdateDqScoresProcessor(AbstractProcessor):
    """UpdateDqScoresProcessor is an processor which updates all data quality
    scores of a local instance.

    Parameters
    ----------
    name :str
        Name of the processor
    """
    def __init__(self,
                name:str):
        super().__init__(name)
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:
       #TODO: do something meaningful here
       return input_data
    # end of process

# end of class UpdateDqScoresProcessor





class UpdateListEntryProcessor(AbstractProcessor):
    """UpdateListEntryProcessor is an processor which updates the a given value in a list with another provided value
    scores of a local instance.

    Parameters
    ----------
    name: str
        Name of the processor
    key: str
        This is the name of the field in the app search schema
    old_value: str
        This is the value to be changed in the provided field
    new_value: str
        This is the new value that will be inserted in the provided field with the index that corresponds to the old_value
    """

    def __init__(self,
                name:str,
                key: str,
                guid_key: str,
                reference_guid:str,
                new_value : str
                ):
        super().__init__(name)
        self.key = key
        self.guid_key = guid_key
        self.reference_guid = reference_guid
        self.new_value = new_value

    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:
        if not self.key in input_data.keys():
            raise Exception(f"Key {self.key} not in input data")

        if not self.guid_key in input_data.keys():
            raise Exception(f"Guid Key {self.guid_key} not in input data")

        arr = input_data[self.guid_key]
        try:
            index = arr.index(self.reference_guid)
            arr_to_be_changed = input_data[self.key]
            arr_to_be_changed[index] = self.new_value
            input_data[self.key] =  arr_to_be_changed
        except:
            logging.warning(f"did not find the specified reference_guid {self.reference_guid} int the array {input_data[self.guid_key]}")
            pass

        return input_data
    # end of process

# end of class UpdateListEntryProcessor

# class DeleteListEntryProcessor(AbstractProcessor):
#     """UpdateListEntryProcessor is an processor which updates the a given value in a list with another provided value
#     scores of a local instance.

#     Parameters
#     ----------
#     name: str
#         Name of the processor
#     key: str
#         This is the name of the field in the app search schema
#     old_value: str
#         This is the value to be changed in the provided field
#     new_value: str
#         This is the new value that will be inserted in the provided field with the index that corresponds to the old_value
#     """

#     def __init__(self,
#                 name:str,
#                 key: str,
#                 value : str,
#                 ):
#         super().__init__(name)
#         self.key = key
#         self.value = value

#     # end of __init__

#     def process(self, input_data:Dict) -> Dict:
#         if not self.key in input_data.keys():
#             raise Exception(f"Key {self.key} not in input data")

#         if not self.value in input_data[self.key]:
#         # raise Exception(f"Value {self.old_value} not in input data")  # I commented this out, so no exception is failed when the breadcrumbname is updated.
#             pass
#         input_data[self.key].remove(self.value)
#         return input_data
#     # end of process

# # end of class UpdateListEntryProcessor

class Delete_Hierarchical_Relationship(AbstractProcessor):
    def __init__(self,
            name:str,
            parent_entity_guid : str,
            child_entity_guid: str,
            current_entity_guid: str,
            derived_guid: str):
        super().__init__(name)
        self.parent_entity_guid = parent_entity_guid
        self.child_entity_guid = child_entity_guid
        self.current_entity_guid = current_entity_guid
        self.derived_guid = derived_guid
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:

        if self.current_entity_guid == self.parent_entity_guid:
            return input_data

        elif self.current_entity_guid == self.child_entity_guid:

            # breadcrumb updates -> relevant for child entity
            input_data = DeletePrefixFromList(name="update breadcrumb name", key="breadcrumbname", guid_key="breadcrumbguid" , first_guid_to_keep=self.child_entity_guid).process(input_data, app_search)
            input_data = DeletePrefixFromList(name="update breadcrumb type", key="breadcrumbtype", guid_key="breadcrumbguid" , first_guid_to_keep=self.child_entity_guid).process(input_data, app_search)
            input_data = DeletePrefixFromList(name="update breadcrumb guid", key="breadcrumbguid", guid_key="breadcrumbguid" , first_guid_to_keep=self.child_entity_guid).process(input_data, app_search)

            # delete parent guid -> relevant for child
            input_data = DeleteLocalAttributeProcessor(name=f"delete attribute {parent_guid}", key=parent_guid).process(input_data, app_search)

            # delete derived entity guid -> relevant for child
            if self.derived_guid in conceptual_hierarchical_derived_entity_guid_fields_list:
                index = conceptual_hierarchical_derived_entity_guid_fields_list.index(self.derived_guid)
                to_be_deleted_derived_guid_fields = conceptual_hierarchical_derived_entity_guid_fields_list[:index+1]


            if self.derived_guid in technical_hierarchical_derived_entity_guid_fields_list:
                index = technical_hierarchical_derived_entity_guid_fields_list.index(self.derived_guid)
                to_be_deleted_derived_guid_fields = technical_hierarchical_derived_entity_guid_fields_list[:index+1]

            for to_be_deleted_derived_guid_field in to_be_deleted_derived_guid_fields:

                input_data = DeleteLocalAttributeProcessor(name=f"delete derived entity field: {to_be_deleted_derived_guid_field}", key = to_be_deleted_derived_guid_field).process(input_data, app_search)
                input_data = DeleteLocalAttributeProcessor(name=f"delete derived entity field: {hierarchical_derived_entity_fields_mapping[to_be_deleted_derived_guid_field]}", key = hierarchical_derived_entity_fields_mapping[to_be_deleted_derived_guid_field]).process(input_data, app_search)
            return input_data

        else:
            logging.warning(f"parent entity guid: {self.parent_entity_guid}, child entity guid: {self.child_entity_guid}, current entity guid: {self.current_entity_guid}")
            raise Exception(f"Unexpected state.")

    def transform(self, input_data:Dict, app_search:AppSearch) -> Dict:
        steps = []
        if self.current_entity_guid == self.parent_entity_guid:
            return Delete_Hierarchical_Relationship(name="delete hierarchical relationship", parent_entity_guid=self.parent_entity_guid, child_entity_guid=self.child_entity_guid, current_entity_guid=self.child_entity_guid, derived_guid = self.derived_guid)

            # steps.append(Delete_Hierarchical_Relationship(name="delete hierarchical relationship", parent_entity_guid=self.parent_entity_guid, child_entity_guid=self.child_entity_guid, current_entity_guid=self.child_entity_guid, derived_guid = self.derived_guid))
            # return Sequence(name="trasnformed deleted hierarchical relationships", steps = steps)

        elif self.current_entity_guid == self.child_entity_guid:
            steps.append(DeletePrefixFromList(name="update breadcrumb name", key="breadcrumbname", guid_key="breadcrumbguid" , first_guid_to_keep=self.child_entity_guid))
            steps.append(DeletePrefixFromList(name="update breadcrumb type", key="breadcrumbtype", guid_key="breadcrumbguid" , first_guid_to_keep=self.child_entity_guid))
            steps.append(DeletePrefixFromList(name="update breadcrumb guid", key="breadcrumbguid", guid_key="breadcrumbguid" , first_guid_to_keep=self.child_entity_guid))

            if self.derived_guid in conceptual_hierarchical_derived_entity_guid_fields_list:
                index = conceptual_hierarchical_derived_entity_guid_fields_list.index(self.derived_guid)
                to_be_deleted_derived_guid_fields = conceptual_hierarchical_derived_entity_guid_fields_list[:index+1]


            if self.derived_guid in technical_hierarchical_derived_entity_guid_fields_list:
                index = technical_hierarchical_derived_entity_guid_fields_list.index(self.derived_guid)
                to_be_deleted_derived_guid_fields = technical_hierarchical_derived_entity_guid_fields_list[:index+1]

            for to_be_deleted_derived_guid_field in to_be_deleted_derived_guid_fields:

                steps.append(DeleteLocalAttributeProcessor(name=f"delete derived entity field: {to_be_deleted_derived_guid_field}", key = to_be_deleted_derived_guid_field))
                steps.append(DeleteLocalAttributeProcessor(name=f"delete derived entity field: {hierarchical_derived_entity_fields_mapping[to_be_deleted_derived_guid_field]}", key = hierarchical_derived_entity_fields_mapping[to_be_deleted_derived_guid_field]))

            return Sequence(name="transforms deleted hierarchical relationships", steps = steps)

        else:
            logging.warning(f"parent entity guid: {self.parent_entity_guid}, child entity guid: {self.child_entity_guid}, current entity guid: {self.current_entity_guid}")
            raise Exception(f"Unexpected state.")



class Insert_Hierarchical_Relationship(AbstractProcessor):
    def __init__(self,
            name:str,
            parent_entity_guid : str,
            child_entity_guid: str,
            current_entity_guid: str):
        super().__init__(name)
        self.parent_entity_guid = parent_entity_guid
        self.child_entity_guid = child_entity_guid
        self.current_entity_guid = current_entity_guid
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:

        if self.current_entity_guid == self.parent_entity_guid:
            return input_data

        elif self.current_entity_guid == self.child_entity_guid:
            parent_entity_document = get_document(self.parent_entity_guid, app_search)


            if not parent_entity_document:
                logging.warning(f"no parent entity found corresponding to guid {self.parent_entity_guid}")
                # raise Exception(f"no parent entity found corresponding to guid {self.parent_entity_guid}. This entity should be created, but is not created.")
                return input_data

            parent_m4isourcetype = parent_entity_document["m4isourcetype"]
            derived_guid, derived_type = get_relevant_hierarchy_entity_fields(parent_m4isourcetype[0])

            # derived entity fields -> relevant for child entity

            if derived_guid in conceptual_hierarchical_derived_entity_guid_fields_list:
                index = conceptual_hierarchical_derived_entity_guid_fields_list.index(derived_guid)
                to_be_inserted_derived_guid_fields = conceptual_hierarchical_derived_entity_guid_fields_list[:index]


            if derived_guid in technical_hierarchical_derived_entity_guid_fields_list:
                index = technical_hierarchical_derived_entity_guid_fields_list.index(derived_guid)
                to_be_inserted_derived_guid_fields = technical_hierarchical_derived_entity_guid_fields_list[:index]

            for to_be_inserted_derived_guid_field in to_be_inserted_derived_guid_fields:

                input_data = (UpdateLocalAttributeProcessor(name=f"insert derived entity field {to_be_inserted_derived_guid_field}", key = to_be_inserted_derived_guid_field, value = parent_entity_document[to_be_inserted_derived_guid_field])).process(input_data, app_search)
                input_data = (UpdateLocalAttributeProcessor(name=f"insert derived entity field {hierarchical_derived_entity_fields_mapping[to_be_inserted_derived_guid_field]}", key = hierarchical_derived_entity_fields_mapping[to_be_inserted_derived_guid_field], value = parent_entity_document[hierarchical_derived_entity_fields_mapping[to_be_inserted_derived_guid_field]])).process(input_data, app_search)

            # define parent guid -> relevant for child
            input_data = (UpdateLocalAttributeProcessor(name=f"insert attribute {parent_guid}", key=parent_guid, value=self.parent_entity_guid)).process(input_data, app_search)

            input_data = (UpdateLocalAttributeProcessor(name=f"insert attribute {derived_guid}", key=derived_guid, value=parent_entity_document[derived_guid] + [self.parent_entity_guid])).process(input_data, app_search)

            input_data = (UpdateLocalAttributeProcessor(name=f"insert attribute {derived_type}", key=derived_type, value=parent_entity_document[derived_type] + [parent_entity_document[name]])).process(input_data, app_search) # Charif: Validate whether this will work for nested structures!!

            # breadcrumb updates -> relevant for child entity
            breadcrumbguid_prefix = parent_entity_document["breadcrumbguid"] + [parent_entity_document["guid"]]
            breadcrumbname_prefix = parent_entity_document["breadcrumbname"] + [parent_entity_document["name"]]
            breadcrumbtype_prefix = parent_entity_document["breadcrumbtype"] + [parent_entity_document["typename"]]

            input_data = (InsertPrefixToList(name="update breadcrumb guid", key="breadcrumbguid", input_list=breadcrumbguid_prefix)).process(input_data, app_search)
            input_data = (InsertPrefixToList(name="update breadcrumb name", key="breadcrumbname", input_list=breadcrumbname_prefix)).process(input_data, app_search)
            input_data = (InsertPrefixToList(name="update breadcrumb type", key="breadcrumbtype", input_list=breadcrumbtype_prefix)).process(input_data, app_search)

            return input_data

        else:
            logging.warning(f"parent entity guid: {self.parent_entity_guid}, child entity guid: {self.child_entity_guid}, current entity guid: {self.current_entity_guid}")
            raise Exception(f"Unexpected state.")


    def transform(self, input_data:Dict, app_search: AppSearch) -> Dict:
        steps = []
        if self.current_entity_guid == self.parent_entity_guid:
            return Insert_Hierarchical_Relationship(name="transformed inserted hierarchical relationships", parent_entity_guid=self.parent_entity_guid, child_entity_guid=self.child_entity_guid, current_entity_guid=self.child_entity_guid)

            # steps.append(Insert_Hierarchical_Relationship(name="transformed inserted hierarchical relationships", parent_entity_guid=self.parent_entity_guid, child_entity_guid=self.child_entity_guid, current_entity_guid=self.child_entity_guid))
            # return Sequence(name="transformed inserted hierarchical relationship", steps = steps)

        elif self.current_entity_guid == self.child_entity_guid:
            child_entity_document = input_data
            child_m4isourcetype = child_entity_document["m4isourcetype"]
            derived_guid, derived_type = get_relevant_hierarchy_entity_fields(child_m4isourcetype[0])

            if derived_guid in conceptual_hierarchical_derived_entity_guid_fields_list:
                index = conceptual_hierarchical_derived_entity_guid_fields_list.index(derived_guid)
                to_be_inserted_derived_guid_fields = conceptual_hierarchical_derived_entity_guid_fields_list[:index]


            if derived_guid in technical_hierarchical_derived_entity_guid_fields_list:
                index = technical_hierarchical_derived_entity_guid_fields_list.index(derived_guid)
                to_be_inserted_derived_guid_fields = technical_hierarchical_derived_entity_guid_fields_list[:index]

            for to_be_inserted_derived_guid_field in to_be_inserted_derived_guid_fields:

                steps.append((UpdateLocalAttributeProcessor(name=f"insert derived entity field {to_be_inserted_derived_guid_field}", key = to_be_inserted_derived_guid_field, value = child_entity_document[to_be_inserted_derived_guid_field])))
                steps.append((UpdateLocalAttributeProcessor(name=f"insert derived entity field {hierarchical_derived_entity_fields_mapping[to_be_inserted_derived_guid_field]}", key = hierarchical_derived_entity_fields_mapping[to_be_inserted_derived_guid_field], value = child_entity_document[hierarchical_derived_entity_fields_mapping[to_be_inserted_derived_guid_field]])))

            breadcrumbguid_prefix = child_entity_document["breadcrumbguid"]
            breadcrumbname_prefix = child_entity_document["breadcrumbname"]
            breadcrumbtype_prefix = child_entity_document["breadcrumbtype"]

            steps.append((InsertPrefixToList(name="update breadcrumb guid", key="breadcrumbguid", input_list=breadcrumbguid_prefix)))
            steps.append((InsertPrefixToList(name="update breadcrumb name", key="breadcrumbname", input_list=breadcrumbname_prefix)))
            steps.append((InsertPrefixToList(name="update breadcrumb type", key="breadcrumbtype", input_list=breadcrumbtype_prefix)))

            return Sequence(name="transformed inserted hierarchical relationships", steps = steps)

        else:
            logging.warning(f"parent entity guid: {self.parent_entity_guid}, child entity guid: {self.child_entity_guid}, current entity guid: {self.current_entity_guid}")
            raise Exception(f"Unexpected state.")



# always provide a generic name and description
class InsertPrefixToList(AbstractProcessor):
    """InsertPrefixToList is an processor which updates a provided list by inserting a provided input list as prefix
DeletePrefixFromList
    Parameters
    ----------
    name: str
        Name of the processor
    key: str
        This is the name of the field in the app search schema
    new_valueDeletePrefixFromList: str
    """

    def __init__(self,
                name:str,
                key : str,
                input_list: list
                ):
        super().__init__(name)
        self.key = key
        self.input_list = input_list
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:

        if not self.key in input_data.keys():
            raise Exception(f"Key {self.key} not in input data")

        if not type(input_data[self.key] == list):
            raise Exception(f"App search field {self.key} is of unexpected type.")

        input_data[self.key] = self.input_list  + input_data[self.key]

        return input_data

    # end of process

# end of class InsertPrefixToList

class DeletePrefixFromList(AbstractProcessor):
    """DeletePrefixFromList is an processor which updates a provided list by deleting a all entries from list before provided index

    Parameters
    ----------
    name: str
        Name of the processor
    key: str
        This is the name of the field in the app search schema
    new_value: str
    """
    # Charif: I make use of index because names and types are not guaranteed to be unique in all lists

    def __init__(self,
                name:str,
                key : str,
                guid_key: str,
                first_guid_to_keep:str = None
                ):
        super().__init__(name)
        self.key = key
        self.guid_key = guid_key
        self.first_guid_to_keep = first_guid_to_keep
    # end of __init__


    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:
        if not self.key in input_data.keys():
            raise Exception(f"Key {self.key} not in input data")

        if not self.guid_key in input_data.keys():
            raise Exception(f"Guid_Key {self.guid_key} not in input data")

        if self.first_guid_to_keep == None:
            raise Exception(f"first_guid_to_keep {self.first_guid_to_keep} not specified")

        if not isinstance(input_data[self.key],list):
            raise Exception(f"App search field {self.key} is of unexpected type.")

        arr = input_data[self.guid_key]
        try:
            index = arr.index(self.first_guid_to_keep)
            input_data[self.key] = input_data[self.key][index::]
        except:
            input_data[self.key] = []
            pass

        return input_data

    # end of process

# end of class DeletePrefixFromList


# class DeletePrefixFromList2(AbstractProcessor):
#     """DeletePrefixFromList is an processor which updates a provided list by deleting a all entries from list before provided index

#     Parameters
#     ----------
#     name: str
#         Name of the processor
#     key: str
#         This is the name of the field in the app search schema
#     new_value: str
#     """
#     # Charif: I make use of index because names and types are not guaranteed to be unique in all lists

#     def __init__(self,
#                 name:str,
#                 key : str,
#                 # first position you want to keep
#                 index: int,
#                 incremental: bool = False
#                 ):
#         super().__init__(name)
#         self.key = key
#         self.index = index
#         self.incremental = incremental
#     # end of __init__

#     def translate_index(self, input_data: Dict):
#         if self.index == -1:
#             # first position you want to keep
#             self.index = len(input_data[self.key])



#     def process(self, input_data:Dict) -> Dict:
#         if not self.key in input_data.keys():
#             raise Exception(f"Key {self.key} not in input data")

#         if not isinstance(input_data[self.key],list):
#             raise Exception(f"App search field {self.key} is of unexpected type.")

#         self.translate_index(input_data)

#         if (self.index < 0) or (self.index > len(input_data[self.key])):
#             raise Exception(f"Provided index {self.index} is invalid considering the list .")
#         elif self.index == len(input_data[self.key]):
#             input_data[self.key] = []
#         else:
#             input_data[self.key] = input_data[self.index::]

#         if self.incremental:
#             self.index +=1

#         return input_data

#     # end of process

# # end of class DeletePrefixFromList





class ComputeDqScoresProcessor(AbstractProcessor):
    """ComputeDqScoresProcessor is an processor which updates all data quality
    scores of a local instance.

    Parameters
    ----------
    name :str
        Name of the processor
    """
    def __init__(self,
                name:str):
        super().__init__(name)
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:
        child_entity_dicts = get_direct_child_entity_docs(input_data["guid"], make_elastic_app_search_connect())
        input_data = update_dq_score_fields(input_data, child_entity_dicts)
        return input_data
    # end of process

# end of class ComputeDqScoresProcessor



class ResetDqScoresProcessor(AbstractProcessor):
    """ResetDqScoresProcessor is an processor which updates all data quality
    scores of a local instance.

    Parameters
    ----------
    name :str
        Name of the processor
    """
    def __init__(self,
                name:str):
        super().__init__(name)
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:
        for key in input_data.keys():
            if key.startswith("dqscore"):
                input_data[key] = 0
        return input_data

    # end of process

# end of class ResetDqScoresProcessor

class InsertElementInList(AbstractProcessor):
    """InsertElementInList is an processor which updates the a name in breadcrumb
    scores of a local instance.

    Parameters
    ----------
    name :str
        Name of the processor
    """


    def __init__(self,
                name:str,
                key: str,
                index : int,
                value
                ):
        super().__init__(name)
        self.key = key
        self.index = index
        self.value = value
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:

        if not self.key in input_data.keys():
            raise Exception(f"Key {self.key} not in input data")

        if not type(input_data[self.key] == list):

            raise Exception(f"App search field {self.key} is of unexpected type.")

        if (self.index < 0) or (not self.index < len(input_data[self.key])):

            raise Exception(f"Provided index {self.index} is invalid considering the list .")

        input_data[self.key][self.index] = self.value

        # input_data[self.key][:self.index] + self.value + input_data[self.key][self.index:]
        return input_data
    # end of process

# end of class InsertElementInList

class DeleteElementFromList(AbstractProcessor):
    """DeleteElementFromList is an processor which updates the a name in breadcrumb
    scores of a local instance.

    Parameters
    ----------
    name :str
        Name of the processor
    """


    def __init__(self,
                name:str,
                key: str,
                index : int,
                ):
        super().__init__(name)
        self.key = key
        self.index = index
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:

        if not self.key in input_data.keys():
            raise Exception(f"Key {self.key} not in input data")

        if not type(input_data[self.key] == list):

            raise Exception(f"App search field {self.key} is of unexpected type.")


        if (self.index < 0) or (not self.index < len(input_data[self.key])):

            raise Exception(f"Provided index {self.index} is invalid considering the list .")

        del input_data[self.key][self.index]

        return input_data
    # end of process

# end of class DeleteElementFromList

#################################################################

class InheritDerviedEntitiesFromParentEntity(AbstractProcessor):
    """UpdateBreadcrumbValueProcessor is an processor which updates the a name in breadcrumb
    scores of a local instance.

    Parameters
    ----------
    name :str
        Name of the processor
    """
    def __init__(self,
                name:str,
                parent_entity_guid: str,
                ):
        super().__init__(name)
        self.parent_entity_guid = parent_entity_guid
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:
        # input_data[] = self.parent_entity_guid
        # get_document(self.parent_entity_guid, make_elastic_app_search_connect()) # This is bad practice .. think of an alternaive
        pass
       #TODO: do something meaningful here
    #    return input_data
    # end of process

# end of class UpdateBreadcrumbValueProcessor


class DeleteEntityOperator(AbstractProcessor):
    """DeleteEntityOperator is an processor which updates all data quality
    scores of a local instance.

    Parameters
    ----------
    name :str
        Name of the processor
    """
    def __init__(self,
                name:str):
        super().__init__(name)
    # end of __init__

    def process(self, input_data:Dict, app_search: AppSearch) -> Dict:
        return
    # end of process

# end of class DeleteEntityOperator


#################################################################

# class DeleteElemtsFromLists(AbstractProcessor):
#     """DeleteValuesFromLists is an processor which deletes elements from lists corresponding to the provided key set
#     scores of a local instance.

#     Parameters
#     ----------
#     name :str
#         Name of the processor
#     """
#     def __init__(self,
#                 name:str,
#                 element_list_key: list,
#                 key_list: list
#                 ):
#         super().__init__(name)
#         self.element_list_key = element_list_key
#         self.key_list = key_list
#     # end of __init__

#     def process(self, input_data:Dict) -> Dict:
#         for key in self.key_list:
#             for element in self.element_list_key:
#                 if element in input_data[key]:
#                     # input_data[key].remove(element)
#                     DeleteListEntryBasedOnUniqueValueList(name=f"update derived entity field", unique_list_key=key, target_list_key=derived_type, unique_value=value)

#         return input_data

#     # end of process

# # end of class DeleteValuesFromLists

