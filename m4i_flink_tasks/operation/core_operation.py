from abc import ABC
import pandas as pd
import numpy as np
import math
import datetime
from typing import List, Dict, Optional
import jsonpickle
#from datetime import datetime, timedelta
import traceback
import sys
import json
import asyncio

from m4i_flink_tasks.synchronize_app_search.elastic import get_document
from ..synchronize_app_search import get_direct_child_entity_docs, make_elastic_app_search_connect, update_dq_score_fields
from ..synchronize_app_search import get_super_types_names,get_m4i_source_types,get_source_type
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

    def process(self, input_data:Dict) -> Dict:
        return {}
    # end of process

# end of class AbstractProcessor

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
    # end of __init__

    def process(self, input_data:Dict) -> Dict:        
        super_types = asyncio.run( get_super_types_names(self.entity_type))
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

    def process(self, input_data:Dict) -> Dict:

        index = input_data[self.unqiue_list_key].index(self.unqiue_value)
        input_data = InsertElementInList(name=self.name, key=self.target_list_key, index = index, value=self.target_value).process(input_data)
        
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

    def process(self, input_data:Dict) -> Dict:

        index = input_data[self.unqiue_list_key].index(self.unqiue_value)
        input_data = DeleteElementFromList(name=self.name, key=self.target_list_key, index = index).process(input_data)
        
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

    def process(self, input_data:Dict) -> Dict:
       input_data[self.key] = self.value
       return input_data
    # end of process

# end of class UpdateLocalAttributeProcessor

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

    def process(self, input_data:Dict) -> Dict:
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

    def process(self, input_data:Dict) -> Dict:
       #TODO: do something meaningful here 
       return input_data
    # end of process

# end of class UpdateDqScoresProcessor




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

    def process(self, input_data:Dict) -> Dict:
        #logging = input_data["logging"]
        input_data_ = input_data
        for step in self.steps:
            #logging.append({"ts":str(datetime.datetime.now()),
            #                "class":"sequence",
            #                "step_name":step.name})
            #print(f"====LOG {str(datetime.datetime.now())},sequence,{step.name}")
            input_data_ = step.process(input_data_)
        return input_data_
    # end of proces

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

    def run(self, input_data:Dict) -> Dict:
        data = self.specification.process(input_data)
        return data
    # end of run
# end of class WorkflowEngine


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
                old_value : str,  
                new_value : str
                ):
        super().__init__(name)
        self.key = key
        self.old_value = old_value
        self.new_value = new_value

    # end of __init__

    def process(self, input_data:Dict) -> Dict:
        if not self.key in input_data.keys():
            raise Exception(f"Key {self.key} not in input data")
        
        if not self.old_value in input_data[self.key]:
        # raise Exception(f"Value {self.old_value} not in input data")  # I commented this out, so no exception is failed when the breadcrumbname is updated.
            pass  
        index = input_data[self.key].index(self.old_value)
        input_data[index] = self.new_value
        return input_data
    # end of process

# end of class UpdateListEntryProcessor

class DeleteListEntryProcessor(AbstractProcessor):
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
                value : str,  
                ):
        super().__init__(name)
        self.key = key
        self.value = value

    # end of __init__

    def process(self, input_data:Dict) -> Dict:
        if not self.key in input_data.keys():
            raise Exception(f"Key {self.key} not in input data")
        
        if not self.value in input_data[self.key]:
        # raise Exception(f"Value {self.old_value} not in input data")  # I commented this out, so no exception is failed when the breadcrumbname is updated.
            pass  
        input_data[self.key].remove(self.value)
        return input_data
    # end of process

# end of class UpdateListEntryProcessor




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

    def process(self, input_data:Dict) -> Dict:

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
                index: list,
                incremental: bool = False
                ):
        super().__init__(name)
        self.key = key
        self.index = index
        self.incremental = incremental
    # end of __init__

    def translate_index(self, input_data: Dict):
        if self.index == -1:
            self.index = len(input_data[self.key])
           
    

    def process(self, input_data:Dict) -> Dict:

        

        if not self.key in input_data.keys():
            raise Exception(f"Key {self.key} not in input data")

        if not type(input_data[self.key] == list):

            raise Exception(f"App search field {self.key} is of unexpected type.")

        self.translate_index(input_data)

        if (not self.index < 0) or (not self.index < len(input_data[self.key])):
            
            raise Exception(f"Provided index {self.index} is invalid considering the list .")

        input_data[self.key] = input_data[self.index+1::] 

        if self.incremental:
            self.index +=1

        return input_data

    # end of process

# end of class DeletePrefixFromList


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

    def process(self, input_data:Dict) -> Dict:
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

    def process(self, input_data:Dict) -> Dict:
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

    def process(self, input_data:Dict) -> Dict:

        if not self.key in input_data.keys():
            raise Exception(f"Key {self.key} not in input data")

        if not type(input_data[self.key] == list):

            raise Exception(f"App search field {self.key} is of unexpected type.")

        if (not self.index < 0) or (not self.index < len(input_data[self.key])):
            
            raise Exception(f"Provided index {self.index} is invalid considering the list .")

        input_data[self.key][:self.index] + self.value + input_data[self.key][self.index:]
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

    def process(self, input_data:Dict) -> Dict:

        if not self.key in input_data.keys():
            raise Exception(f"Key {self.key} not in input data")

        if not type(input_data[self.key] == list):

            raise Exception(f"App search field {self.key} is of unexpected type.")


        if (not self.index < 0) or (not self.index < len(input_data[self.key])):
            
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

    def process(self, input_data:Dict) -> Dict:
        # input_data[] = self.parent_entity_guid
        # get_document(self.parent_entity_guid, make_elastic_app_search_connect()) # This is bad practice .. think of an alternaive
        pass
       #TODO: do something meaningful here 
    #    return input_data
    # end of process

# end of class UpdateBreadcrumbValueProcessor

#################################################################

class DeleteElemtsFromLists(AbstractProcessor):
    """DeleteValuesFromLists is an processor which deletes elements from lists corresponding to the provided key set
    scores of a local instance.

    Parameters
    ----------
    name :str
        Name of the processor
    """
    def __init__(self,
                name:str,
                element_list_key: list,
                key_list: list
                ):
        super().__init__(name)
        self.element_list_key = element_list_key
        self.key_list = key_list
    # end of __init__

    def process(self, input_data:Dict) -> Dict:
        for key in self.key_list:
            for element in self.element_list_key:
                if element in input_data[key]:
                    # input_data[key].remove(element)
                    DeleteListEntryBasedOnUniqueValueList(name=f"update derived entity field", unique_list_key=key, target_list_key=derived_type, unique_value=value)

        return input_data
                
    # end of process

# end of class DeleteValuesFromLists

