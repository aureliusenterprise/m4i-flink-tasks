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
from ..synchronize_app_search import get_direct_child_entity_docs, make_elastic_app_search_connect, update_dq_score_fields
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
                entity_guid:str):
        super().__init__(name)
        self.entity_guid = entity_guid
        self.entity_type = entity_type
    # end of __init__

    def process(self, input_data:Dict) -> Dict:        
        new_data = {}
        new_data["id"] = self.entity_guid
        new_data["guid"] = self.entity_guid
        new_data["typename"] = self.entity_type
        return new_data
    # end of process

# end of class CreateLocalEntityProcessor

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
       del input_data[self.key] 
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


# always provide a generic name and description 
class InsertPrefixToList(AbstractProcessor):
    """InsertPrefixToList is an processor which updates a provided list by inserting a provided input list as prefix

    Parameters
    ----------
    name: str
        Name of the processor
    key: str
        This is the name of the field in the app search schema
    new_value: str 
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
                index: list
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

        input_data[self.key] = input_data[self.index+1::] 

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

class UpdateDerivedEntityOfChildEntities(AbstractProcessor):
    """UpdateBreadcrumbValueProcessor is an processor which updates the a name in breadcrumb
    scores of a local instance.

    Parameters
    ----------
    name :str
        Name of the processor
    """
    def __init__(self,
                name:str,
                key: str,
                value : Optional[str],
                index : int,
                operation 
                ):
        super().__init__(name)
    # end of __init__

    def process(self, input_data:Dict) -> Dict:

       #TODO: do something meaningful here 
       return input_data
    # end of process

# end of class UpdateBreadcrumbValueProcessor