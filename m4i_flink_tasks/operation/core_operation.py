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


###################################################################################

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
                old_value : str,  # Charif: I could also provide the index and the new value only...
                new_value : str
                ):
        super().__init__(name)
        self.key = key
        self.old_value = old_value
        self.new_value = new_value


    # end of __init__
    def process(self, input_data:Dict) -> Dict:
        # check whether the key and old value exists in the input data 
        index = input_data[self.key].index(self.old_value)
        input_data[index] = self.new_value
        return input_data
    # end of process

# end of class UpdateListEntryProcessor


# always provide a generic name and description 
class InsertPrefixBreadcrumbProcessor(AbstractProcessor):
    """InsertPrefixBreadcrumbProcessor is an processor which updates the a name in breadcrumb
    scores of a local instance.

    Parameters
    ----------
    name: str
        Name of the processor
    key: str
        This is the name of the field in the app search schema: breadcrumbname (is this wise to add or unneccessary?)
    old_value: str
        This is the value to be changed in the provided field
    new_value: str 
    """
    
    def __init__(self,
                name:str, 
                key : str,
    
                ):
        super().__init__(name)
        self.key = key
    # end of __init__

    def process(self, input_data:Dict, parent_entity_document : Dict) -> Dict:

        # option A 
        
        # input_data["breadcrumbguids"] = parent_entity_document["breadcrumbguids"] + input_data["breadcrumbguids"] # is this ok? 
        # input_data["breadcrumbname"] =  parent_entity_document["breadcrumbname"] + input_data["breadcrumbname"]
        # input_data["breadcrumbtype"] =  parent_entity_document["breadcrumbtype"] + input_data["breadcrumbtype"] 


        # option B keep this option
        # in this case I would have to call this function several times 
        input_data[self.key] = parent_entity_document[self.key]  + input_data[self.key] 

       
    # option C 
    def process(self, input_data: Dict, prefix: List) -> Dict:

  
        input_data[self.key] =  prefix + input_data[self.key] 

        

        return input_data
    # end of process

# end of class InsertPrefixBreadcrumbProcessor

class DeletePrefixBreadcrumbProcessor(AbstractProcessor):

    
    """DeletePrefixBreadcrumbProcessor is an processor which updates the a name in breadcrumb
    scores of a local instance.

    Parameters
    ----------
    name: str
        Name of the processor
    key: str
        This is the name of the field in the app search schema: breadcrumbname (is this wise to add or unneccessary?)
    old_value: str
        This is the value to be changed in the provided field
    new_value: str 
    """
    
    def __init__(self,
        name:str, 
        key : str,

        ):
        super().__init__(name)
        self.key = key
    # end of __init__


    def  process(self, input_data: Dict, parent_entity_document: Dict) -> Dict:
        guid_index = input_data["breadcrumbguid"].index(parent_entity_document["guid"])
        input_data["breadcrumbguids"] = input_data["breadcrumbguids"][guid_index::] 
        input_data["breadcrumbname"] = input_data["breadcrumbname"][guid_index::] 
        input_data["breadcrumbtype"] = input_data["breadcrumbtype"][guid_index::] 

        return input_data
    # end of process

# end of class DeletePrefixBreadcrumbProcessor


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


class ComputeDqScoresProcessor(AbstractProcessor):
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

    def process(self, input_data:Dict, child_entity_docs: List) -> Dict:
        # TODO: add semantics
        # for child_entity_doc in child_entity_docs:
        #     child_entity_doc[]
        return input_data
    # end of process

# end of class ComputeDqScoresProcessor

class ResetDqScoresProcessor(AbstractProcessor):
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
        for key in input_data.keys:
            if key.startswith("dqscore"):
                input_data[key] = 0
        return input_data
        
    # end of process

# end of class UpdateDqScoresProcessor