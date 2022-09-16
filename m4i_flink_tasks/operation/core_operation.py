from abc import ABC
import pandas as pd
import numpy as np
import math
import datetime
from typing import List, Dict
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
