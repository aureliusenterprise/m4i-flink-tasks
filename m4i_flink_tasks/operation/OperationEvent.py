# -*- coding: utf-8 -*-


from dataclasses import dataclass
from dataclasses_json import DataClassJsonMixin, LetterCase, dataclass_json
from typing import List, Dict
import logging
import jsonpickle
import json

from m4i_flink_tasks.synchronize_app_search.elastic import (
    get_child_entity_guids, make_elastic_app_search_connect)

from m4i_flink_tasks.operation import Delete_Hierarchical_Relationship, Insert_Hierarchical_Relationship
from m4i_flink_tasks.operation.core_operation import Sequence
@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class OperationChange(DataClassJsonMixin):
    propagate: bool
    propagate_down: bool
    
    operation: dict

    def transform(self, input_data, app_search, entity_guid, app_search_engine_name) -> Dict:
        result = {}
        operation_dict = {}

        if self.propagate:
            propagate_ids = None
            if self.propagate_down:
                # propagae downwards
                seq = jsonpickle.decode(json.dumps(self.operation))
                for step in seq.steps:
                    translated_seq = step.transform(input_data, app_search)

                    if isinstance(translated_seq, Delete_Hierarchical_Relationship):
                        id_ = translated_seq.child_entity_guid
                        if id_ not in operation_dict.keys():
                            operation_dict[id_] = [translated_seq]
                        else:
                            operation_dict[id_].append(translated_seq)     

                    elif isinstance(translated_seq, Insert_Hierarchical_Relationship):
                        id_ = translated_seq.child_entity_guid
                        if id_ not in operation_dict.keys():
                            operation_dict[id_] = [translated_seq]
                        else:
                            operation_dict[id_].append(translated_seq)
                    
                    else:
                        retry = 0
                        while retry<3 and propagate_ids==None:
                            try:
                                
                                propagate_ids = get_child_entity_guids(entity_guid=entity_guid,
                                                            app_search=app_search,
                                                            engine_name=app_search_engine_name)
                                logging.info(f"derived ids to be propagated: {propagate_ids}")
                            except Exception as e:
                                logging.error("connection to app search could not be established "+str(e))
                                app_search = make_elastic_app_search_connect()
                            retry = retry+1

                        if propagate_ids==None:
                            raise Exception(f"Could not find document with guid {entity_guid} ")


                        for id_ in propagate_ids:
                            if id_ not in operation_dict.keys():
                                operation_dict[id_] = [translated_seq]
                            else:
                                operation_dict[id_].append(translated_seq)  

                for id_ in operation_dict.keys():
                    
                    seq = Sequence(name="propagated downwards operation", steps = operation_dict[id_])
                    spec = jsonpickle.encode(seq) 
                    oc = OperationChange(propagate=True, propagate_down=True, operation = json.loads(spec))

                    result[id_] = oc
            else:
                # propagate upwards
                propagate_ids = []
                breadcrumbguid = input_data['breadcrumbguid']
                if isinstance(breadcrumbguid,list) and len(breadcrumbguid)>0:
                    propagate_ids = [breadcrumbguid[-1]]
        # end of if change.propagate
        return result        




@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class OperationEvent(DataClassJsonMixin): 
    id: str
    creation_time: int
    entity_guid: str
    
    changes: List[OperationChange]

# import uuid
# import datetime

# oc = OperationChange(propagate=True, propagate_down=True, operation = {"hello": "workld"})
# ocj = oc.to_json()

# oe = OperationEvent(id=str(uuid.uuid4()), 
#                     creation_time=int(datetime.datetime.now().timestamp()*1000),
#                     entity_guid="d56db187-2627-41a6-8698-f74d4b76227e",
#                     changes=[oc])
# oej = oe.to_json()

# oe2 = OperationEvent.from_json(oej)

# print(oe2)
