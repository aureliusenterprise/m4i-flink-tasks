# -*- coding: utf-8 -*-


from dataclasses import dataclass
from dataclasses_json import DataClassJsonMixin, LetterCase, dataclass_json
from typing import List


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class OperationChange(DataClassJsonMixin):
    propagate: bool
    propagate_down: bool
    
    operation: dict


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
