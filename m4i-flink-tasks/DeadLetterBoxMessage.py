
from dataclasses import dataclass, field
from tokenize import Double
from dataclasses_json import (DataClassJsonMixin, LetterCase, config,
                              dataclass_json)
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional
from m4i_atlas_core import AtlasChangeMessage, EntityAuditAction, get_entity_by_guid, Entity

'''
This data class "DeadLetterBoxMessage" describes the structure of a message forwarded to the dead letter box. 
This is a Kafka topic which contains a message for each input notification that could not be handled by any of the jobs implemented.
'''
@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class DeadLetterBoxMesage(DataClassJsonMixin):
    timestamp: Double
    original_notification: str
    job: str
    description: Exception

