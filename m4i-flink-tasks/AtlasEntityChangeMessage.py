
from dataclasses import dataclass, field
from dataclasses_json import (DataClassJsonMixin, LetterCase, config,
                              dataclass_json)
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Dict
from m4i_atlas_core import AtlasChangeMessage, EntityAuditAction, get_entity_by_guid, Entity



@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class EntityMessage(DataClassJsonMixin):

    type_name: str
    qualified_name: str
    guid: str
    original_event_type: EntityAuditAction
    direct_change: bool
    event_type : str

    inserted_attributes: List[str]
    changed_attributes: List[str]
    deleted_attributes: List[str]

    inserted_relationships: dict
    changed_relationships: dict
    deleted_relationships: dict

    old_value: Entity = field(default_factory=dict)
    new_value: Entity = field(default_factory=dict) 
    # old_value: Entity 
    # new_value: Entity


    # atlas_kafka_notification: AtlasChangeMessage
    
@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class AtlasEntityChangeMessageBody(DataClassJsonMixin):
    entity: EntityMessage

@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class AtlasEntityChangeMessage(DataClassJsonMixin):

    message: AtlasEntityChangeMessageBody

