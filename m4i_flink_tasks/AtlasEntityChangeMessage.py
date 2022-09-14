
from dataclasses import dataclass, field
from dataclasses_json import (DataClassJsonMixin, LetterCase, dataclass_json)
from dataclasses import dataclass, field
from typing import List
from m4i_atlas_core import EntityAuditAction, Entity



@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class EntityMessage(DataClassJsonMixin):

    type_name: str
    qualified_name: str
    guid: str
    msg_creation_time: int
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


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class AtlasEntityChangeMessageBody(DataClassJsonMixin):
    entity: EntityMessage

@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class AtlasEntityChangeMessage(DataClassJsonMixin):

    message: AtlasEntityChangeMessageBody

