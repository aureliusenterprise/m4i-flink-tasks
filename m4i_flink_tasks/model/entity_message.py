from dataclasses import dataclass, field
from enum import Enum
from time import time
from typing import List, Optional

from dataclasses_json import DataClassJsonMixin, LetterCase, dataclass_json
from m4i_atlas_core import AtlasChangeMessage, Entity, EntityAuditAction


class EntityMessageType(Enum):
    ENTITY_ATTRIBUTE_AUDIT = "EntityAttributeAudit"
    ENTITY_CREATED = "EntityCreated"
    ENTITY_DELETED = "EntityDeleted"
    ENTITY_RELATIONSHIP_AUDIT = "EntityRelationshipAudit"
# END EntityMessageType


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class EntityMessage(DataClassJsonMixin):

    type_name: str
    guid: str
    original_event_type: EntityAuditAction
    event_type: EntityMessageType

    inserted_attributes: List[str] = field(default_factory=list)
    changed_attributes: List[str] = field(default_factory=list)
    deleted_attributes: List[str] = field(default_factory=list)

    inserted_relationships: dict = field(default_factory=dict)
    changed_relationships: dict = field(default_factory=dict)
    deleted_relationships: dict = field(default_factory=dict)

    direct_change: bool = True
    msg_creation_time: int = field(default_factory=time)
    old_value: Optional[Entity] = None
    new_value: Optional[Entity] = None

    @classmethod
    def from_change_message(cls, change_message: AtlasChangeMessage, event_type: EntityMessageType):
        entity = change_message.message.entity

        return cls(
            event_type=event_type,
            guid=entity.guid,
            original_event_type=change_message.message.operation_type,
            type_name=entity.type_name
        )
    # END from_change_message
# END EntityMessage
