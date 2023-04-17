from typing import Callable, List

from .....model import AppSearchDocument, EntityMessage, EntityMessageType
from .attribute_audit import (handle_update_attributes,
                              handle_update_breadcrumbs,
                              handle_update_derived_entities)
from .entity_created import handle_entity_created
from .entity_deleted import handle_entity_deleted
from .relationship_audit import handle_relationship_audit
from .utils import EventHandlerContext

EventHandler = Callable[
    [EntityMessage, EventHandlerContext],
    List[AppSearchDocument]
]

EVENT_HANDLERS = {
    EntityMessageType.ENTITY_ATTRIBUTE_AUDIT: [handle_update_attributes, handle_update_breadcrumbs, handle_update_derived_entities],
    EntityMessageType.ENTITY_CREATED: [handle_entity_created],
    EntityMessageType.ENTITY_DELETED: [handle_entity_deleted],
    EntityMessageType.ENTITY_RELATIONSHIP_AUDIT: [handle_relationship_audit]
}
