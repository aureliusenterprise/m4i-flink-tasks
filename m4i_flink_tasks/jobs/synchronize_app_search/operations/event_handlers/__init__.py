from typing import Callable

from .....model import EntityMessage, EntityMessageType
from .attribute_audit import handle_attribute_audit
from .entity_created import handle_entity_created
from .entity_deleted import handle_entity_deleted
from .event_handler_context import EventHandlerContext
from .relationship_audit import handle_relationship_audit

EventHandler = Callable[[EntityMessage, EventHandlerContext], None]

EVENT_HANDLERS = {
    EntityMessageType.ENTITY_ATTRIBUTE_AUDIT: handle_attribute_audit,
    EntityMessageType.ENTITY_CREATED: handle_entity_created,
    EntityMessageType.ENTITY_DELETED: handle_entity_deleted,
    EntityMessageType.ENTITY_RELATIONSHIP_AUDIT: handle_relationship_audit
}
