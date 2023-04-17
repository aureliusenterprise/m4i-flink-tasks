from typing import List

from ......model import AppSearchDocument, EntityMessage
from ..utils import EventHandlerContext


def handle_entity_deleted(message: EntityMessage, context: EventHandlerContext) -> List[AppSearchDocument]:
    return []
# END handle_entity_deleted
