from typing import List

from ......model import AppSearchDocument, EntityMessage
from ..utils import EventHandlerContext


def handle_relationship_audit(message: EntityMessage, context: EventHandlerContext) -> List[AppSearchDocument]:
    return []
# END handle_relationship_audit
