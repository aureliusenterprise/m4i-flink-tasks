from typing import List

from ......model import AppSearchDocument, EntityMessage
from ..utils import EntityDataNotProvidedError, EventHandlerContext

ATTRIBUTES_WHITELIST = {"definition", "email"}


def handle_update_attributes(message: EntityMessage, context: EventHandlerContext) -> List[AppSearchDocument]:
    """
    Update specified attributes for an entity in the Elasticsearch index.

    Args:
        message (EntityMessage): The message containing the entity's update details, such as
            inserted_attributes, changed_attributes, and new_value.
        context (EventHandlerContext): The context containing Elasticsearch and index name information.

    Returns:
        List[AppSearchDocument]: A list containing the updated AppSearchDocument.

    Raises:
        EntityDataNotProvidedError: If the new_value attribute of the message is not provided.
    """

    attributes_to_update = ATTRIBUTES_WHITELIST & (
        set(message.inserted_attributes) |
        set(message.changed_attributes)
    )

    if len(attributes_to_update) == 0:
        return []
    # END IF

    entity_details = message.new_value

    if entity_details is None:
        raise EntityDataNotProvidedError(message.guid)
    # END IF

    attributes = entity_details.attributes.unmapped_attributes
    result = context.elastic.get(context.index_name, entity_details.guid)

    for attribute in attributes_to_update:
        result[attribute] = attributes.get(attribute)
    # END LOOP

    return [AppSearchDocument.from_dict(result)]
# END handle_update_attributes
