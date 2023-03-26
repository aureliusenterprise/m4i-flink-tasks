from typing import List

from ......model import (AtlasChangeMessageWithPreviousVersion, EntityMessage,
                         EntityMessageType)


def handle_delete_operation(change_message: AtlasChangeMessageWithPreviousVersion) -> List[EntityMessage]:
    """
    Process the delete operation for an entity and generate an EntityMessage.

    Args:
        change_message (AtlasChangeMessage): The AtlasChangeMessage containing information about the entity delete operation.

    Returns:
        List[EntityMessage]: A list containing a single EntityMessage with information about the deleted attributes and relationships.
    """

    entity = change_message.message.entity

    attributes_dict = entity.attributes.to_dict()
    deleted_attributes = list(attributes_dict.keys())

    entity_message = EntityMessage.from_change_message(
        change_message=change_message,
        event_type=EntityMessageType.ENTITY_DELETED
    )

    entity_message.deleted_attributes = deleted_attributes
    entity_message.deleted_relationships = entity.relationship_attributes
    entity_message.old_value = entity

    return [entity_message]
# END handle_delete_operation
