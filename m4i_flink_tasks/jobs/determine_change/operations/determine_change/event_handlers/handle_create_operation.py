from typing import List

from m4i_atlas_core import AtlasChangeMessage

from ......model import EntityMessage, EntityMessageType


def handle_create_operation(change_message: AtlasChangeMessage) -> List[EntityMessage]:
    """
    Process the create operation for an entity and generate an EntityMessage.

    Args:
        change_message (AtlasChangeMessage): The AtlasChangeMessage containing information about the entity create operation.

    Returns:
        List[EntityMessage]: A list containing a single EntityMessage with information about the inserted attributes and relationships.
    """

    entity = change_message.message.entity

    attributes_dict = entity.attributes.to_dict()
    inserted_attributes = list(attributes_dict.keys())

    entity_message = EntityMessage.from_change_message(
        change_message=change_message,
        event_type=EntityMessageType.ENTITY_CREATED
    )

    entity_message.inserted_attributes = inserted_attributes
    entity_message.inserted_relationships = entity.relationship_attributes
    entity_message.new_value = entity

    return [entity_message]
# END handle_create_operation
