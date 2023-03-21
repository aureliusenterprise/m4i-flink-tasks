from typing import Dict, List, Optional

from m4i_atlas_core import AtlasChangeMessage, Entity, RelationshipAttribute

from ......model import EntityMessage, EntityMessageType


def handle_attribute_changes(change_message: AtlasChangeMessage, previous: Entity, current: Entity) -> Optional[EntityMessage]:
    """
    Process attribute changes between two entity versions and generate an EntityMessage if changes are found.

    Args:
        change_message (AtlasChangeMessage): The AtlasChangeMessage containing information about the entity change.
        previous (Entity): The previous version of the entity.
        current (Entity): The current version of the entity.

    Returns:
        Optional[EntityMessage]: An EntityMessage containing attribute change information or None if there are no attribute changes.
    """

    attributes_dict = current.attributes.to_dict()
    previous_attributes_dict = previous.attributes.to_dict()

    inserted_attributes = [
        key for key in attributes_dict.keys()
        if key not in previous_attributes_dict
    ]

    deleted_attributes = [
        key for key in previous_attributes_dict.keys()
        if key not in attributes_dict
    ]

    changed_attributes = [
        key for key in attributes_dict.keys()
        if key in previous_attributes_dict
        and attributes_dict[key] != previous_attributes_dict[key]
    ]

    if not any(inserted_attributes) and not any(deleted_attributes) and not any(changed_attributes):
        return None

    entity_message = EntityMessage.from_change_message(
        change_message=change_message,
        event_type=EntityMessageType.ENTITY_ATTRIBUTE_AUDIT
    )

    entity_message.old_value = previous
    entity_message.new_value = current

    entity_message.inserted_attributes = inserted_attributes
    entity_message.deleted_attributes = deleted_attributes
    entity_message.changed_attributes = changed_attributes

    return entity_message
# END handle_attribute_changes


def get_relationships_diff(a: Entity, b: Entity) -> Dict[List[RelationshipAttribute]]:
    """
    Get the difference in relationships between two Entity objects.

    This function compares the relationships of two Entity objects and returns
    a dictionary with relationship attributes and their differences, specifically
    the relationships present in `b` but not in `a`.

    Args:
        a (Entity): The first Entity object to compare.
        b (Entity): The second Entity object to compare.

    Returns:
        Dict[str, List[RelationshipAttribute]]: A dictionary containing the differences in
        relationships between the two Entity objects. The keys represent the relationship
        attributes, and the values are lists of Relationship objects found in `b`
        but not in `a`.
    """

    a_relationships = {
        relationship.guid
        for relationships in a.relationship_attributes.values()
        for relationship in relationships
    }

    return {
        [attribute]: [
            relationship
            for relationship in relationships
            if relationship.guid not in a_relationships
        ]
        for attribute, relationships in b.relationship_attributes.items()
        if isinstance(relationships, list)
    }
# END get_relationships_diff


def handle_relationship_changes(change_message: AtlasChangeMessage, previous: Entity, current: Entity) -> Optional[EntityMessage]:
    """
    Process relationship changes between two entity versions and generate an EntityMessage if changes are found.

    Args:
        change_message (AtlasChangeMessage): The AtlasChangeMessage containing information about the entity change.
        previous (Entity): The previous version of the entity.
        current (Entity): The current version of the entity.

    Returns:
        Optional[EntityMessage]: An EntityMessage containing relationship change information or None if there are no relationship changes.
    """

    inserted_relationships = get_relationships_diff(previous, current)
    deleted_relationships = get_relationships_diff(current, previous)

    has_inserted_relationships = any(
        any(relationships)
        for relationships in inserted_relationships.values()
    )

    has_deleted_relationships = any(
        any(relationships)
        for relationships in deleted_relationships.values()
    )

    if not has_inserted_relationships and not has_deleted_relationships:
        return None
    # END IF

    entity_message = EntityMessage.from_change_message(
        change_message=change_message,
        event_type=EntityMessageType.ENTITY_RELATIONSHIP_AUDIT
    )

    entity_message.old_value = previous
    entity_message.new_value = current

    entity_message.inserted_relationships = inserted_relationships
    entity_message.deleted_relationships = deleted_relationships

    return entity_message
# END handle_relationship_changes


def handle_update_operation(change_message: AtlasChangeMessage) -> List[EntityMessage]:
    """
    Handles an update operation by processing attribute and relationship changes between two entity versions,
    and returns a list of EntityMessages corresponding to the changes found.

    Args:
        change_message (AtlasChangeMessage): The AtlasChangeMessage containing information about the entity change.

    Returns:
        List[EntityMessage]: A list of EntityMessages containing attribute and relationship change information.
    """

    entity = change_message.message.entity
    previous_entity = change_message.previouse_version

    messages: List[EntityMessage] = []

    attribute_audit_message = handle_attribute_changes(
        change_message=change_message,
        previous=previous_entity,
        current=entity
    )

    if attribute_audit_message is not None:
        messages.append(attribute_audit_message)
    # END IF

    relationship_audit_message = handle_relationship_changes(
        change_message=change_message,
        previous=previous_entity,
        current=entity
    )

    if relationship_audit_message is not None:
        messages.append(relationship_audit_message)
    # END IF

    return messages
# END handle_update_operation
