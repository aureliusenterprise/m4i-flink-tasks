from typing import List

from ......model import AppSearchDocument, EntityMessage
from ..utils import (EntityDataNotProvidedError, EventHandlerContext,
                     get_documents, update_related_name_by_guid)


def create_handler_for_relationship(relationship_name: str):
    """
    Create a handler for updating derived entities with the specified relationship.

    Args:
        relationship_name (str): The relationship name used to build the handler.

    Returns:
        callable: A function that takes entity_guid, entity_name, and context as
        input and returns a list of updated AppSearchDocument instances.
    """

    relationship_attribute_guid = f"derived{relationship_name}guid"
    relationship_attribute_name = f"derived{relationship_name}"

    def handle_derived_entities_update(
        entity_guid: str,
        entity_name: str,
        context: EventHandlerContext
    ) -> List[AppSearchDocument]:
        """
        Update derived entities based on a specified relationship.

        Args:
            entity_guid (str): The GUID of the entity being updated.
            entity_name (str): The new name of the entity.
            context (EventHandlerContext): The context containing Elasticsearch
            and index name information.

        Returns:
            List[AppSearchDocument]: A list of updated AppSearchDocument instances.
        """

        query = {
            "query": {"match_all": {}},
            "filters": {
                relationship_attribute_guid: [entity_guid]
            }
        }

        documents = get_documents(query, context)

        for document in documents:
            # The query guarantees that the field referenced by relationship_attribute_guid is
            # present in the document. No need for try/except block to handle a potential KeyError.
            guids: List[str] = getattr(
                document,
                relationship_attribute_guid
            )

            names: List[str] = getattr(
                document,
                relationship_attribute_name
            )

            new_names = update_related_name_by_guid(
                guids=guids,
                names=names,
                guid=entity_guid,
                name=entity_name
            )

            setattr(
                document,
                relationship_attribute_name,
                new_names
            )
        # END LOOP

        return documents
    # END handle_derived_entities_update

    return handle_derived_entities_update
# END create_handler_for_relationship


RELATIONSHIP_MAP = {
    "m4i_data_domain": ["dataentity"],
    "m4i_data_entity": ["datadomain", "dataattribute", "dataset"],
    "m4i_data_attribute": ["dataentity", "field"],
    "m4i_field": ["dataattribute", "dataset"],
    "m4i_dataset": ["field", "collection", "dataentity"],
    "m4i_collection": ["dataset", "system"],
    "m4i_system": ["collection"]
}

DERIVED_ENTITY_UPDATE_HANDLERS = {
    entity_type: [
        create_handler_for_relationship(relationship)
        for relationship in relationships
    ]
    for entity_type, relationships in RELATIONSHIP_MAP.items()
}


def handle_update_derived_entities(message: EntityMessage, context: EventHandlerContext):
    """
    Update derived entities based on changes in the provided message.

    Args:
        message (EntityMessage): The entity message containing the details of the updated entity.
        context (EventHandlerContext): The context containing Elasticsearch and index name information.

    Returns:
        List[AppSearchDocument]: A list of updated AppSearchDocument instances.
    """

    updated_attributes = (
        set(message.inserted_attributes) |
        set(message.changed_attributes)
    )

    if not "name" in updated_attributes:
        return []
    # END IF

    entity_details = message.new_value

    if entity_details is None:
        raise EntityDataNotProvidedError(message.guid)
    # END IF

    entity_name = entity_details.attributes.unmapped_attributes.get("name")
    entity_type = entity_details.type_name

    handlers = DERIVED_ENTITY_UPDATE_HANDLERS.get(entity_type, [])

    return [
        entity
        for handler in handlers
        for entity in handler(entity_details.guid, entity_name, context)
    ]
# END handle_update_derived_entities
