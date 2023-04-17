from typing import List

from m4i_atlas_core import Entity

from ......model import AppSearchDocument, EntityMessage
from ..utils import EntityDataNotProvidedError, EventHandlerContext


def default_create_handler(entity_details: Entity) -> AppSearchDocument:
    """
    Create an AppSearchDocument instance using the provided entity details.

    :param entity_details: The entity details to extract the necessary attributes from.

    :return: The created AppSearchDocument instance.
    """

    attributes = entity_details.attributes.unmapped_attributes

    result = AppSearchDocument(
        id=entity_details.guid,
        guid=entity_details.guid,
        typename=entity_details.type_name,
        name=attributes.get("name"),
        referenceablequalifiedname=attributes.get("qualifiedName")
    )

    return result
# END default_create_handler


def create_person_handler(entity_details: Entity) -> AppSearchDocument:
    """
    Create an AppSearchDocument instance for a person entity using the provided entity details.

    :param entity_details: The person entity details to extract the necessary attributes from.

    :return: The created AppSearchDocument instance.
    """

    result = default_create_handler(entity_details)

    attributes = entity_details.attributes.unmapped_attributes
    result.email = attributes.get("email")

    return result
# END create_person_handler


ENTITY_CREATED_HANDLERS = {
    "m4i_person": create_person_handler
}


def handle_entity_created(message: EntityMessage, context: EventHandlerContext) -> List[AppSearchDocument]:
    """
    Process the entity creation message and create an AppSearchDocument instance accordingly.

    :param message: The EntityMessage instance containing the entity creation details.
    :param context: The EventHandlerContext instance (unused).

    :return: A list containing the created AppSearchDocument instance.
    """

    entity_details = message.new_value

    if entity_details is None:
        raise EntityDataNotProvidedError(message.guid)
    # END IF

    create_handler = ENTITY_CREATED_HANDLERS.get(
        entity_details.type_name,
        default_create_handler
    )

    return [create_handler(entity_details)]
# END handle_entity_created
