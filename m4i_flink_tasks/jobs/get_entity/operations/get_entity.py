import asyncio

from m4i_atlas_core import (AtlasChangeMessage, get_entity_by_guid,
                            get_keycloak_token)
from pyflink.datastream.functions import MapFunction


async def get_entity_details(kafka_notification: str):
    """
    Asynchronously retrieves the details of an entity based on the given Kafka notification.

    Args:
        kafka_notification (str): A JSON-formatted string containing the Kafka notification.

    Returns:
        str: A JSON-formatted string containing the updated AtlasChangeMessage with entity details.
    """

    change_message = AtlasChangeMessage.from_json(kafka_notification)
    entity = change_message.message.entity

    access_token = get_keycloak_token()

    # The cache_read keyword argument is available through the @cached decorator for get_entity_by_guid.
    # Source: https://aiocache.aio-libs.org/en/latest/decorators.html#cached
    # pylint: disable=unexpected-keyword-arg
    entity_details = await get_entity_by_guid(
        guid=entity.guid,
        access_token=access_token,
        cache_read=False
    )

    change_message.message.entity = entity_details

    return change_message.to_json()
# END get_entity_details


class GetEntity(MapFunction):
    """
    A PyFlink MapFunction class that asynchronously retrieves the details of an entity
    based on a Kafka notification and returns the updated AtlasChangeMessage with entity details.
    """

    def map(self, value: str):
        """
        Maps the input Kafka notification to the updated AtlasChangeMessage with entity details.

        Args:
            value (str): A JSON-formatted string containing the Kafka notification.

        Returns:
            str: A JSON-formatted string containing the updated AtlasChangeMessage with entity details.
        """

        entity_details_co = get_entity_details(value)

        return asyncio.run(entity_details_co)
    # END map
# END GetEntity
