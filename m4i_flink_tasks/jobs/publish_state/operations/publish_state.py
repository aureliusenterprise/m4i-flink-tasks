from typing import Optional

from elasticsearch import Elasticsearch
from m4i_atlas_core import Entity
from pyflink.datastream import MapFunction

from ....model import AtlasChangeMessageWithPreviousVersion


class PublishState(MapFunction):
    """
    A class that implements a PyFlink MapFunction for retrieving the previous version of an entity from Elasticsearch for further processing.

    Attributes:
        elastic (Elasticsearch): An instance of the Elasticsearch client.
        index_name (str): The name of the Elasticsearch index to query.
    """

    def __init__(self, elastic: Elasticsearch, index_name: str):
        """
        Initialize the PublishState class.

        Args:
            elastic (Elasticsearch): An instance of the Elasticsearch client.
            index_name (str): The name of the Elasticsearch index to query.
        """

        self.elastic = elastic
        self.index_name = index_name
    # END __init__

    def map(self, value: str) -> str:
        """
        Process an AtlasChangeMessageWithPreviousVersion and update its 'previous_version' attribute
        with the previous version of the entity retrieved from Elasticsearch.

        Args:
            value (str): A JSON-formatted string that represents the AtlasChangeMessageWithPreviousVersion.

        Returns:
            str: A JSON-formatted string that represents the updated AtlasChangeMessageWithPreviousVersion.
        """

        change_message = AtlasChangeMessageWithPreviousVersion.from_json(value)
        entity = change_message.message.entity

        previous_entity = self.get_previous_atlas_entity(
            guid=entity.guid,
            timestamp=change_message.msg_creation_time
        )

        change_message.previous_version = previous_entity

        return change_message.to_json()
    # END map

    def get_previous_atlas_entity(self, guid: str, timestamp: int) -> Optional[Entity]:
        """
        Retrieve the previous version of an entity from Elasticsearch using its GUID and a timestamp.

        Args:
            guid (str): The GUID of the entity to search for.
            timestamp (int): The timestamp to use as a threshold for finding previous versions.

        Returns:
            Optional[Entity]: The previous version of the entity if found, or None if no previous versions are available.
        """

        query = {
            "bool": {
                "filter": [
                    {
                        "match": {
                            "body.guid.keyword": guid
                        }
                    },
                    {
                        "range": {
                            "msgCreationTime": {
                                "lt": timestamp
                            }
                        }
                    }
                ]
            }
        }

        sort = {
            "msgCreationTime": {"numeric_type": "long", "order": "desc"}
        }

        result = self.elastic.search(
            index=self.index_name,
            query=query,
            sort=sort,
            size=1
        )

        hits = result["hits"]

        # No previous versions of this entity available in the index
        if hits["total"] == 0:
            return None
        # END IF

        doc = hits["hits"][0]["_source"]["body"]
        entity = Entity.from_dict(doc)

        return entity
    # END get_previous_atlas_entity
# END PublishState
