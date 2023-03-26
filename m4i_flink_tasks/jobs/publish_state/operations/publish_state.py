from typing import Optional

from elasticsearch import Elasticsearch
from m4i_atlas_core import Entity
from pyflink.datastream import MapFunction

from ....model import AtlasChangeMessageWithPreviousVersion


class PublishState(MapFunction):
    def __init__(self, elastic: Elasticsearch, index_name: str):
        self.elastic = elastic
        self.index_name = index_name
    # END __init__()

    def map(self, value: str) -> str:
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
