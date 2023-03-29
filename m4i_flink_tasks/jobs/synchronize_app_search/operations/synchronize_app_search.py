from elasticsearch import Elasticsearch
from pyflink.datastream import MapFunction

from ....model import EntityMessage
from .event_handlers import EVENT_HANDLERS, EventHandlerContext


class SynchronizeAppSearch(MapFunction, EventHandlerContext):

    def __init__(self, elastic: Elasticsearch, index_name: str):
        self.elastic = elastic
        self.index_name = index_name
    # END __init__

    def map(self, value: str) -> str:
        entity_message = EntityMessage.from_json(value)
        return entity_message.to_json()
    # END map
# END SynchronizeAppSearch
