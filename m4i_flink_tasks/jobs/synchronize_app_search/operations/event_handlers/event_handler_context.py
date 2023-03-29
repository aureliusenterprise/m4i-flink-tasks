from typing import Protocol
from elasticsearch import Elasticsearch

class EventHandlerContext(Protocol):
    """
    EventHandlerContext is a protocol that defines the required properties for an event handler context.

    This protocol is intended to be used as a type hint for objects that should provide an Elasticsearch client
    and an index name to be used by event handlers. Implementing classes should provide the `elastic` and
    `index_name` properties, which are used by event handlers to interact with an Elasticsearch index.
    """

    @property
    def elastic(self) -> Elasticsearch:
        """
        This property should return an instance of the Elasticsearch client.

        Returns:
            Elasticsearch: The Elasticsearch client instance that event handlers will use to interact
                           with an Elasticsearch cluster.
        """

    @property
    def index_name(self) -> str:
        """
        This property should return the name of the Elasticsearch index that event handlers will use.

        Returns:
            str: The name of the Elasticsearch index to be used by event handlers.
        """
