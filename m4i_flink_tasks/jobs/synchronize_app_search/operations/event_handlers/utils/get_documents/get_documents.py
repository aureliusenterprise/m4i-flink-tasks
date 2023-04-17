from typing import Generator

from elasticsearch.helpers import scan

from .......model import AppSearchDocument
from ..event_handler_context import EventHandlerContext


def get_documents(query: dict, context: EventHandlerContext) -> Generator[AppSearchDocument, None, None]:
    """
    Retrieve documents from Elasticsearch using the given query and context.

    Args:
        query (dict): The Elasticsearch query to use when searching for documents.
        context (EventHandlerContext): The context containing Elasticsearch and index name information.

    Returns:
        Generator[AppSearchDocument, None, None]: A generator that yields AppSearchDocument instances.
    """

    search_results = scan(
        context.elastic,
        index=context.index_name,
        query=query
    )

    documents = map(
        AppSearchDocument.from_dict,
        (result["_source"] for result in search_results)
    )

    yield from documents
# END get_documents
