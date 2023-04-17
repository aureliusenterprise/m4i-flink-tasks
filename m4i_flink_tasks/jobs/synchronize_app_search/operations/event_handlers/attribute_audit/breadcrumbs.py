from typing import List

from ......model import AppSearchDocument, Breadcrumb, EntityMessage
from ..utils import (EntityDataNotProvidedError, EventHandlerContext,
                     get_documents)


def update_document_breadcrumb(document: AppSearchDocument, guid: str, name: str):
    breadcrumb = Breadcrumb.from_app_search_document(document)

    breadcrumb.metadata[guid].name = name

    guids, names, type_names = breadcrumb.serialize()

    document.breadcrumbguid = guids
    document.breadcrumbname = names
    document.breadcrumbtype = type_names
# END update_document_breadcrumb


def handle_update_breadcrumbs(message: EntityMessage, context: EventHandlerContext) -> List[AppSearchDocument]:

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

    query = {
        "query": {"match_all": {}},
        "filters": {
            "breadcrumb_guid": [entity_details.guid]
        }
    }

    documents = get_documents(query, context)

    for document in documents:
        update_document_breadcrumb(document, entity_details.guid, entity_name)
    # END LOOP

    return documents
# END handle_update_breadcrumbs
