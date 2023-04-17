import pytest
from elasticsearch import Elasticsearch
from m4i_atlas_core import EntityAuditAction
from mock import Mock
from pytest_mock import MockerFixture

from ......model import AppSearchDocument, EntityMessage, EntityMessageType
from ..utils import EntityDataNotProvidedError, EventHandlerContext
from .breadcrumbs import handle_update_breadcrumbs


class MockEventHandlerContext():
    def __init__(self, elastic, index_name):
        self.elastic = elastic
        self.index_name = index_name
    # END __init__
# END MockEventHandlerContext


@pytest.fixture
def mock_elastic(mocker: MockerFixture):
    return mocker.Mock(spec=Elasticsearch)
# END mock_elastic


@pytest.fixture
def context(mock_elastic: Mock):
    return MockEventHandlerContext(elastic=mock_elastic, index_name="test_index")
# END context


@pytest.fixture
def entity_message():
    return EntityMessage.from_dict({
        "guid": "1234",
        "inserted_attributes": ["name"],
        "changed_attributes": [],
        "type_name": "test_type",
        "event_type": EntityMessageType.ENTITY_ATTRIBUTE_AUDIT,
        "original_event_type": EntityAuditAction.ENTITY_UPDATE,
        "new_value": {
            "guid": "1234",
            "type_name": "test_type",
            "attributes": {
                "name": "New Entity Name"
            }
        }
    })
# END entity_message


def test__handle_update_breadcrumbs_no_name_update(context: EventHandlerContext, entity_message: EntityMessage):
    entity_message.inserted_attributes = []
    entity_message.changed_attributes = ["description"]
    updated_documents = handle_update_breadcrumbs(entity_message, context)
    assert len(updated_documents) == 0
# END test__handle_update_breadcrumbs_no_name_update


def test__handle_update_breadcrumbs_no_new_value(context: EventHandlerContext, entity_message: EntityMessage):
    entity_message.new_value = None
    with pytest.raises(EntityDataNotProvidedError):
        handle_update_breadcrumbs(entity_message, context)
    # END WITH
# END test__handle_update_breadcrumbs_no_new_value


def test__handle_update_breadcrumbs_update_document(context: EventHandlerContext, mocker: MockerFixture, entity_message: EntityMessage):
    # Mock a document to update
    document_to_update = AppSearchDocument(
        guid="2345",
        typename="test_type",
        name="Example",
        referenceablequalifiedname="example",
        breadcrumbguid=["1234"],
        breadcrumbname=["Old Entity Name"],
        breadcrumbtype=["test_type"]
    )

    # Mock get_documents function
    mocker.patch(
        "m4i_flink_tasks.jobs.synchronize_app_search.operations.event_handlers.attribute_audit.breadcrumbs.get_documents",
        return_value=[document_to_update]
    )

    # Call handle_update_breadcrumbs function
    updated_documents = handle_update_breadcrumbs(entity_message, context)

    # Check the list of updated documents
    assert len(updated_documents) == 1

    # Check the updated document
    updated_document = updated_documents[0]
    assert updated_document.guid == "2345"
    assert updated_document.typename == "test_type"
    assert updated_document.name == "Example"
    assert updated_document.referenceablequalifiedname == "example"
    assert updated_document.breadcrumbguid == ["1234"]
    assert updated_document.breadcrumbname == ["New Entity Name"]
    assert updated_document.breadcrumbtype == ["test_type"]
# END test__handle_update_breadcrumbs_update_document


def test__handle_update_breadcrumbs_no_documents(context: EventHandlerContext, mocker: MockerFixture, entity_message: EntityMessage):
    # Mock get_documents function
    mocker.patch(
        "m4i_flink_tasks.jobs.synchronize_app_search.operations.event_handlers.attribute_audit.breadcrumbs.get_documents",
        return_value=[]
    )

    # Call handle_update_breadcrumbs function
    updated_documents = handle_update_breadcrumbs(entity_message, context)

    # Check the list of updated documents
    assert len(updated_documents) == 0
# END test__handle_update_breadcrumbs_no_documents
