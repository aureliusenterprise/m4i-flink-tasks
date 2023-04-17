import pytest
from elasticsearch import Elasticsearch
from m4i_atlas_core import EntityAuditAction
from mock import Mock
from pytest_mock import MockerFixture

from ......model import EntityMessage, EntityMessageType
from ..utils import EntityDataNotProvidedError, EventHandlerContext
from .attributes import handle_update_attributes


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
# END mock_event_handler_context


def test__handle_update_attributes(context: EventHandlerContext):
    # Prepare test data
    message = EntityMessage.from_dict({
        "guid": "1234",
        "inserted_attributes": ["definition"],
        "changed_attributes": ["email"],
        "type_name": "test",
        "event_type": EntityMessageType.ENTITY_ATTRIBUTE_AUDIT,
        "original_event_type": EntityAuditAction.ENTITY_UPDATE,
        "new_value": {
            "guid": "1234",
            "type_name": "test",
            "attributes": {
                "definition": "New definition",
                "email": "new.email@example.com"
            }
        }
    })

    # Mock Elasticsearch response
    mock_es_response = {
        "guid": "1234",
        "typename": "test",
        "definition": "Old definition",
        "email": "old.email@example.com",
        "name": "Example",
        "referenceablequalifiedname": "example"
    }

    context.elastic.get.return_value = mock_es_response

    # Call handle_update_attributes function
    updated_documents = handle_update_attributes(message, context)

    # Check the updated AppSearchDocument
    assert len(updated_documents) == 1
    updated_document = updated_documents[0]
    assert updated_document.guid == "1234"
    assert updated_document.typename == "test"
    assert updated_document.definition == "New definition"
    assert updated_document.email == "new.email@example.com"

    # Check if the Elasticsearch get method was called with the correct parameters
    context.elastic.get.assert_called_once_with(
        context.index_name,
        message.guid
    )
# END test__handle_update_attributes


def test__handle_update_attributes_no_relevant_attributes(context: EventHandlerContext):
    message = EntityMessage.from_dict({
        "guid": "1234",
        "inserted_attributes": ["not_relevant_attribute"],
        "changed_attributes": ["another_irrelevant_attribute"],
        "type_name": "test",
        "event_type": EntityMessageType.ENTITY_ATTRIBUTE_AUDIT,
        "original_event_type": EntityAuditAction.ENTITY_UPDATE,
        "new_value": {
            "guid": "1234",
            "type_name": "test",
            "attributes": {
                "definition": "New definition",
                "email": "new.email@example.com"
            }
        }
    })

    # Call handle_update_attributes function
    updated_documents = handle_update_attributes(message, context)

    # Check that no documents were returned
    assert len(updated_documents) == 0

    # Check if the Elasticsearch get method was not called
    context.elastic.get.assert_not_called()
# END test__handle_update_attributes_no_relevant_attributes


def test__handle_update_attributes_no_new_value(context: EventHandlerContext):
    message = EntityMessage.from_dict({
        "guid": "1234",
        "inserted_attributes": ["definition"],
        "changed_attributes": ["email"],
        "type_name": "test",
        "event_type": EntityMessageType.ENTITY_ATTRIBUTE_AUDIT,
        "original_event_type": EntityAuditAction.ENTITY_UPDATE,
        "new_value": None
    })

    # Call handle_update_attributes function and expect an error
    with pytest.raises(EntityDataNotProvidedError):
        handle_update_attributes(message, context)
    # END WITH
# END test__handle_update_attributes_no_new_value
