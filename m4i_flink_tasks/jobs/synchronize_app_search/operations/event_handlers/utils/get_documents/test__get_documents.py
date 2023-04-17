from mock import Mock, patch
import pytest
from elasticsearch import Elasticsearch
from pytest_mock import MockerFixture

from .......model import AppSearchDocument
from ..event_handler_context import EventHandlerContext
from .get_documents import get_documents


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


def test__get_documents(context: EventHandlerContext):
    mock_scan_results = [
        {
            "_source": {
                "guid": "1234",
                "name": "test1",
                "referenceablequalifiedname": "test1",
                "typename": "test"
            }
        },
        {
            "_source": {
                "guid": "2345",
                "name": "test2",
                "referenceablequalifiedname": "test2",
                "typename": "test"
            }
        },
    ]

    with patch("m4i_flink_tasks.jobs.synchronize_app_search.operations.event_handlers.utils.get_documents.get_documents.get_documents.scan", return_value=mock_scan_results) as mock_scan:
        query = {"query": {"match_all": {}}}

        expected_documents = [
            AppSearchDocument(
                guid="1234",
                name="test1",
                referenceablequalifiedname="test1",
                typename="test"
            ),
            AppSearchDocument(
                guid="2345",
                name="test2",
                referenceablequalifiedname="test2",
                typename="test"
            )
        ]

        # Call the get_documents function and store the generator object
        generator = get_documents(query, context)

        # Iterate over the expected documents and generator
        assert all(
            actual_doc == expected_doc
            for expected_doc, actual_doc
            in zip(expected_documents, generator)
        )

        # Test that the generator stops yielding after the expected results
        with pytest.raises(StopIteration):
            next(generator)
        # END WITH

        # Ensure that the Elasticsearch scan function was called with the correct parameters
        mock_scan.assert_called_once_with(
            index=context.index_name,
            query=query
        )
    # END WITH
# END test__get_documents
