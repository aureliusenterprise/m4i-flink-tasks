import json
from unittest.mock import MagicMock

from elasticsearch import Elasticsearch
from m4i_atlas_core import Entity

from .publish_state import PublishState


def create_mock_change_message(entity: dict):
    return {
        "msgCompressionKind": "none",
        "msgSplitIdx": 0,
        "msgSplitCount": 1,
        "msgCreatedBy": "user",
        "msgCreationTime": 162392394,
        "message": {
            "eventTime": 162392394,
            "operationType": "ENTITY_CREATE",
            "type": "ENTITY_NOTIFICATION_V2",
            "entity": entity,
            "relationship": None
        },
        "version": {
            "version": "1.0",
            "versionParts": [1, 0]
        },
        "msgSourceIP": "192.168.1.1"
    }
# END create_mock_change_message


def test_get_previous_atlas_entity_found():
    elastic = MagicMock(spec=Elasticsearch)
    index_name = "test_index"
    publish_state = PublishState(elastic, index_name)

    guid = "test_guid"
    timestamp = 123456789

    sample_entity = {
        "typeName": "test_type",
        "attributes": {"name": "Test Entity"},
        "guid": guid,
    }

    search_result = {
        "hits": {
            "total": 1,
            "hits": [{"_source": {"body": sample_entity}}],
        },
    }

    elastic.search.return_value = search_result

    entity = publish_state.get_previous_atlas_entity(guid, timestamp)

    assert isinstance(entity, Entity)
    assert entity.guid == guid
    assert entity.type_name == "test_type"
    assert entity.attributes.unmapped_attributes["name"] == "Test Entity"


def test_get_previous_atlas_entity_not_found():
    elastic = MagicMock(spec=Elasticsearch)
    index_name = "test_index"
    publish_state = PublishState(elastic, index_name)

    guid = "test_guid"
    timestamp = 123456789

    search_result = {
        "hits": {
            "total": 0,
            "hits": [],
        },
    }

    elastic.search.return_value = search_result

    entity = publish_state.get_previous_atlas_entity(guid, timestamp)

    assert entity is None


def test_publish_state_map():
    elastic = MagicMock(spec=Elasticsearch)
    index_name = "test_index"
    publish_state = PublishState(elastic, index_name)

    entity_guid = "12345"

    sample_entity = {
        "typeName": "test_type",
        "attributes": {"name": "Test Entity"},
        "guid": entity_guid,
    }

    sample_change_message = create_mock_change_message(sample_entity)

    sample_previous_entity = {
        "typeName": "test_type",
        "attributes": {"name": "Previous Entity"},
        "guid": entity_guid,
    }

    search_result = {
        "hits": {
            "total": 1,
            "hits": [{"_source": {"body": sample_previous_entity}}],
        },
    }

    elastic.search.return_value = search_result

    mapped_change_message_json = publish_state.map(
        value=json.dumps(sample_change_message)
    )

    mapped_change_message = json.loads(mapped_change_message_json)

    assert "previousVersion" in mapped_change_message
    assert mapped_change_message["previousVersion"]["guid"] == entity_guid
    assert mapped_change_message["previousVersion"]["attributes"]["name"] == "Previous Entity"
