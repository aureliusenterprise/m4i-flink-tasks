from m4i_atlas_core import RelationshipAttribute

from ......model import (AtlasChangeMessageWithPreviousVersion,
                         EntityMessageType)
from .handle_delete_operation import handle_delete_operation


def create_mock_change_message(entity: dict) -> AtlasChangeMessageWithPreviousVersion:
    change_message = {
        "msg_compression_kind": "none",
        "msg_split_idx": 0,
        "msg_split_count": 1,
        "msg_created_by": "user",
        "msg_creation_time": 162392394,
        "message": {
            "event_time": 162392394,
            "operation_type": "ENTITY_DELETE",
            "type": "ENTITY_NOTIFICATION_V2",
            "entity": entity
        },
        "version": {
            "version": "1.0",
            "version_parts": [1, 0]
        },
        "msg_source_ip": "192.168.1.1",
        "previous_version": entity
    }

    return AtlasChangeMessageWithPreviousVersion.from_dict(change_message)
# END create_mock_change_message


def test_handle_delete_operation_no_attributes_relationships():
    entity = {
        "type_name": "SampleEntity",
        "relationship_attributes": {}
    }

    change_message = create_mock_change_message(entity)

    messages = handle_delete_operation(change_message)

    assert len(messages) == 1
    assert messages[0].event_type == EntityMessageType.ENTITY_DELETED
    assert messages[0].deleted_attributes == []
    assert messages[0].deleted_relationships == {}


def test_handle_delete_operation_attributes_only():
    entity = {
        "type_name": "SampleEntity",
        "attributes": {"attr1": "test"},
        "relationship_attributes": {}
    }

    change_message = create_mock_change_message(entity)

    messages = handle_delete_operation(change_message)

    assert len(messages) == 1
    assert messages[0].event_type == EntityMessageType.ENTITY_DELETED
    assert messages[0].deleted_attributes == ["attr1"]
    assert messages[0].deleted_relationships == {}


def test_handle_delete_operation_relationships_only():
    entity = {
        "type_name": "SampleEntity",
        "relationship_attributes": {
            "relation1": [
                {
                    "guid": "12345",
                    "relationship_guid": "12345",
                    "type_name": "RelatedEntity"
                },
                {
                    "guid": "23456",
                    "relationship_guid": "23456",
                    "type_name": "RelatedEntity"
                }
            ]
        }
    }

    change_message = create_mock_change_message(entity)

    messages = handle_delete_operation(change_message)

    assert len(messages) == 1
    assert messages[0].event_type == EntityMessageType.ENTITY_DELETED
    assert messages[0].deleted_attributes == []
    assert messages[0].deleted_relationships == {
        "relation1": [
            RelationshipAttribute.from_dict({
                "guid": "12345",
                "relationship_guid": "12345",
                "type_name": "RelatedEntity"
            }),
            RelationshipAttribute.from_dict({
                "guid": "23456",
                "relationship_guid": "23456",
                "type_name": "RelatedEntity"
            })
        ]
    }


def test_handle_delete_operation_both_attributes_relationships():
    entity = {
        "type_name": "SampleEntity",
        "attributes": {"attr1": "test", "attr2": "value"},
        "relationship_attributes": {
            "relation1": [
                {
                    "guid": "12345",
                    "relationship_guid": "12345",
                    "type_name": "RelatedEntity"
                },
                {
                    "guid": "23456",
                    "relationship_guid": "23456",
                    "type_name": "RelatedEntity"
                }
            ]
        }
    }

    change_message = create_mock_change_message(entity)

    messages = handle_delete_operation(change_message)

    assert len(messages) == 1
    assert messages[0].event_type == EntityMessageType.ENTITY_DELETED
    assert messages[0].deleted_attributes == ["attr1", "attr2"]
    assert messages[0].deleted_relationships == {
        "relation1": [
            RelationshipAttribute.from_dict({
                "guid": "12345",
                "relationship_guid": "12345",
                "type_name": "RelatedEntity"
            }),
            RelationshipAttribute.from_dict({
                "guid": "23456",
                "relationship_guid": "23456",
                "type_name": "RelatedEntity"
            })
        ]
    }
