import json
from unittest.mock import AsyncMock, patch

import pytest
from m4i_atlas_core import Entity

from .get_entity import get_entity_details


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


@pytest.mark.asyncio
async def test__get_entity_details():
    change_message = create_mock_change_message({
        "guid": "1234-5678-9012",
        "name": "Sample Entity",
        "typeName": "SampleType"
    })

    sample_notification = json.dumps(change_message)
    sample_entity_details = Entity(guid="1234-5678-9012")

    expected = create_mock_change_message(sample_entity_details.to_dict())

    with patch("m4i_flink_tasks.jobs.get_entity.operations.get_entity.get_keycloak_token") as mock_get_keycloak_token, \
            patch("m4i_flink_tasks.jobs.get_entity.operations.get_entity.get_entity_by_guid", new_callable=AsyncMock) as mock_get_entity_by_guid:

        mock_get_keycloak_token.return_value = "fake_token"
        mock_get_entity_by_guid.return_value = sample_entity_details

        result = await get_entity_details(sample_notification)

        assert json.loads(result) == expected
    # END WITH mock_get_keycloak_token, mock_get_entity_by_guid
# END test__get_entity_details
