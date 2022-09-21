from m4i_flink_tasks.operation.LocalOperationLocal import LocalOperationLocal

import pytest

from config import config
from credentials import credentials

from m4i_atlas_core import ConfigStore
from AtlasEntityChangeMessage import EntityMessage
from elastic_app_search import Client

from m4i_atlas_core import *
import json 
import requests
import asyncio

test_engine_name = "test_synchronize_app_search_engine"


@pytest.fixture(autouse=True)
def store():
    config_store = ConfigStore.get_instance()
    config_store.load({**config, **credentials})

    yield config_store

    config_store.reset()
# END store

def test_map_local():
    kafka_message = '{"id": "3ff49d7a-0c7e-4b56-a354-fb689547e502", "creationTime": 1663762643749, "entityGuid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "changes": [{"propagate": false, "propagateDown": false, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update attribute", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "update attribute", "key": "definition", "value": "This domain contains data related to Finance & Control which is relevant for budgeting, forecasting and monitoring on employee level. (update 21 September 2022 14:17)"}]}}]}' 
    local_operation_local = LocalOperationLocal()
    local_operation_local.open_local(config, credentials,

    )
   
    assert  local_operation_local.map_local(kafka_message) == None
