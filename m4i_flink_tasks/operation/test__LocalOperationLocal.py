import pytest
import json 
import requests
import asyncio

from m4i_atlas_core import ConfigStore
from m4i_flink_tasks.AtlasEntityChangeMessage import EntityMessage
from elastic_app_search import Client

from m4i_flink_tasks.operation.LocalOperationLocal import LocalOperationLocal
from m4i_atlas_core import ConfigStore, Entity
from scripts.config import config
from scripts.credentials import credentials
config_store = ConfigStore.get_instance()

config = {
    "elastic.search.index" : "atlas-dev-test",
    "elastic.app.search.engine.name" : "atlas-dev-test",
    "operations.appsearch.engine.name": "atlas-dev",
    "elastic.base.endpoint" : "https://aureliusdev.westeurope.cloudapp.azure.com:443/anwo/elastic/api/as/v1",
    "elastic.search.endpoint" : "https://aureliusdev.westeurope.cloudapp.azure.com:443/anwo/elastic",
    "elastic.enterprise.search.endpoint": "https://aureliusdev.westeurope.cloudapp.azure.com:443/anwo/app-search",
    }

credentials = {
    "elastic.user": "elastic",
    "elastic.passwd": "09NMCUQW5v69Cz21e4ZTSo65",
}

test_engine_name = "test_synchronize_app_search_engine"

kafka_notification = '''{"id": "9bea1d2f-a397-43a0-80cc-a9b6558f6bcf",
	"creationTime": 1663832965568,
	"entityGuid": "d3e7fe15-9863-4d14-b895-081ecd282591",
	"changes": [
		{
			"propagate": false,
			"propagateDown": false,
			"operation": {
				"py/object": "m4i_flink_tasks.operation.core_operation.Sequence",
				"name": "update and inser attributes",
				"steps": [
					{
						"py/object": "m4i_flink_tasks.operation.core_operation.CreateLocalEntityProcessor",
						"name": "create entity with guid d3e7fe15-9863-4d14-b895-081ecd282591 of type hdfs_path",
						"entity_guid": "d3e7fe15-9863-4d14-b895-081ecd282591",
						"entity_type": "hdfs_path"
					},
					{
						"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor",
						"name": "insert attribute name",
						"key": "name",
						"value": "asd"
					}]}}]}'''

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
    local_operation_local.open_local(config, credentials,config_store  )
   
    res = local_operation_local.map_local(kafka_notification)
    assert  local_operation_local.map_local(kafka_message) == None
    
    