import pytest
import json 
import requests
import asyncio

from m4i_atlas_core import ConfigStore
from m4i_flink_tasks.AtlasEntityChangeMessage import EntityMessage
from elastic_app_search import Client

from m4i_flink_tasks.operation.LocalOperationLocal import LocalOperationLocal
from m4i_atlas_core import ConfigStore, Entity
# from scripts.config import config
# from scripts.credentials import credentials

from .config import config
from .credentials import credentials


config_store = ConfigStore.get_instance()

# config = {
#     "elastic.search.index" : "atlas-dev-test",
#     "elastic.app.search.engine.name" : "atlas-dev-test",
#     "operations.appsearch.engine.name": "atlas-dev",
#     "elastic.base.endpoint" : "https://aureliusdev.westeurope.cloudapp.azure.com:443/anwo/elastic/api/as/v1",
#     "elastic.search.endpoint" : "https://aureliusdev.westeurope.cloudapp.azure.com:443/anwo/elastic",
#     "elastic.enterprise.search.endpoint": "https://aureliusdev.westeurope.cloudapp.azure.com:443/anwo/app-search",
#     }

# credentials = {
#     "elastic.user": "elastic",
#     "elastic.passwd": "1aYh9R16np9KWjz96v5x3J1Z",
# }

test_engine_name = "test_synchronize_app_search_engine"

kafka_notification = '''{
	"id": "9bac6d0f-e883-4900-93ed-c7f9a8e996ea",
	"creationTime": 1663848414993,
	"entityGuid": "871dfdf5-fe98-4673-b5bc-75037bb1ca3b",
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
						"name": "create entity with guid 871dfdf5-fe98-4673-b5bc-75037bb1ca3b of type hdfs_path",
						"entity_guid": "871dfdf5-fe98-4673-b5bc-75037bb1ca3b",
						"entity_type": "hdfs_path"
					},
					{
						"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor",
						"name": "insert attribute name",
						"key": "name",
						"value": "asdfasd3333"
					}]}}]}
'''

@pytest.fixture(autouse=True)
def store():
    config_store = ConfigStore.get_instance()
    config_store.load({**config, **credentials})

    yield config_store

    config_store.reset()
# END store

def test__map_local0(store):
#    kafka_message = '{"id": "3ff49d7a-0c7e-4b56-a354-fb689547e502", "creationTime": 1663762643749, "entityGuid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "changes": [{"propagate": false, "propagateDown": false, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update attribute", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "update attribute", "key": "definition", "value": "This domain contains data related to Finance & Control which is relevant for budgeting, forecasting and monitoring on employee level. (update 21 September 2022 14:17)"}]}}]}' 
    local_operation_local = LocalOperationLocal()
    local_operation_local.open_local(config, credentials,config_store  )
   
    res = local_operation_local.map_local(kafka_notification)
    # assert  local_operation_local.map_local(kafka_message) == None
    

def test__map_local1(store):
	kafka_message = '{"id":"f006810a-84ba-4c4b-a145-9d70a6d7da68","creationTime":1663923271838,"entityGuid":"b6044c9a-61b3-4a02-acec-e028e1f2c951","changes":[{"propagate":true,"propagateDown":true,"operation":{"py/object":"m4i_flink_tasks.operation.core_operation.Sequence","name":"update and inser attributes","steps":[{"py/object":"m4i_flink_tasks.operation.core_operation.UpdateListEntryProcessor","name":"update attribute name","key":"breadcrumbname","old_value":"Finance and Control (16:36)","new_value":"Finance and Control"}]}},{"propagate":false,"propagateDown":false,"operation":{"py/object":"m4i_flink_tasks.operation.core_operation.Sequence","name":"update and inser attributes","steps":[{"py/object":"m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor","name":"update attribute name","key":"name","value":"Finance and Control"}]}}]}'
	local_operation_local = LocalOperationLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)

	assert len(res) == 1 


def test__map_local2(store):
	kafka_message = '{"id": "fbf78656-dcea-4bce-b849-70fec329c903", "creationTime": 1664190104173, "entityGuid": "863394a9-eb15-4673-bddd-e20b2fe7dc52", "changes": [{"propagate": true, "propagateDown": true, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.InsertPrefixToList", "name": "update breadcrumb guid", "key": "breadcrumbguid", "input_list": ["b6044c9a-61b3-4a02-acec-e028e1f2c951"]}, {"py/object": "m4i_flink_tasks.operation.core_operation.InsertPrefixToList", "name": "update breadcrumb name", "key": "breadcrumbname", "input_list": ["Finance and Control"]}, {"py/object": "m4i_flink_tasks.operation.core_operation.InsertPrefixToList", "name": "update breadcrumb type", "key": "breadcrumbtype", "input_list": ["m4i_data_domain"]}]}}]}'
	local_operation_local = LocalOperationLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)

	# assert len(res) == 1 

def test__map_local3(store):
	kafka_message = '{"id": "3da9cda7-31fc-4609-b5d3-15ff301a8e87", "creationTime": 1664538044351, "entityGuid": "deff4a82-97b2-44c4-8078-71cf00be66de", "changes": [{"propagate": true, "propagateDown": true, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.InsertPrefixToList", "name": "update breadcrumb guid", "key": "breadcrumbguid", "input_list": ["eafb7a69-80bb-41ea-a52b-e76b96376cd1"]}, {"py/object": "m4i_flink_tasks.operation.core_operation.InsertPrefixToList", "name": "update breadcrumb name", "key": "breadcrumbname", "input_list": ["test-data-domain"]}, {"py/object": "m4i_flink_tasks.operation.core_operation.InsertPrefixToList", "name": "update breadcrumb type", "key": "breadcrumbtype", "input_list": ["m4i_data_domain"]}]}}, {"propagate": false, "propagateDown": false, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "insert attribute parentguid", "key": "parentguid", "value": "eafb7a69-80bb-41ea-a52b-e76b96376cd1"}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "insert attribute deriveddatadomainguid", "key": "deriveddatadomainguid", "value": ["eafb7a69-80bb-41ea-a52b-e76b96376cd1"]}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "insert attribute deriveddatadomain", "key": "deriveddatadomain", "value": "test-data-domain"}]}}]}'
	local_operation_local = LocalOperationLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)
	