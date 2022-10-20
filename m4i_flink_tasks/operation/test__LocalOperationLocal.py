import asyncio
import json

import pytest
import requests
from elastic_app_search import Client
from m4i_atlas_core import ConfigStore, Entity

from m4i_flink_tasks.AtlasEntityChangeMessage import EntityMessage
from m4i_flink_tasks.operation.LocalOperationLocal import LocalOperationLocal

from .config import config
from .credentials import credentials

# from scripts.config import config
# from scripts.credentials import credentials



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


def test__map_local4(store):
	kafka_message = '{"id": "18016d4e-f151-4597-a8f9-6a6a0cf948d3", "creationTime": 1664867605259, "entityGuid": "a1f871ca-c77c-47f0-94d7-9b315e4f7b89", "changes": [{"propagate": true, "propagateDown": true, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.UpdateListEntryBasedOnUniqueValueList", "name": "update breadcrumb name", "unqiue_list_key": "breadcrumbguid", "target_list_key": "breadcrumbname", "unqiue_value": "a1f871ca-c77c-47f0-94d7-9b315e4f7b89", "target_value": "test-data-domain-09:13"}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateListEntryBasedOnUniqueValueList", "name": "update derived entity field", "unqiue_list_key": "deriveddatadomainguid", "target_list_key": "deriveddatadomain", "unqiue_value": "a1f871ca-c77c-47f0-94d7-9b315e4f7b89", "target_value": "test-data-domain-09:13"}]}}, {"propagate": false, "propagateDown": false, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "update attribute name", "key": "name", "value": "test-data-domain-09:13"}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "update attribute definition", "key": "definition", "value": "test-data-domain-09:12 "}]}}]}'
	local_operation_local = LocalOperationLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)

def test__map_local5(store):
	kafka_message = '{"id": "f4071f41-9cab-4d0e-bace-cf61b85e4d57", "creationTime": 1664873957208, "entityGuid": "9eb52c56-4f60-42d4-bec5-468308a7308c", "changes": [{"propagate": true, "propagateDown": true, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.UpdateListEntryBasedOnUniqueValueList", "name": "update breadcrumb name", "unqiue_list_key": "breadcrumbguid", "target_list_key": "breadcrumbname", "unqiue_value": "a1f871ca-c77c-47f0-94d7-9b315e4f7b89", "target_value": "test-data-domain-10:59"}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateListEntryBasedOnUniqueValueList", "name": "update derived entity field", "unqiue_list_key": "deriveddatadomainguid", "target_list_key": "deriveddatadomain", "unqiue_value": "a1f871ca-c77c-47f0-94d7-9b315e4f7b89", "target_value": "test-data-domain-10:59"}]}}]}'
	local_operation_local = LocalOperationLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)

def test__map_local6(store):
	kafka_message = '{"id": "b0779b16-5f29-47a1-9d08-40d08ff2cc66", "creationTime": 1664956419156, "entityGuid": "fbdabcd3-751e-482a-a34f-c10e4d142a5f", "changes": [{"propagate": true, "propagateDown": true, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "insert derived entity field deriveddatadomainguid", "key": "deriveddatadomainguid", "value": ["e9b1c37d-49f7-4968-a50d-f6e5eb715b54"]}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "insert derived entity field deriveddatadomain", "key": "deriveddatadomain", "value": ["charif2"]}, {"py/object": "m4i_flink_tasks.operation.core_operation.InsertPrefixToList", "name": "update breadcrumb guid", "key": "breadcrumbguid", "input_list": ["e9b1c37d-49f7-4968-a50d-f6e5eb715b54", "a8a2af6e-db69-46c0-9eb6-1c6ac21b9a30"]}, {"py/object": "m4i_flink_tasks.operation.core_operation.InsertPrefixToList", "name": "update breadcrumb name", "key": "breadcrumbname", "input_list": ["charif2", "americooo"]}, {"py/object": "m4i_flink_tasks.operation.core_operation.InsertPrefixToList", "name": "update breadcrumb type", "key": "breadcrumbtype", "input_list": ["m4i_data_domain", "m4i_data_entity"]}]}}, {"propagate": false, "propagateDown": false, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.CreateLocalEntityProcessor", "name": "create entity with guid fbdabcd3-751e-482a-a34f-c10e4d142a5f of type m4i_data_entity", "entity_guid": "fbdabcd3-751e-482a-a34f-c10e4d142a5f", "entity_type": "m4i_data_entity", "entity_name": "charif1", "entity_qualifiedname": "43ef4b0e-b0bc-4763-86ee-fcd4da5ccd1b"}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "insert attribute name", "key": "name", "value": "charif1"}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "insert attribute parentguid", "key": "parentguid", "value": "a8a2af6e-db69-46c0-9eb6-1c6ac21b9a30"}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "insert attribute deriveddataentityguid", "key": "deriveddataentityguid", "value": ["a8a2af6e-db69-46c0-9eb6-1c6ac21b9a30"]}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "insert attribute deriveddataentity", "key": "deriveddataentity", "value": ["americooo"]}]}}]}'
	local_operation_local = LocalOperationLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)

def test__map_local8(store):
	kafka_message = '{"id": "440cd50f-3950-41d7-b962-ed8cf5ba4dda", "creationTime": 1666083288074, "entityGuid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "changes": [{"propagate": true, "propagateDown": true, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "propagated downwards operation", "steps": [[{"py/object": "m4i_flink_tasks.operation.core_operation.Insert_Hierarchical_Relationship", "name": "insert hierarchical relationship", "parent_entity_guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "child_entity_guid": "863394a9-eb15-4673-bddd-e20b2fe7dc52", "current_entity_guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951"}]]}}]}'
	local_operation_local = LocalOperationLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)

def test__map_local9(store):
	kafka_message = '{"id": "f5c7f798-ad8f-4097-a4f1-00c44c3bd345", "creationTime": 1666103211438, "entityGuid": "9a738814-c77d-4a85-83c8-db8845bbd3f0", "changes": [{"propagate": true, "propagateDown": true, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "propagated downwards operation", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.Delete_Hierarchical_Relationship", "name": "delete hierarchical relationship", "parent_entity_guid": "018dedb6-18bb-4450-a58f-a320147c92d8", "child_entity_guid": "9a738814-c77d-4a85-83c8-db8845bbd3f0", "current_entity_guid": "9a738814-c77d-4a85-83c8-db8845bbd3f0", "derived_guid": "deriveddataentityguid"}, {"py/object": "m4i_flink_tasks.operation.core_operation.Insert_Hierarchical_Relationship", "name": "insert hierarchical relationship", "parent_entity_guid": "018dedb6-18bb-4450-a58f-a320147c92d8", "child_entity_guid": "9a738814-c77d-4a85-83c8-db8845bbd3f0", "current_entity_guid": "9a738814-c77d-4a85-83c8-db8845bbd3f0"}]}}]}' 
	local_operation_local = LocalOperationLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)

def test__map_local10(store):
	kafka_message = '{"id": "26ffa68f-140f-4096-933a-1612dca48768", "creationTime": 1666168391277, "entityGuid": "018dedb6-18bb-4450-a58f-a320147c92d8", "changes": [{"propagate": true, "propagateDown": true, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "propagated downwards operation", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.UpdateListEntryBasedOnUniqueValueList", "name": "update breadcrumb name", "unqiue_list_key": "breadcrumbguid", "target_list_key": "breadcrumbname", "unqiue_value": "018dedb6-18bb-4450-a58f-a320147c92d8", "target_value": "test-data-domain-19-10-22-10:33"}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateListEntryBasedOnUniqueValueList", "name": "update derived entity field", "unqiue_list_key": "deriveddatadomainguid", "target_list_key": "deriveddatadomain", "unqiue_value": "018dedb6-18bb-4450-a58f-a320147c92d8", "target_value": "test-data-domain-19-10-22-10:33"}]}}, {"propagate": false, "propagateDown": false, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "local operations", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "update attribute name", "key": "name", "value": "test-data-domain-19-10-22-10:33"}]}}]}'
	local_operation_local = LocalOperationLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)

def test__map_local11(store):
	kafka_message = '{"id": "18e41391-bee8-4a1a-ad60-2dd9f3b90108", "creationTime": 1666265344133, "entityGuid": "c660893f-1a14-4940-894f-9a91391c1b39", "changes": [{"propagate": true, "propagateDown": true, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "propagated downwards operation", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.Insert_Hierarchical_Relationship", "name": "insert hierarchical relationship", "parent_entity_guid": "3a2429e1-d80f-4beb-a59f-e8f4f21b4d4b", "child_entity_guid": "c660893f-1a14-4940-894f-9a91391c1b39", "current_entity_guid": "3a2429e1-d80f-4beb-a59f-e8f4f21b4d4b"}]}}]}'	
	local_operation_local = LocalOperationLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)