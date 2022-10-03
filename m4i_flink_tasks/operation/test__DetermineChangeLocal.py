import pytest
import json 
import requests
import asyncio

from m4i_atlas_core import ConfigStore
from m4i_flink_tasks.AtlasEntityChangeMessage import EntityMessage
from elastic_app_search import Client

from m4i_flink_tasks.operation.LocalOperationLocal import LocalOperationLocal

from m4i_flink_tasks.operation.DetermineChangeLocal import DetermineChangeLocal

from m4i_flink_tasks.operation.PublishStateLocal import PublishStateLocal

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




def test__map_local(store):
	kafka_message = '{"kafka_notification": {"version": {"version": "1.0.0", "versionParts": [1]}, "msgCompressionKind": "NONE", "msgSplitIdx": 1, "msgSplitCount": 1, "msgSourceIP": "10.0.2.4", "msgCreatedBy": "", "msgCreationTime": 1664544395397, "message": {"eventTime": 1664544395230, "operationType": "ENTITY_DELETE", "type": "ENTITY_NOTIFICATION_V2", "entity": {"typeName": "m4i_data_entity", "attributes": {"qualifiedName": "7ea9c0cf-4970-44c3-befe-24394f0e655f", "name": "test-data-entity-15:10"}, "classifications": [], "createTime": null, "createdBy": null, "customAttributes": null, "guid": "8452256a-f849-4385-84b8-bb72723bb8e3", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": null, "proxy": null, "relationshipAttributes": null, "status": "ACTIVE", "updateTime": null, "updatedBy": null, "version": null}, "relationship": null}}, "atlas_entity": {}, "msg_creation_time": 1664544395397}'
	local_operation_local = DetermineChangeLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)

	assert True

def test__map_local2(store):
	kafka_message = '{"kafka_notification": {"version": {"version": "1.0.0", "versionParts": [1]}, "msgCompressionKind": "NONE", "msgSplitIdx": 1, "msgSplitCount": 1, "msgSourceIP": "10.0.2.4", "msgCreatedBy": "", "msgCreationTime": 1664552461474, "message": {"eventTime": 1664552461438, "operationType": "ENTITY_UPDATE", "type": "ENTITY_NOTIFICATION_V2", "entity": {"typeName": "m4i_data_domain", "attributes": {"archimateReference": [], "qualifiedName": "b279c84d-77c9-4bbe-9dda-75ae48bd23dd", "domainLead": [], "name": "test-data-domain-17:39", "dataEntity": [{"guid": "246e9672-a59f-4b52-9027-508451f35615", "typeName": "m4i_data_entity", "uniqueAttributes": {"qualifiedName": "15fb38cc-e190-431e-a4d2-ab8750da302b"}}], "definition": "test-data-domain-17:39", "source": []}, "classifications": [], "createTime": null, "createdBy": null, "customAttributes": null, "guid": "ac914e31-01f6-4736-a641-e355ef212410", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": null, "proxy": null, "relationshipAttributes": null, "status": "ACTIVE", "updateTime": null, "updatedBy": null, "version": null}, "relationship": null}}, "atlas_entity": {"typeName": "m4i_data_domain", "attributes": {"archimateReference": [], "replicatedTo": null, "replicatedFrom": null, "qualifiedName": "b279c84d-77c9-4bbe-9dda-75ae48bd23dd", "domainLead": [], "name": "test-data-domain-17:39", "dataEntity": [{"guid": "246e9672-a59f-4b52-9027-508451f35615", "typeName": "m4i_data_entity", "uniqueAttributes": {"qualifiedName": "15fb38cc-e190-431e-a4d2-ab8750da302b"}}], "definition": "test-data-domain-17:39", "source": []}, "classifications": [], "createTime": 1664552401180, "createdBy": "atlas", "customAttributes": null, "guid": "ac914e31-01f6-4736-a641-e355ef212410", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": null, "proxy": null, "relationshipAttributes": {"ArchiMateReference": [], "domainLead": [], "dataEntity": [{"guid": "246e9672-a59f-4b52-9027-508451f35615", "typeName": "m4i_data_entity", "entityStatus": "ACTIVE", "displayText": "test-data-entity-17:40", "relationshipType": "m4i_data_entity_assignment", "relationshipGuid": "3a0c839b-5824-41b5-989d-6f4fb60422f3", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_data_entity_assignment"}}], "source": [], "meanings": []}, "status": "ACTIVE", "updateTime": 1664552461438, "updatedBy": "atlas", "version": 0}, "msg_creation_time": 1664552461474}' 
	local_operation_local = DetermineChangeLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)

	assert True

def test__map_local3(store):
	kafka_message = '{"kafka_notification": {"version": {"version": "1.0.0", "versionParts": [1]}, "msgCompressionKind": "NONE", "msgSplitIdx": 1, "msgSplitCount": 1, "msgSourceIP": "10.0.2.4", "msgCreatedBy": "", "msgCreationTime": 1664799679902, "message": {"eventTime": 1664799679823, "operationType": "ENTITY_UPDATE", "type": "ENTITY_NOTIFICATION_V2", "entity": {"typeName": "m4i_data_entity", "attributes": {"archimateReference": [], "qualifiedName": "7d325e9d-4278-4b80-a8fb-d1bd29bc0456", "name": "data-entity-14:03", "definition": "data-entity-14:03"}, "classifications": [], "createTime": null, "createdBy": null, "customAttributes": null, "guid": "c3449cb9-ea37-4d91-a54d-6034b4562181", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": null, "proxy": null, "relationshipAttributes": null, "status": "ACTIVE", "updateTime": null, "updatedBy": null, "version": null}, "relationship": null}}, "atlas_entity": {"typeName": "m4i_data_entity", "attributes": {"archimateReference": [], "replicatedTo": null, "replicatedFrom": null, "steward": [], "qualifiedName": "7d325e9d-4278-4b80-a8fb-d1bd29bc0456", "parentEntity": [], "source": [], "dataDomain": [], "childEntity": [], "name": "data-entity-14:03", "definition": "data-entity-14:03", "attributes": [], "businessOwner": []}, "classifications": [], "createTime": 1664798611870, "createdBy": "atlas", "customAttributes": null, "guid": "c3449cb9-ea37-4d91-a54d-6034b4562181", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": null, "proxy": null, "relationshipAttributes": {"ArchiMateReference": [], "steward": [], "dataDomain": [], "parentEntity": [], "childEntity": [], "attributes": [], "source": [], "businessOwner": [], "meanings": []}, "status": "ACTIVE", "updateTime": 1664799679823, "updatedBy": "atlas", "version": 0}, "msg_creation_time": 1664799679902}' 

	local_operation_local = DetermineChangeLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)

	assert True



