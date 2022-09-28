from m4i_flink_tasks.operation.SynchronizeAppsearchLocal import SynchronizeAppsearchLocal
from m4i_atlas_core import ConfigStore, Entity
import json 


config_store = ConfigStore.get_instance()

config = {
    "elastic.search.index" : "atlas-dev-test",
    "elastic.app.search.engine.name" : "atlas-dev-test",
    "operations.appsearch.engine.name": "atlas-dev",
    "elastic.base.endpoint" : "https://aureliusdev.westeurope.cloudapp.azure.com:443/demo/elastic/api/as/v1",
    "elastic.search.endpoint" : "https://aureliusdev.westeurope.cloudapp.azure.com:443/demo/elastic",
    "elastic.enterprise.search.endpoint": "https://aureliusdev.westeurope.cloudapp.azure.com:443/demo/app-search",
    }

credentials = {
    "elastic.user": "elastic",
    "elastic.passwd": "gY722L658znu5T3uDJ8m6uHi",
}


msg1 = '''{
	"typeName": "hdfs_path",
	"qualifiedName": "sdfsdf",
	"guid": "7f4eae15-9b19-41cc-9ed1-8313bf1b5536",
	"msgCreationTime": 1663829130370,
	"originalEventType": "ENTITY_CREATE",
	"directChange": true,
	"eventType": "EntityCreated",
	"insertedAttributes": [
		"modifiedTime",
		"isFile",
		"numberOfReplicas",
		"qualifiedName",
		"path",
		"createTime",
		"fileSize",
		"name",
		"isSymlink"
	],
	"changedAttributes": [],
	"deletedAttributes": [],
	"insertedRelationships": {
		"inputToProcesses": [],
		"pipeline": null,
		"schema": [],
		"hiveDb": null,
		"model": null,
		"meanings": [],
		"outputFromProcesses": []
	},
	"changedRelationships": {},
	"deletedRelationships": {},
	"oldValue": {},
	"newValue": {
		"typeName": "hdfs_path",
		"attributes": {
			"owner": null,
			"modifiedTime": 1663797600000,
			"replicatedTo": [],
			"userDescription": null,
			"isFile": false,
			"numberOfReplicas": 0,
			"replicatedFrom": [],
			"qualifiedName": "sdfsdf",
			"displayName": null,
			"description": null,
			"extendedAttributes": null,
			"nameServiceId": null,
			"path": "sdf",
			"posixPermissions": null,
			"createTime": 1663797600000,
			"fileSize": 0,
			"clusterName": null,
			"name": "test",
			"isSymlink": false,
			"group": null
		},
		"classifications": [],
		"createTime": 1663829129845,
		"createdBy": "atlas",
		"customAttributes": null,
		"guid": "7f4eae15-9b19-41cc-9ed1-8313bf1b5536",
		"homeId": null,
		"isIncomplete": false,
		"labels": [],
		"meanings": [],
		"provenanceType": null,
		"proxy": null,
		"relationshipAttributes": {
			"inputToProcesses": [],
			"pipeline": null,
			"schema": [],
			"hiveDb": null,
			"model": null,
			"meanings": [],
			"outputFromProcesses": []
		},
		"status": "ACTIVE",
		"updateTime": 1663829129845,
		"updatedBy": "atlas",
		"version": 0
	}
}'''

kafka_msg2 = '''{"typeName": "m4i_data_domain", "qualifiedName": "Finance and Control", "guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "msgCreationTime": 1663922517117, "originalEventType": "ENTITY_UPDATE", "directChange": true, "eventType": "EntityAttributeAudit", "insertedAttributes": [], "changedAttributes": ["name"], "deletedAttributes": [], "insertedRelationships": {}, "changedRelationships": {}, "deletedRelationships": {}, "oldValue": {"typeName": "m4i_data_domain", "attributes": {"archimateReference": [], "replicatedTo": [], "replicatedFrom": [], "qualifiedName": "Finance and Control", "domainLead": [{"guid": "0a736fdd-df31-4deb-936d-fdf92812789b", "typeName": "m4i_person", "uniqueAttributes": {"qualifiedName": "albertjan.kroezen@vanoord.com", "email": "albertjan.kroezen@vanoord.com"}}], "name": "Finance and Control (16:36)", "dataEntity": [], "definition": "This domain contains data related to Finance & Control which is relevant for budgeting, forecasting and monitoring on employee level. (update 23 September 2022 10:26)", "source": []}, "classifications": [], "createTime": 1649238419087, "createdBy": "admin", "customAttributes": null, "guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": 0, "proxy": false, "relationshipAttributes": {"ArchiMateReference": [], "domainLead": [{"guid": "0a736fdd-df31-4deb-936d-fdf92812789b", "typeName": "m4i_person", "entityStatus": "ACTIVE", "displayText": "Albert-Jan Kroezen", "relationshipType": "m4i_domainLead_assignment", "relationshipGuid": "53a50ae8-a056-494d-b324-c79c2002e619", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_domainLead_assignment"}}], "dataEntity": [], "source": [], "meanings": []}, "status": "ACTIVE", "updateTime": 1663921604992, "updatedBy": "atlas", "version": 0}, "newValue": {"typeName": "m4i_data_domain", "attributes": {"archimateReference": [], "replicatedTo": [], "replicatedFrom": [], "qualifiedName": "Finance and Control", "domainLead": [{"guid": "0a736fdd-df31-4deb-936d-fdf92812789b", "typeName": "m4i_person", "uniqueAttributes": {"qualifiedName": "albertjan.kroezen@vanoord.com", "email": "albertjan.kroezen@vanoord.com"}}], "name": "Finance and Control", "dataEntity": [], "definition": "This domain contains data related to Finance & Control which is relevant for budgeting, forecasting and monitoring on employee level. (update 23 September 2022 10:26)", "source": []}, "classifications": [], "createTime": 1649238419087, "createdBy": "admin", "customAttributes": null, "guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": 0, "proxy": false, "relationshipAttributes": {"ArchiMateReference": [], "domainLead": [{"guid": "0a736fdd-df31-4deb-936d-fdf92812789b", "typeName": "m4i_person", "entityStatus": "ACTIVE", "displayText": "Albert-Jan Kroezen", "relationshipType": "m4i_domainLead_assignment", "relationshipGuid": "53a50ae8-a056-494d-b324-c79c2002e619", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_domainLead_assignment"}}], "dataEntity": [], "source": [], "meanings": []}, "status": "ACTIVE", "updateTime": 1663922517090, "updatedBy": "atlas", "version": 0}}'''
kafka_msg3 = '''{"typeName": "m4i_data_domain", "qualifiedName": "Finance and Control", "guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "msgCreationTime": 1663921605018, "originalEventType": "ENTITY_UPDATE", "directChange": true, "eventType": "EntityAttributeAudit", "insertedAttributes": [], "changedAttributes": ["definition"], "deletedAttributes": [], "insertedRelationships": {}, "changedRelationships": {}, "deletedRelationships": {}, "oldValue": {"typeName": "m4i_data_domain", "attributes": {"archimateReference": [], "replicatedTo": [], "replicatedFrom": [], "qualifiedName": "Finance and Control", "domainLead": [{"guid": "0a736fdd-df31-4deb-936d-fdf92812789b", "typeName": "m4i_person", "uniqueAttributes": {"qualifiedName": "albertjan.kroezen@vanoord.com", "email": "albertjan.kroezen@vanoord.com"}}], "name": "Finance and Control (16:36)", "dataEntity": [], "definition": "This domain contains data related to Finance & Control which is relevant for budgeting, forecasting and monitoring on employee level. (update 22 September 2022 13:11)", "source": []}, "classifications": [], "createTime": 1649238419087, "createdBy": "admin", "customAttributes": null, "guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": 0, "proxy": false, "relationshipAttributes": {"ArchiMateReference": [], "domainLead": [{"guid": "0a736fdd-df31-4deb-936d-fdf92812789b", "typeName": "m4i_person", "entityStatus": "ACTIVE", "displayText": "Albert-Jan Kroezen", "relationshipType": "m4i_domainLead_assignment", "relationshipGuid": "53a50ae8-a056-494d-b324-c79c2002e619", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_domainLead_assignment"}}], "dataEntity": [], "source": [], "meanings": []}, "status": "ACTIVE", "updateTime": 1663845104455, "updatedBy": "atlas", "version": 0}, "newValue": {"typeName": "m4i_data_domain", "attributes": {"archimateReference": [], "replicatedTo": [], "replicatedFrom": [], "qualifiedName": "Finance and Control", "domainLead": [{"guid": "0a736fdd-df31-4deb-936d-fdf92812789b", "typeName": "m4i_person", "uniqueAttributes": {"qualifiedName": "albertjan.kroezen@vanoord.com", "email": "albertjan.kroezen@vanoord.com"}}], "name": "Finance and Control (16:36)", "dataEntity": [], "definition": "This domain contains data related to Finance & Control which is relevant for budgeting, forecasting and monitoring on employee level. (update 23 September 2022 10:26)", "source": []}, "classifications": [], "createTime": 1649238419087, "createdBy": "admin", "customAttributes": null, "guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": 0, "proxy": false, "relationshipAttributes": {"ArchiMateReference": [], "domainLead": [{"guid": "0a736fdd-df31-4deb-936d-fdf92812789b", "typeName": "m4i_person", "entityStatus": "ACTIVE", "displayText": "Albert-Jan Kroezen", "relationshipType": "m4i_domainLead_assignment", "relationshipGuid": "53a50ae8-a056-494d-b324-c79c2002e619", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_domainLead_assignment"}}], "dataEntity": [], "source": [], "meanings": []}, "status": "ACTIVE", "updateTime": 1663921604992, "updatedBy": "atlas", "version": 0}}'''

from .config import config
from .credentials import credentials
import pytest

from .SynchronizeAppsearchLocal import SynchronizeAppsearchLocal

def test_msg1():
    config_store.load({**config, **credentials})
    
    asl = SynchronizeAppsearchLocal()
    asl.open_local(config, credentials, config_store)
    
    res = asl.map_local(msg1)
    expected_res = '{"id": "1fc0f72a-063e-4015-8bb6-6666d40de645", "creationTime": 1663832173352, "entityGuid": "7f4eae15-9b19-41cc-9ed1-8313bf1b5536", "changes": [{"propagate": false, "propagateDown": false, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.CreateLocalEntityProcessor", "name": "create entity with guid 7f4eae15-9b19-41cc-9ed1-8313bf1b5536 of type hdfs_path", "entity_guid": "7f4eae15-9b19-41cc-9ed1-8313bf1b5536", "entity_type": "hdfs_path"}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "insert attribute name", "key": "name", "value": "test"}]}}]}'
    assert(res == expected_res)
    




@pytest.mark.asyncio
async def test_msg2():
	config_store.load({**config, **credentials})
	asl = SynchronizeAppsearchLocal()
	asl.open_local(config, credentials, config_store)
	res = await asl.map_local(kafka_msg2)
	expected_res = '{"id": "9c6a3d1d-ca84-4125-a5ce-4249a9f9818d", "creationTime": 1663923390185, "entityGuid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "changes": [{"propagate": true, "propagateDown": true, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.UpdateListEntryProcessor", "name": "update attribute name", "key": "breadcrumbname", "old_value": "Finance and Control (16:36)", "new_value": "Finance and Control"}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateListEntryProcessor", "name": "update derived entity field {deriveddatadomain", "key": "deriveddatadomain", "old_value": "Finance and Control (16:36)", "new_value": "Finance and Control"}]}}, {"propagate": false, "propagateDown": false, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "update attribute name", "key": "name", "value": "Finance and Control"}]}}]}'
	expected_res = json.loads(expected_res)
	res = json.loads(res)

	expected_res["id"] = -1
	expected_res["creationTime"] = -1

	res["id"] = -1
	res["creationTime"] = -1
	assert res == expected_res

@pytest.mark.asyncio
async def test_msg3():
	kafka_msg3 = '{"typeName": "m4i_data_domain", "qualifiedName": "Finance and Control", "guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "msgCreationTime": 1664180603673, "originalEventType": "ENTITY_UPDATE", "directChange": true, "eventType": "EntityRelationshipAudit", "insertedAttributes": [], "changedAttributes": [], "deletedAttributes": [], "insertedRelationships": {"dataEntity": [{"guid": "863394a9-eb15-4673-bddd-e20b2fe7dc52", "typeName": "m4i_data_entity", "entityStatus": "ACTIVE", "displayText": "Cost Centre ", "relationshipType": "m4i_data_entity_assignment", "relationshipGuid": "17dac5e5-5afe-4dfc-9c8d-c4808d869e10", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_data_entity_assignment"}}]}, "changedRelationships": {}, "deletedRelationships": {}, "oldValue": {"typeName": "m4i_data_domain", "attributes": {"archimateReference": [], "replicatedTo": [], "replicatedFrom": [], "qualifiedName": "Finance and Control", "domainLead": [{"guid": "0a736fdd-df31-4deb-936d-fdf92812789b", "typeName": "m4i_person", "uniqueAttributes": {"qualifiedName": "albertjan.kroezen@vanoord.com", "email": "albertjan.kroezen@vanoord.com"}}], "name": "Finance and Control", "dataEntity": [], "definition": "This domain contains data related to Finance & Control which is relevant for budgeting, forecasting and monitoring on employee level. (update 23 September 2022 10:26)", "source": []}, "classifications": [], "createTime": 1649238419087, "createdBy": "admin", "customAttributes": null, "guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": 0, "proxy": false, "relationshipAttributes": {"ArchiMateReference": [], "domainLead": [{"guid": "0a736fdd-df31-4deb-936d-fdf92812789b", "typeName": "m4i_person", "entityStatus": "ACTIVE", "displayText": "Albert-Jan Kroezen", "relationshipType": "m4i_domainLead_assignment", "relationshipGuid": "53a50ae8-a056-494d-b324-c79c2002e619", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_domainLead_assignment"}}], "dataEntity": [], "source": [], "meanings": []}, "status": "ACTIVE", "updateTime": 1663922517090, "updatedBy": "atlas", "version": 0}, "newValue": {"typeName": "m4i_data_domain", "attributes": {"archimateReference": [], "replicatedTo": [], "replicatedFrom": [], "qualifiedName": "Finance and Control", "domainLead": [{"guid": "0a736fdd-df31-4deb-936d-fdf92812789b", "typeName": "m4i_person", "uniqueAttributes": {"qualifiedName": "albertjan.kroezen@vanoord.com", "email": "albertjan.kroezen@vanoord.com"}}], "name": "Finance and Control", "dataEntity": [{"guid": "863394a9-eb15-4673-bddd-e20b2fe7dc52", "typeName": "m4i_data_entity", "uniqueAttributes": {"qualifiedName": "finance-and-control--cost-centre"}}], "definition": "This domain contains data related to Finance & Control which is relevant for budgeting, forecasting and monitoring on employee level. (update 23 September 2022 10:26)", "source": []}, "classifications": [], "createTime": 1649238419087, "createdBy": "admin", "customAttributes": null, "guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": 0, "proxy": false, "relationshipAttributes": {"ArchiMateReference": [], "domainLead": [{"guid": "0a736fdd-df31-4deb-936d-fdf92812789b", "typeName": "m4i_person", "entityStatus": "ACTIVE", "displayText": "Albert-Jan Kroezen", "relationshipType": "m4i_domainLead_assignment", "relationshipGuid": "53a50ae8-a056-494d-b324-c79c2002e619", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_domainLead_assignment"}}], "dataEntity": [{"guid": "863394a9-eb15-4673-bddd-e20b2fe7dc52", "typeName": "m4i_data_entity", "entityStatus": "ACTIVE", "displayText": "Cost Centre ", "relationshipType": "m4i_data_entity_assignment", "relationshipGuid": "17dac5e5-5afe-4dfc-9c8d-c4808d869e10", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_data_entity_assignment"}}], "source": [], "meanings": []}, "status": "ACTIVE", "updateTime": 1664180603286, "updatedBy": "atlas", "version": 0}}'
	config_store.load({**config, **credentials})
	asl = SynchronizeAppsearchLocal()
	asl.open_local(config, credentials, config_store)
	res = await asl.map_local(kafka_msg3)
	json.loads(res)


@pytest.mark.asyncio
async def test_msg4():
	kafka_msg4 = '{"typeName": "m4i_data_entity", "qualifiedName": "finance-and-control--cost-centre", "guid": "863394a9-eb15-4673-bddd-e20b2fe7dc52", "msgCreationTime": 1664187000490, "originalEventType": "ENTITY_UPDATE", "directChange": true, "eventType": "EntityRelationshipAudit", "insertedAttributes": [], "changedAttributes": [], "deletedAttributes": [], "insertedRelationships": {"dataDomain": [{"guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "typeName": "m4i_data_domain", "entityStatus": "ACTIVE", "displayText": "Finance and Control", "relationshipType": "m4i_data_entity_assignment", "relationshipGuid": "6d811d2a-60fc-452a-ada2-7be06dd8f344", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_data_entity_assignment"}}]}, "changedRelationships": {}, "deletedRelationships": {}, "oldValue": {"typeName": "m4i_data_entity", "attributes": {"archimateReference": [], "replicatedTo": null, "replicatedFrom": null, "steward": [], "qualifiedName": "finance-and-control--cost-centre", "parentEntity": [], "source": [], "dataDomain": [], "childEntity": [], "name": "Cost Centre ", "definition": "A cost centre is a responsibility area to which costs can be allocated and that is used for management reporting and cost controlling; both company wide as on fiscal entity level within the Van Oord structure. Cost centres are used for differentiated assignment of overhead costs to organizational activities and are either linked to Business Units, Departments or General purposes. Each cost centre has an owner or manager who is responsible for a budget and for the costs allocated to it. (updated 30 july 2022 16:55)", "attributes": [{"guid": "5c065dfd-4af0-4780-b696-c852bd85e6ea", "typeName": "m4i_data_attribute", "uniqueAttributes": {"qualifiedName": "finance-and-control--cost-centre--job-name"}}, {"guid": "42e5e4df-be7f-4d0f-b8a1-5302725d9dc1", "typeName": "m4i_data_attribute", "uniqueAttributes": {"qualifiedName": "finance-and-control--cost-centre--cost-centre-employee"}}], "businessOwner": []}, "classifications": [], "createTime": 1649238419390, "createdBy": "admin", "customAttributes": null, "guid": "863394a9-eb15-4673-bddd-e20b2fe7dc52", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": 0, "proxy": false, "relationshipAttributes": {"ArchiMateReference": [], "steward": [], "dataDomain": [], "parentEntity": [], "childEntity": [], "attributes": [{"guid": "5c065dfd-4af0-4780-b696-c852bd85e6ea", "typeName": "m4i_data_attribute", "entityStatus": "ACTIVE", "displayText": "Job Name", "relationshipType": "m4i_data_entity_attribute_assignment", "relationshipGuid": "70542911-78bc-446d-ad49-e9511ff0cf5d", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_data_entity_attribute_assignment"}}, {"guid": "42e5e4df-be7f-4d0f-b8a1-5302725d9dc1", "typeName": "m4i_data_attribute", "entityStatus": "ACTIVE", "displayText": "Cost Centre Employee", "relationshipType": "m4i_data_entity_attribute_assignment", "relationshipGuid": "ed03b0cd-cf5f-42d0-b602-718b57a90f75", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_data_entity_attribute_assignment"}}], "source": [{"guid": "b00468d4-f309-4553-b8f5-54c6b7648551", "typeName": "m4i_source", "entityStatus": "ACTIVE", "displayText": "/po/FTE_Actuals/Data Dictionary_FTE Actuals.xlsm", "relationshipType": "m4i_referenceable_source_assignment", "relationshipGuid": "787f8306-02fe-493a-a5df-c22d17dd81d3", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_referenceable_source_assignment"}}], "businessOwner": [], "meanings": []}, "status": "ACTIVE", "updateTime": 1664186993994, "updatedBy": "atlas", "version": 0}, "newValue": {"typeName": "m4i_data_entity", "attributes": {"archimateReference": [], "replicatedTo": null, "replicatedFrom": null, "steward": [], "qualifiedName": "finance-and-control--cost-centre", "parentEntity": [], "source": [], "dataDomain": [{"guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "typeName": "m4i_data_domain", "uniqueAttributes": {"qualifiedName": "Finance and Control"}}], "childEntity": [], "name": "Cost Centre ", "definition": "A cost centre is a responsibility area to which costs can be allocated and that is used for management reporting and cost controlling; both company wide as on fiscal entity level within the Van Oord structure. Cost centres are used for differentiated assignment of overhead costs to organizational activities and are either linked to Business Units, Departments or General purposes. Each cost centre has an owner or manager who is responsible for a budget and for the costs allocated to it. (updated 30 july 2022 16:55)", "attributes": [{"guid": "5c065dfd-4af0-4780-b696-c852bd85e6ea", "typeName": "m4i_data_attribute", "uniqueAttributes": {"qualifiedName": "finance-and-control--cost-centre--job-name"}}, {"guid": "42e5e4df-be7f-4d0f-b8a1-5302725d9dc1", "typeName": "m4i_data_attribute", "uniqueAttributes": {"qualifiedName": "finance-and-control--cost-centre--cost-centre-employee"}}], "businessOwner": []}, "classifications": [], "createTime": 1649238419390, "createdBy": "admin", "customAttributes": null, "guid": "863394a9-eb15-4673-bddd-e20b2fe7dc52", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": 0, "proxy": false, "relationshipAttributes": {"ArchiMateReference": [], "steward": [], "dataDomain": [{"guid": "b6044c9a-61b3-4a02-acec-e028e1f2c951", "typeName": "m4i_data_domain", "entityStatus": "ACTIVE", "displayText": "Finance and Control", "relationshipType": "m4i_data_entity_assignment", "relationshipGuid": "6d811d2a-60fc-452a-ada2-7be06dd8f344", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_data_entity_assignment"}}], "parentEntity": [], "childEntity": [], "attributes": [{"guid": "5c065dfd-4af0-4780-b696-c852bd85e6ea", "typeName": "m4i_data_attribute", "entityStatus": "ACTIVE", "displayText": "Job Name", "relationshipType": "m4i_data_entity_attribute_assignment", "relationshipGuid": "70542911-78bc-446d-ad49-e9511ff0cf5d", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_data_entity_attribute_assignment"}}, {"guid": "42e5e4df-be7f-4d0f-b8a1-5302725d9dc1", "typeName": "m4i_data_attribute", "entityStatus": "ACTIVE", "displayText": "Cost Centre Employee", "relationshipType": "m4i_data_entity_attribute_assignment", "relationshipGuid": "ed03b0cd-cf5f-42d0-b602-718b57a90f75", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_data_entity_attribute_assignment"}}], "source": [{"guid": "b00468d4-f309-4553-b8f5-54c6b7648551", "typeName": "m4i_source", "entityStatus": "ACTIVE", "displayText": "/po/FTE_Actuals/Data Dictionary_FTE Actuals.xlsm", "relationshipType": "m4i_referenceable_source_assignment", "relationshipGuid": "787f8306-02fe-493a-a5df-c22d17dd81d3", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_referenceable_source_assignment"}}], "businessOwner": [], "meanings": []}, "status": "ACTIVE", "updateTime": 1664187000431, "updatedBy": "atlas", "version": 0}}'
	config_store.load({**config, **credentials})
	asl = SynchronizeAppsearchLocal()
	asl.open_local(config, credentials, config_store)
	
	res = await asl.map_local(kafka_msg4)

	expected_res = '{"id": "fbf78656-dcea-4bce-b849-70fec329c903", "creationTime": 1664190104173, "entityGuid": "863394a9-eb15-4673-bddd-e20b2fe7dc52", "changes": [{"propagate": true, "propagateDown": true, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.InsertPrefixToList", "name": "update breadcrumb guid", "key": "breadcrumbguid", "input_list": ["b6044c9a-61b3-4a02-acec-e028e1f2c951"]}, {"py/object": "m4i_flink_tasks.operation.core_operation.InsertPrefixToList", "name": "update breadcrumb name", "key": "breadcrumbname", "input_list": ["Finance and Control"]}, {"py/object": "m4i_flink_tasks.operation.core_operation.InsertPrefixToList", "name": "update breadcrumb type", "key": "breadcrumbtype", "input_list": ["m4i_data_domain"]}]}}]}'
	
	expected_res = json.loads(expected_res)
	res = json.loads(res)

	expected_res["id"] = -1
	expected_res["creationTime"] = -1

	res["id"] = -1
	res["creationTime"] = -1
	assert  res == expected_res


