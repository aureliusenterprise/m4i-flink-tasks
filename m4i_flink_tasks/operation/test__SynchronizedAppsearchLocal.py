from m4i_flink_tasks.operation.SynchronizeAppsearchLocal import SynchronizeAppsearchLocal
from m4i_atlas_core import ConfigStore, Entity
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

def test_msg1():
    config_store.load({**config, **credentials})
    
    asl = SynchronizeAppsearchLocal()
    asl.open_local(config, credentials, config_store)
    
    res = asl.map_local(msg1)
    expected_res = '{"id": "1fc0f72a-063e-4015-8bb6-6666d40de645", "creationTime": 1663832173352, "entityGuid": "7f4eae15-9b19-41cc-9ed1-8313bf1b5536", "changes": [{"propagate": false, "propagateDown": false, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.CreateLocalEntityProcessor", "name": "create entity with guid 7f4eae15-9b19-41cc-9ed1-8313bf1b5536 of type hdfs_path", "entity_guid": "7f4eae15-9b19-41cc-9ed1-8313bf1b5536", "entity_type": "hdfs_path"}, {"py/object": "m4i_flink_tasks.operation.core_operation.UpdateLocalAttributeProcessor", "name": "insert attribute name", "key": "name", "value": "test"}]}}]}'
    assert(res == expected_res)
    
    
    