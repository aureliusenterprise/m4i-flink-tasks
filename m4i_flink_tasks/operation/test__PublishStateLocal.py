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


@pytest.fixture(autouse=True)
def store():
    config_store = ConfigStore.get_instance()
    config_store.load({**config, **credentials})

    yield config_store

    config_store.reset()
# END store






def test__map_local1(store):
	
	kafka_message = '{"kafka_notification": {"version": {"version": "1.0.0", "versionParts": [1]}, "msgCompressionKind": "NONE", "msgSplitIdx": 1, "msgSplitCount": 1, "msgSourceIP": "10.0.2.4", "msgCreatedBy": "", "msgCreationTime": 1664804764843, "message": {"eventTime": 1664804764792, "operationType": "ENTITY_UPDATE", "type": "ENTITY_NOTIFICATION_V2", "entity": {"typeName": "m4i_data_entity", "attributes": {"archimateReference": [], "qualifiedName": "58d253b5-bb9e-440d-bf52-d0c7edd35523", "name": "test-data-entity-14545", "definition": "test-data-entity-14545"}, "classifications": [], "createTime": null, "createdBy": null, "customAttributes": null, "guid": "f135948a-7c48-4c70-9f1b-9048fcfc761d", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": null, "proxy": null, "relationshipAttributes": null, "status": "ACTIVE", "updateTime": null, "updatedBy": null, "version": null}, "relationship": null}}, "atlas_entity": {"typeName": "m4i_data_entity", "attributes": {"archimateReference": [], "replicatedTo": null, "replicatedFrom": null, "steward": [], "qualifiedName": "58d253b5-bb9e-440d-bf52-d0c7edd35523", "parentEntity": [], "source": [], "dataDomain": [{"guid": "9aa9f003-cde0-42cb-881d-65d4c396d3cf", "typeName": "m4i_data_domain", "uniqueAttributes": {"qualifiedName": "030d55a2-aa74-489c-a6bb-f328f10d150e"}}], "childEntity": [], "name": "test-data-entity-14545", "definition": "test-data-entity-14545", "attributes": [], "businessOwner": []}, "classifications": [], "createTime": 1664804722018, "createdBy": "atlas", "customAttributes": null, "guid": "f135948a-7c48-4c70-9f1b-9048fcfc761d", "homeId": null, "isIncomplete": false, "labels": [], "meanings": [], "provenanceType": null, "proxy": null, "relationshipAttributes": {"ArchiMateReference": [], "steward": [], "dataDomain": [{"guid": "9aa9f003-cde0-42cb-881d-65d4c396d3cf", "typeName": "m4i_data_domain", "entityStatus": "ACTIVE", "displayText": "test-data-domain-14545", "relationshipType": "m4i_data_entity_assignment", "relationshipGuid": "3255de76-73de-4a35-9660-165e5d908df8", "relationshipStatus": "ACTIVE", "relationshipAttributes": {"typeName": "m4i_data_entity_assignment"}}], "parentEntity": [], "childEntity": [], "attributes": [], "source": [], "businessOwner": [], "meanings": []}, "status": "ACTIVE", "updateTime": 1664804764792, "updatedBy": "atlas", "version": 0}, "msg_creation_time": 1664804764843}'
	
	local_operation_local = PublishStateLocal()
	local_operation_local.open_local(config, credentials,store)
   
	res = local_operation_local.map_local(kafka_message)

	assert True





