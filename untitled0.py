# -*- coding: utf-8 -*-
"""
Created on Thu Sep 29 15:53:11 2022

@author: andre
"""
from m4i_flink_tasks import EntityMessage
kafka_notification = """
{
	"typeName": "m4i_data_domain",
	"qualifiedName": "ce628e2e-7785-437f-bae1-04c6662b22bd",
	"guid": "09f4ae07-499b-489e-896a-6c5e9f44571f",
	"msgCreationTime": 1664440384689,
	"originalEventType": "ENTITY_CREATE",
	"directChange": true,
	"eventType": "EntityCreated",
	"insertedAttributes": [
		"qualifiedName",
		"name",
		"definition"
	],
	"changedAttributes": [],
	"deletedAttributes": [],
	"insertedRelationships": {
		"ArchiMateReference": [],
		"domainLead": [],
		"dataEntity": [],
		"source": [],
		"meanings": []
	},
	"changedRelationships": {},
	"deletedRelationships": {},
	"oldValue": {},
	"newValue": {
		"typeName": "m4i_data_domain",
		"attributes": {
			"archimateReference": [],
			"replicatedTo": null,
			"replicatedFrom": null,
			"qualifiedName": "ce628e2e-7785-437f-bae1-04c6662b22bd",
			"domainLead": [],
			"name": "Test domain",
			"dataEntity": [],
			"definition": "Andreas defines a new domain",
			"source": []
		},
		"classifications": [],
		"createTime": 1664440384462,
		"createdBy": "atlas",
		"customAttributes": null,
		"guid": "09f4ae07-499b-489e-896a-6c5e9f44571f",
		"homeId": null,
		"isIncomplete": false,
		"labels": [],
		"meanings": [],
		"provenanceType": null,
		"proxy": null,
		"relationshipAttributes": {
			"ArchiMateReference": [],
			"domainLead": [],
			"dataEntity": [],
			"source": [],
			"meanings": []
		},
		"status": "ACTIVE",
		"updateTime": 1664440384462,
		"updatedBy": "atlas",
		"version": 0
	}
}
"""
entity_message = EntityMessage.from_json((kafka_notification))
print(entity_message.qualified_name)

input_entity = entity_message.new_value
print(input_entity.attributes.unmapped_attributes['name'])

#%%
import json
from m4i_flink_tasks.synchronize_app_search.AppSearchDocument import AppSearchDocument
app_search_document = AppSearchDocument(id="0815",
            guid = "0815",
            sourcetype = "m4i_data_domain",
            typename = "m4i_data_domain",
            m4isourcetype = ["m4i_data_domain"],
            supertypenames = ["m4i_data_domain"],
            name="helo worlks",
            referenceablequalifiedname="helo world"
        )  
print(app_search_document.to_json())  
json.loads(app_search_document.to_json())

#%%
import asyncio
from m4i_flink_tasks.synchronize_app_search import get_super_types_names,get_m4i_source_types,get_source_type
from config import config
from credentials import credentials
from m4i_atlas_core import ConfigStore
config_store = ConfigStore.get_instance()
config_store.load({**config, **credentials})
await get_super_types_names("m4i_data_entity")
asyncio.run( get_super_types_names("m4i_data_entity"))

#%%
import asyncio
from m4i_flink_tasks.synchronize_app_search import get_super_types_names,get_m4i_source_types,get_source_type
from config import config
from credentials import credentials
from m4i_atlas_core import ConfigStore
from m4i_atlas_core import get_keycloak_token
from m4i_atlas_core import get_entity_audit
from m4i_atlas_core import Entity

config_store = ConfigStore.get_instance()
config_store.load({**config, **credentials})

access__token = get_keycloak_token()
# data domain
entity_audit =  asyncio.run(get_entity_audit(entity_guid = "e3b4f3e8-86cf-4ad1-8163-9b472243a8aa", access_token = access__token))
# data entity
entity_audit2 =  asyncio.run(get_entity_audit(entity_guid = "99a10a2e-55a8-4d4d-b1e0-779be349a31c", access_token = access__token))

#%%
kafka_notification = """
{"id": "766fe40c-8ce8-4d83-b563-a468f97895d0", "creationTime": 1664899578266, "entityGuid": "55d25b6b-d219-4392-bb35-5332c1aa8e47", "changes": {"propagate": false, "propagateDown": false, "operation": {"py/object": "m4i_flink_tasks.operation.core_operation.Sequence", "name": "update and inser attributes", "steps": [{"py/object": "m4i_flink_tasks.operation.core_operation.AddElementToListProcessor", "name": "update attribute derivedperson", "key": "derivedperson", "value": "asd"}, {"py/object": "m4i_flink_tasks.operation.core_operation.AddElementToListProcessor", "name": "update attribute derivedpersonguid", "key": "derivedpersonguid", "value": "2678b2aa-21cb-47b0-978c-fca2429cb511"}]}}}
"""
oe = OperationEvent.from_json(kafka_notification)
        
