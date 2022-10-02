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
entity_audit =  asyncio.run(get_entity_audit(entity_guid = "85222630-dccb-4509-b106-57806bf99b58", access_token = access__token))
# data entity
entity_audit2 =  asyncio.run(get_entity_audit(entity_guid = "56dce48a-1bea-49c3-95a7-58083c6d2208", access_token = access__token))

