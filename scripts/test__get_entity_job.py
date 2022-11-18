# -*- coding: utf-8 -*-
import pytest

from m4i_flink_tasks.operation.GetEntityLocal import GetEntityLocal

from config import config
from credentials import credentials
from m4i_atlas_core import AtlasChangeMessage, ConfigStore, EntityAuditAction, get_entity_by_guid, get_keycloak_token
from m4i_flink_tasks.DeadLetterBoxMessage import DeadLetterBoxMesage
# store = ConfigStore.get_instance()
# store.load({**config, **credentials})

@pytest.fixture(autouse=True)
def store():
    store = ConfigStore.get_instance()
    store.load({**config, **credentials})

    yield store

    store.reset()
    
    
def test__get_entity():
    msg = """
        {
    	"version": {
    		"version": "1.0.0",
    		"versionParts": [
    			1
    		]
    	},
    	"msgCompressionKind": "NONE",
    	"msgSplitIdx": 1,
    	"msgSplitCount": 1,
    	"msgSourceIP": "10.244.3.95",
    	"msgCreatedBy": "",
    	"msgCreationTime": 1668766221607,
    	"spooled": false,
    	"message": {
    		"type": "ENTITY_NOTIFICATION_V2",
    		"entity": {
    			"typeName": "m4i_data_domain",
    			"attributes": {
    				"qualifiedName": "a1f469ee-77b5-4efa-a54e-8bed7fa259c2",
    				"name": "HR",
    				"definition": "This i s tyhe hR department",
    				"typeAlias": "Department"
    			},
    			"guid": "7069b2cf-b7bb-4954-a858-b80a7702e85b",
    			"displayText": "HR",
    			"isIncomplete": false
    		},
    		"operationType": "ENTITY_CREATE",
    		"eventTime": 1668766221174
    	}
        """
    cl = GetEntityLocal()
    res= cl.map_local(msg)
    cl.get_access_token()

# def test__get_relationship():
#     msg =  '{"version":{"version":"1.0.0","versionParts":[1]},"msgCompressionKind":"NONE","msgSplitIdx":1,"msgSplitCount":1,"msgSourceIP":"10.244.2.147","msgCreatedBy":"","msgCreationTime":1666818667739,"spooled":false,"message":{"type":"ENTITY_NOTIFICATION_V2","relationship":{"typeName":"m4i_data_attribute_business_owner_assignment","guid":"5fe56ca3-9dd9-44f8-bd5d-3effd14a1bc8","status":"ACTIVE","propagateTags":"NONE","label":"r:m4i_data_attribute_business_owner_assignment","end1":{"guid":"45e581d9-452c-4d48-98e0-452b7a109efb","typeName":"m4i_person"},"end2":{"guid":"b4bf70d3-1121-408e-bcf9-39e96a13d980","typeName":"m4i_data_attribute","uniqueAttributes":{"qualifiedName":"personnel-and-organization--personnel--internal--functional-organization"}}},"operationType":"RELATIONSHIP_CREATE","eventTime":1666818547291}}'
#     cl = GetEntityLocal()
#     res= cl.map_local(msg)
#     cl.get_access_token()
