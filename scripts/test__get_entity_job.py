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
    msg = '{"version":{"version":"1.0.0","versionParts":[1]},"msgCompressionKind":"NONE","msgSplitIdx":1,"msgSplitCount":1,"msgSourceIP":"10.244.2.233","msgCreatedBy":"","msgCreationTime":1663702503306,"spooled":false,"message":{"type":"ENTITY_NOTIFICATION_V2","entity":{"typeName":"hdfs_path","attributes":{"path":"gfhj","createTime":1663624800000,"qualifiedName":"fghj","name":"fghj"},"guid":"1fe2234e-1925-4296-bf7e-a3bf6bfc4d75","displayText":"fghj","isIncomplete":false},"operationType":"ENTITY_CREATE","eventTime":1663702503065}}'
    cl = GetEntityLocal()
    res= cl.map_local(msg)
    cl.get_access_token()

def test__get_relationship():
    msg =  '{"version":{"version":"1.0.0","versionParts":[1]},"msgCompressionKind":"NONE","msgSplitIdx":1,"msgSplitCount":1,"msgSourceIP":"10.244.2.147","msgCreatedBy":"","msgCreationTime":1666818667739,"spooled":false,"message":{"type":"ENTITY_NOTIFICATION_V2","relationship":{"typeName":"m4i_data_attribute_business_owner_assignment","guid":"5fe56ca3-9dd9-44f8-bd5d-3effd14a1bc8","status":"ACTIVE","propagateTags":"NONE","label":"r:m4i_data_attribute_business_owner_assignment","end1":{"guid":"45e581d9-452c-4d48-98e0-452b7a109efb","typeName":"m4i_person"},"end2":{"guid":"b4bf70d3-1121-408e-bcf9-39e96a13d980","typeName":"m4i_data_attribute","uniqueAttributes":{"qualifiedName":"personnel-and-organization--personnel--internal--functional-organization"}}},"operationType":"RELATIONSHIP_CREATE","eventTime":1666818547291}}'
    cl = GetEntityLocal()
    res= cl.map_local(msg)
    cl.get_access_token()
