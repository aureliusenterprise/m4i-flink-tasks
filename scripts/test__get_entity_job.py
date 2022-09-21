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