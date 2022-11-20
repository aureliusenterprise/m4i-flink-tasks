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

from config import config
from credentials import credentials


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

	kafka_message = '{"kafka_notification":{"version":{"version":"1.0.0","versionParts":[1]},"msgCompressionKind":"NONE","msgSplitIdx":1,"msgSplitCount":1,"msgSourceIP":"10.244.3.109","msgCreatedBy":"","msgCreationTime":1668808488968,"message":{"eventTime":1668808488741,"operationType":"ENTITY_CREATE","type":"ENTITY_NOTIFICATION_V2","entity":{"typeName":"m4i_data_domain","attributes":{"qualifiedName":"5fcc603d-3c3b-4604-acbb-05afb8ce103b","name":"test2"},"classifications":[],"createTime":null,"createdBy":null,"customAttributes":null,"guid":"8762f125-7246-494d-a28a-c63e85c275af","homeId":null,"isIncomplete":false,"labels":[],"meanings":[],"provenanceType":null,"proxy":null,"relationshipAttributes":null,"status":null,"updateTime":null,"updatedBy":null,"version":null},"relationship":null}},"atlas_entity":{"typeName":"m4i_data_domain","attributes":{"archimateReference":[],"replicatedTo":null,"replicatedFrom":null,"qualifiedName":"5fcc603d-3c3b-4604-acbb-05afb8ce103b","domainLead":[],"name":"test2","dataEntity":[],"definition":null,"source":[],"typeAlias":null},"classifications":[],"createTime":1668808488741,"createdBy":"atlas","customAttributes":null,"guid":"8762f125-7246-494d-a28a-c63e85c275af","homeId":null,"isIncomplete":false,"labels":[],"meanings":[],"provenanceType":null,"proxy":null,"relationshipAttributes":{"ArchiMateReference":[],"domainLead":[],"dataEntity":[],"source":[],"meanings":[]},"status":"ACTIVE","updateTime":1668808488741,"updatedBy":"atlas","version":0},"msg_creation_time":1668808488968,"atlas_entity_audit":{},"supertypes":["m4i_data_domain","m4i_referenceable","Referenceable"]}'
    local_operation_local = PublishStateLocal()
    local_operation_local.open_local(config, credentials,store)

    res = local_operation_local.map_local(kafka_message)
    assert True
