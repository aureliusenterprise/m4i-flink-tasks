import pytest

from .synchronize_app_search import get_super_types, handle_updated_attributes, handle_inserted_relationships
from .synchronize_app_search import *
from config import config
from credentials import credentials

from m4i_atlas_core import ConfigStore
from AtlasEntityChangeMessage import EntityMessage
from elastic_app_search import Client
from .elastic import * 
from m4i_atlas_core import *
import json 
import requests
import asyncio

test_engine_name = "test_synchronize_app_search_engine"


@pytest.fixture(autouse=True)
def store():
    config_store = ConfigStore.get_instance()
    config_store.load({**config, **credentials})

    yield config_store

    config_store.reset()
# END store

@pytest.fixture(autouse=True)
def create_test_engine(store):
    app_search = make_elastic_app_search_connect()
    app_search.create_engine(test_engine_name)
# END delete_test_engine

@pytest.fixture(autouse=True)
def delete_test_engine(store):
    app_search = make_elastic_app_search_connect()
    app_search.delete_engine(test_engine_name)
# END delete_test_engine


async def create_atlas_entites():
    access_token = get_keycloak_token()
    input_entity = Entity(type_name="m4i_data_domain", attributes=M4IAttributes(qualified_name="test_data_domain_0",unmapped_attributes={"name" : "test_data_domain"}))
    res = await create_entities(entities = input_entity, access_token=access_token)


async def delete_atlas_entities(entity_guids = list):
    access_token = get_keycloak_token()
    res = await delete_entity_hard(entity_guids, access_token)
    

@pytest.mark.asyncio
async def test__get_super_types():
    type_name = "m4i_kafka_field"

    super_types = await get_super_types(type_name)

    assert len(super_types) == 4
# END test__get_super_types


async def test__create_document():
    f = open('input_entity.json')   
    kafka_notification = json.load(f)

    g = open('elastic_document.json')   
    elastic_document = json.load(g)


    input_entity = kafka_notification["newValue"]
    generated_output_entity = asyncio.run(create_document(Entity.from_json(json.dumps(input_entity))))
    assert generated_output_entity == elastic_document



# @pytest.mark.asyncio
# async def test__handle_updated_attributes():
#     config_store = ConfigStore.get_instance()
#     api_key = config_store.get("elastic_search_passwd")
    
#     app_search = Client(
#     base_endpoint="atlas-search.ent.westeurope.azure.elastic-cloud.com/api/as/v1",
#     api_key=api_key,
#     use_https=True
#     )

#     kafka_notification = '''
#     {
#   "typeName": "m4i_data_domain",
#   "qualifiedName": "finance",
#   "guid": "ad49630e-7885-4560-aad3-8fbe743eb0ec",
#   "originalEventType": "ENTITY_CREATE",
#   "directChange": true,
#   "eventType": "EntityCreated",
#   "insertedAttributes": [
#     "archimateReference",
#     "replicatedTo",
#     "replicatedFrom",
#     "qualifiedName",
#     "domainLead",
#     "name",
#     "dataEntity",
#     "definition",
#     "source"
#   ],
#   "changedAttributes": [],
#   "deletedAttributes": [],
#   "insertedRelationships": {
#     "ArchiMateReference": [],
#     "domainLead": [
#       {
#         "guid": "e399f28b-9f72-40c9-b8ec-1b25d8f72dff",
#         "typeName": "m4i_person",
#         "entityStatus": "ACTIVE",
#         "displayText": "Eveline Daphne",
#         "relationshipType": "m4i_domainLead_assignment",
#         "relationshipGuid": "36a5dd79-fd8e-4197-aec1-2c070d78ba53",
#         "relationshipStatus": "ACTIVE",
#         "relationshipAttributes": {
#           "typeName": "m4i_domainLead_assignment"
#         }
#       }
#     ],
#     "dataEntity": [
#       {
#         "guid": "846d3d77-439e-4f96-bb67-c22be61097cf",
#         "typeName": "m4i_data_entity",
#         "entityStatus": "ACTIVE",
#         "displayText": "Cost Centre",
#         "relationshipType": "m4i_data_entity_assignment",
#         "relationshipGuid": "8cab5046-a81a-4d26-a393-957cb86fe0ef",
#         "relationshipStatus": "ACTIVE",
#         "relationshipAttributes": {
#           "typeName": "m4i_data_entity_assignment"
#         }
#       }
#     ],
#     "source": [
#       {
#         "guid": "0d6473a0-5e57-4dfd-b741-da6013a68200",
#         "typeName": "m4i_source",
#         "entityStatus": "ACTIVE",
#         "displayText": "/hr/hr/Data Dictionary_template.xlsm",
#         "relationshipType": "m4i_referenceable_source_assignment",
#         "relationshipGuid": "7b141b71-9821-41ff-8ac5-32f760a617f4",
#         "relationshipStatus": "ACTIVE",
#         "relationshipAttributes": {
#           "typeName": "m4i_referenceable_source_assignment"
#         }
#       }
#     ],
#     "meanings": []
#   },
#   "changedRelationships": {},
#   "deletedRelationships": {},
#   "oldValue": {},
#   "newValue": {
#     "typeName": "m4i_data_domain",
#     "attributes": {
#       "archimateReference": [],
#       "replicatedTo": null,
#       "replicatedFrom": null,
#       "qualifiedName": "finance",
#       "domainLead": [
#         {
#           "guid": "e399f28b-9f72-40c9-b8ec-1b25d8f72dff",
#           "typeName": "m4i_person",
#           "uniqueAttributes": {
#             "qualifiedName": "eveline.daphne@email.com",
#             "email": "eveline.daphne@email.com"
#           }
#         }
#       ],
#       "name": "Finance",
#       "dataEntity": [
#         {
#           "guid": "846d3d77-439e-4f96-bb67-c22be61097cf",
#           "typeName": "m4i_data_entity",
#           "uniqueAttributes": {
#             "qualifiedName": "finance--cost-centre"
#           }
#         }
#       ],
#       "definition": "This data domain contains data from the finance department (8th update)",
#       "source": []
#     },
#     "classifications": [],
#     "createTime": 1656493552392,
#     "createdBy": "admin",
#     "customAttributes": null,
#     "guid": "ad49630e-7885-4560-aad3-8fbe743eb0ec",
#     "homeId": null,
#     "isIncomplete": false,
#     "labels": [],
#     "meanings": [],
#     "provenanceType": 0,
#     "proxy": false,
#     "relationshipAttributes": {
#       "ArchiMateReference": [],
#       "domainLead": [
#         {
#           "guid": "e399f28b-9f72-40c9-b8ec-1b25d8f72dff",
#           "typeName": "m4i_person",
#           "entityStatus": "ACTIVE",
#           "displayText": "Eveline Daphne",
#           "relationshipType": "m4i_domainLead_assignment",
#           "relationshipGuid": "36a5dd79-fd8e-4197-aec1-2c070d78ba53",
#           "relationshipStatus": "ACTIVE",
#           "relationshipAttributes": {
#             "typeName": "m4i_domainLead_assignment"
#           }
#         }
#       ],
#       "dataEntity": [
#         {
#           "guid": "846d3d77-439e-4f96-bb67-c22be61097cf",
#           "typeName": "m4i_data_entity",
#           "entityStatus": "ACTIVE",
#           "displayText": "Cost Centre",
#           "relationshipType": "m4i_data_entity_assignment",
#           "relationshipGuid": "8cab5046-a81a-4d26-a393-957cb86fe0ef",
#           "relationshipStatus": "ACTIVE",
#           "relationshipAttributes": {
#             "typeName": "m4i_data_entity_assignment"
#           }
#         }
#       ],
#       "source": [
#         {
#           "guid": "0d6473a0-5e57-4dfd-b741-da6013a68200",
#           "typeName": "m4i_source",
#           "entityStatus": "ACTIVE",
#           "displayText": "/hr/hr/Data Dictionary_template.xlsm",
#           "relationshipType": "m4i_referenceable_source_assignment",
#           "relationshipGuid": "7b141b71-9821-41ff-8ac5-32f760a617f4",
#           "relationshipStatus": "ACTIVE",
#           "relationshipAttributes": {
#             "typeName": "m4i_referenceable_source_assignment"
#           }
#         }
#       ],
#       "meanings": []
#     },
#     "status": "ACTIVE",
#     "updateTime": 1657099979499,
#     "updatedBy": "admin",
#     "version": 0
#   }
# }
#     '''

#     entity_doc = '''
#     {
#         "id": "ad49630e-7885-4560-aad3-8fbe743eb0ec",
#         "guid": "ad49630e-7885-4560-aad3-8fbe743eb0ec",
#         "referenceablequalifiedname": "finance",
#         "typename": "m4i_data_domain",
#         "sourcetype": "Business",
#         "m4isourcetype": ["m4i_data_domain", "m4i_data_domain"],
#         "supertypenames": ["Referenceable", "m4i_referenceable", "m4i_data_domain",
#             "m4i_data_domain"
#         ],
#         "name": "Finance",
#         "definition": "This data domain contains data from the finance department (8th update)",
#         "email": "NULL"
#     }'''
#     entity_message = EntityMessage.from_json(kafka_notification)
#     updated_docs = await handle_updated_attributes(entity_message, entity_message.new_value,entity_message.inserted_relationships, app_search, entity_doc)
#     assert updated_docs["definiton"] == "This data domain contains data from the finance department (8th update)"


# @pytest.mark.asyncio
# async def test__handle_updated_attributes():
#     config_store = ConfigStore.get_instance()
#     api_key = config_store.get("elastic_search_passwd")
#     app_search = Client(
#     base_endpoint="atlas-search.ent.westeurope.azure.elastic-cloud.com/api/as/v1",
#     api_key=api_key,
#     use_https=True
#     )

#     kafka_notification = '''
#     {
#   "typeName": "m4i_data_domain",
#   "qualifiedName": "finance",
#   "guid": "ad49630e-7885-4560-aad3-8fbe743eb0ec",
#   "originalEventType": "ENTITY_CREATE",
#   "directChange": true,
#   "eventType": "EntityCreated",
#   "insertedAttributes": [
#     "archimateReference",
#     "replicatedTo",
#     "replicatedFrom",
#     "qualifiedName",
#     "domainLead",
#     "name",
#     "dataEntity",
#     "definition",
#     "source"
#   ],
#   "changedAttributes": [],
#   "deletedAttributes": [],
#   "insertedRelationships": {
#     "ArchiMateReference": [],
#     "domainLead": [
#       {
#         "guid": "e399f28b-9f72-40c9-b8ec-1b25d8f72dff",
#         "typeName": "m4i_person",
#         "entityStatus": "ACTIVE",
#         "displayText": "Eveline Daphne",
#         "relationshipType": "m4i_domainLead_assignment",
#         "relationshipGuid": "36a5dd79-fd8e-4197-aec1-2c070d78ba53",
#         "relationshipStatus": "ACTIVE",
#         "relationshipAttributes": {
#           "typeName": "m4i_domainLead_assignment"
#         }
#       }
#     ],
#     "dataEntity": [
#       {
#         "guid": "846d3d77-439e-4f96-bb67-c22be61097cf",
#         "typeName": "m4i_data_entity",
#         "entityStatus": "ACTIVE",
#         "displayText": "Cost Centre",
#         "relationshipType": "m4i_data_entity_assignment",
#         "relationshipGuid": "8cab5046-a81a-4d26-a393-957cb86fe0ef",
#         "relationshipStatus": "ACTIVE",
#         "relationshipAttributes": {
#           "typeName": "m4i_data_entity_assignment"
#         }
#       }
#     ],
#     "source": [
#       {
#         "guid": "0d6473a0-5e57-4dfd-b741-da6013a68200",
#         "typeName": "m4i_source",
#         "entityStatus": "ACTIVE",
#         "displayText": "/hr/hr/Data Dictionary_template.xlsm",
#         "relationshipType": "m4i_referenceable_source_assignment",
#         "relationshipGuid": "7b141b71-9821-41ff-8ac5-32f760a617f4",
#         "relationshipStatus": "ACTIVE",
#         "relationshipAttributes": {
#           "typeName": "m4i_referenceable_source_assignment"
#         }
#       }
#     ],
#     "meanings": []
#   },
#   "changedRelationships": {},
#   "deletedRelationships": {},
#   "oldValue": {},
#   "newValue": {
#     "typeName": "m4i_data_domain",
#     "attributes": {
#       "archimateReference": [],
#       "replicatedTo": null,
#       "replicatedFrom": null,
#       "qualifiedName": "finance",
#       "domainLead": [
#         {
#           "guid": "e399f28b-9f72-40c9-b8ec-1b25d8f72dff",
#           "typeName": "m4i_person",
#           "uniqueAttributes": {
#             "qualifiedName": "eveline.daphne@email.com",
#             "email": "eveline.daphne@email.com"
#           }
#         }
#       ],
#       "name": "Finance",
#       "dataEntity": [
#         {
#           "guid": "846d3d77-439e-4f96-bb67-c22be61097cf",
#           "typeName": "m4i_data_entity",
#           "uniqueAttributes": {
#             "qualifiedName": "finance--cost-centre"
#           }
#         }
#       ],
#       "definition": "This data domain contains data from the finance department (8th update)",
#       "source": []
#     },
#     "classifications": [],
#     "createTime": 1656493552392,
#     "createdBy": "admin",
#     "customAttributes": null,
#     "guid": "ad49630e-7885-4560-aad3-8fbe743eb0ec",
#     "homeId": null,
#     "isIncomplete": false,
#     "labels": [],
#     "meanings": [],
#     "provenanceType": 0,
#     "proxy": false,
#     "relationshipAttributes": {
#       "ArchiMateReference": [],
#       "domainLead": [
#         {
#           "guid": "e399f28b-9f72-40c9-b8ec-1b25d8f72dff",
#           "typeName": "m4i_person",
#           "entityStatus": "ACTIVE",
#           "displayText": "Eveline Daphne",
#           "relationshipType": "m4i_domainLead_assignment",
#           "relationshipGuid": "36a5dd79-fd8e-4197-aec1-2c070d78ba53",
#           "relationshipStatus": "ACTIVE",
#           "relationshipAttributes": {
#             "typeName": "m4i_domainLead_assignment"
#           }
#         }
#       ],
#       "dataEntity": [
#         {
#           "guid": "846d3d77-439e-4f96-bb67-c22be61097cf",
#           "typeName": "m4i_data_entity",
#           "entityStatus": "ACTIVE",
#           "displayText": "Cost Centre",
#           "relationshipType": "m4i_data_entity_assignment",
#           "relationshipGuid": "8cab5046-a81a-4d26-a393-957cb86fe0ef",
#           "relationshipStatus": "ACTIVE",
#           "relationshipAttributes": {
#             "typeName": "m4i_data_entity_assignment"
#           }
#         }
#       ],
#       "source": [
#         {
#           "guid": "0d6473a0-5e57-4dfd-b741-da6013a68200",
#           "typeName": "m4i_source",
#           "entityStatus": "ACTIVE",
#           "displayText": "/hr/hr/Data Dictionary_template.xlsm",
#           "relationshipType": "m4i_referenceable_source_assignment",
#           "relationshipGuid": "7b141b71-9821-41ff-8ac5-32f760a617f4",
#           "relationshipStatus": "ACTIVE",
#           "relationshipAttributes": {
#             "typeName": "m4i_referenceable_source_assignment"
#           }
#         }
#       ],
#       "meanings": []
#     },
#     "status": "ACTIVE",
#     "updateTime": 1657099979499,
#     "updatedBy": "admin",
#     "version": 0
#   }
# }
#     '''

#     entity_doc = '''
#     {
#         "id": "ad49630e-7885-4560-aad3-8fbe743eb0ec",
#         "guid": "ad49630e-7885-4560-aad3-8fbe743eb0ec",
#         "referenceablequalifiedname": "finance",
#         "typename": "m4i_data_domain",
#         "sourcetype": "Business",
#         "m4isourcetype": ["m4i_data_domain", "m4i_data_domain"],
#         "supertypenames": ["Referenceable", "m4i_referenceable", "m4i_data_domain",
#             "m4i_data_domain"
#         ],
#         "name": "Finance",
#         "definition": "This data domain contains data from the finance department (8th update)",
#         "email": "NULL"
#     }'''
#     entity_message = EntityMessage.from_json(kafka_notification)
#     updated_docs = await handle_updated_attributes(entity_message, entity_message.new_value,entity_message.inserted_relationships, app_search, entity_doc)
#     """This entity has exactyly 3 child entities lower in the hierarchy and all of these entity documents should be updated."""
#     assert len(updated_docs) == 4





# kafka_notification ='''{
#   "typeName": "m4i_data_domain",
#   "qualifiedName": "finance",
#   "guid": "ad49630e-7885-4560-aad3-8fbe743eb0ec",
#   "originalEventType": "ENTITY_UPDATE",
#   "directChange": false,
#   "eventType": "EntityChanged",
#   "insertedAttributes": [],
#   "changedAttributes": [
#     "definition"
#   ],
#   "deletedAttributes": [],
#   "insertedRelationships": {},
#   "changedRelationships": {},
#   "deletedRelationships": {},
#   "oldValue": {
#     "typeName": "m4i_data_domain",
#     "attributes": {
#       "archimateReference": [],
#       "replicatedTo": null,
#       "replicatedFrom": null,
#       "qualifiedName": "finance",
#       "domainLead": [
#         {
#           "guid": "e399f28b-9f72-40c9-b8ec-1b25d8f72dff",
#           "typeName": "m4i_person",
#           "uniqueAttributes": {
#             "qualifiedName": "eveline.daphne@email.com",
#             "email": "eveline.daphne@email.com"
#           }
#         }
#       ],
#       "name": "Finance",
#       "dataEntity": [
#         {
#           "guid": "846d3d77-439e-4f96-bb67-c22be61097cf",
#           "typeName": "m4i_data_entity",
#           "uniqueAttributes": {
#             "qualifiedName": "finance--cost-centre"
#           }
#         }
#       ],
#       "definition": "This data domain contains data from the finance department (7th update)",
#       "source": []
#     },
#     "classifications": [],
#     "createTime": 1656493552392,
#     "createdBy": "admin",
#     "customAttributes": null,
#     "guid": "ad49630e-7885-4560-aad3-8fbe743eb0ec",
#     "homeId": null,
#     "isIncomplete": false,
#     "labels": [],
#     "meanings": [],
#     "provenanceType": 0,
#     "proxy": false,
#     "relationshipAttributes": {
#       "ArchiMateReference": [],
#       "domainLead": [
#         {
#           "guid": "e399f28b-9f72-40c9-b8ec-1b25d8f72dff",
#           "typeName": "m4i_person",
#           "entityStatus": "ACTIVE",
#           "displayText": "Eveline Daphne",
#           "relationshipType": "m4i_domainLead_assignment",
#           "relationshipGuid": "36a5dd79-fd8e-4197-aec1-2c070d78ba53",
#           "relationshipStatus": "ACTIVE",
#           "relationshipAttributes": {
#             "typeName": "m4i_domainLead_assignment"
#           }
#         }
#       ],
#       "dataEntity": [
#         {
#           "guid": "846d3d77-439e-4f96-bb67-c22be61097cf",
#           "typeName": "m4i_data_entity",
#           "entityStatus": "ACTIVE",
#           "displayText": "Cost Centre",
#           "relationshipType": "m4i_data_entity_assignment",
#           "relationshipGuid": "8cab5046-a81a-4d26-a393-957cb86fe0ef",
#           "relationshipStatus": "ACTIVE",
#           "relationshipAttributes": {
#             "typeName": "m4i_data_entity_assignment"
#           }
#         }
#       ],
#       "source": [
#         {
#           "guid": "0d6473a0-5e57-4dfd-b741-da6013a68200",
#           "typeName": "m4i_source",
#           "entityStatus": "ACTIVE",
#           "displayText": "/hr/hr/Data Dictionary_template.xlsm",
#           "relationshipType": "m4i_referenceable_source_assignment",
#           "relationshipGuid": "7b141b71-9821-41ff-8ac5-32f760a617f4",
#           "relationshipStatus": "ACTIVE",
#           "relationshipAttributes": {
#             "typeName": "m4i_referenceable_source_assignment"
#           }
#         }
#       ],
#       "meanings": []
#     },
#     "status": "ACTIVE",
#     "updateTime": 1657099718659,
#     "updatedBy": "admin",
#     "version": 0
#   },
#   "newValue": {
#     "typeName": "m4i_data_domain",
#     "attributes": {
#       "archimateReference": [],
#       "replicatedTo": null,
#       "replicatedFrom": null,
#       "qualifiedName": "finance",
#       "domainLead": [
#         {
#           "guid": "e399f28b-9f72-40c9-b8ec-1b25d8f72dff",
#           "typeName": "m4i_person",
#           "uniqueAttributes": {
#             "qualifiedName": "eveline.daphne@email.com",
#             "email": "eveline.daphne@email.com"
#           }
#         }
#       ],
#       "name": "Finance",
#       "dataEntity": [
#         {
#           "guid": "846d3d77-439e-4f96-bb67-c22be61097cf",
#           "typeName": "m4i_data_entity",
#           "uniqueAttributes": {
#             "qualifiedName": "finance--cost-centre"
#           }
#         }
#       ],
#       "definition": "This data domain contains data from the finance department (8th update)",
#       "source": []
#     },
#     "classifications": [],
#     "createTime": 1656493552392,
#     "createdBy": "admin",
#     "customAttributes": null,
#     "guid": "ad49630e-7885-4560-aad3-8fbe743eb0ec",
#     "homeId": null,
#     "isIncomplete": false,
#     "labels": [],
#     "meanings": [],
#     "provenanceType": 0,
#     "proxy": false,
#     "relationshipAttributes": {
#       "ArchiMateReference": [],
#       "domainLead": [
#         {
#           "guid": "e399f28b-9f72-40c9-b8ec-1b25d8f72dff",
#           "typeName": "m4i_person",
#           "entityStatus": "ACTIVE",
#           "displayText": "Eveline Daphne",
#           "relationshipType": "m4i_domainLead_assignment",
#           "relationshipGuid": "36a5dd79-fd8e-4197-aec1-2c070d78ba53",
#           "relationshipStatus": "ACTIVE",
#           "relationshipAttributes": {
#             "typeName": "m4i_domainLead_assignment"
#           }
#         }
#       ],
#       "dataEntity": [
#         {
#           "guid": "846d3d77-439e-4f96-bb67-c22be61097cf",
#           "typeName": "m4i_data_entity",
#           "entityStatus": "ACTIVE",
#           "displayText": "Cost Centre",
#           "relationshipType": "m4i_data_entity_assignment",
#           "relationshipGuid": "8cab5046-a81a-4d26-a393-957cb86fe0ef",
#           "relationshipStatus": "ACTIVE",
#           "relationshipAttributes": {
#             "typeName": "m4i_data_entity_assignment"
#           }
#         }
#       ],
#       "source": [
#         {
#           "guid": "0d6473a0-5e57-4dfd-b741-da6013a68200",
#           "typeName": "m4i_source",
#           "entityStatus": "ACTIVE",
#           "displayText": "/hr/hr/Data Dictionary_template.xlsm",
#           "relationshipType": "m4i_referenceable_source_assignment",
#           "relationshipGuid": "7b141b71-9821-41ff-8ac5-32f760a617f4",
#           "relationshipStatus": "ACTIVE",
#           "relationshipAttributes": {
#             "typeName": "m4i_referenceable_source_assignment"
#           }
#         }
#       ],
#       "meanings": []
#     },
#     "status": "ACTIVE",
#     "updateTime": 1657099979499,
#     "updatedBy": "admin",
#     "version": 0
#   }
# }'''

# assert updated_docs['ad49630e-7885-4560-aad3-8fbe743eb0ec']["definition"] == 'This data domain contains data from the finance department (8th update)'


