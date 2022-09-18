# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 11:01:53 2022

@author: andre
"""

from config import config
from credentials import credentials
from elasticsearch import Elasticsearch
from elastic_enterprise_search import AppSearch
from .elastic import get_document, send_query, get_documents
from .AppSearchDocument import AppSearchDocument
from m4i_atlas_core import ConfigStore
import uuid
from datetime import datetime

config_store = ConfigStore.get_instance()
config_store.load({**config, **credentials})


# elastic search connection
elastic_search_endpoint, username, password = config_store.get_many(
    "elastic.search.endpoint",
    "elastic.user",
    "elastic.passwd"
)

connection = Elasticsearch(elastic_search_endpoint, basic_auth=(username, password))


# enterprise search connection
(
    elastic_base_endpoint, 
    elastic_user, 
    elastic_passwd
) = config_store.get_many(
    "elastic.enterprise.search.endpoint", 
    "elastic.user", 
    "elastic.passwd")

app_search = AppSearch(
        hosts=elastic_base_endpoint,
        basic_auth=(elastic_user, elastic_passwd)
    )

ll = app_search.list_documents(engine_name="atlas-dev")

#%%
# find all child nodes
entity_guid = "d56db187-2627-41a6-8698-f74d4b76227e"
engine_name = "atlas-dev"

doc = list(app_search.get_documents(
    engine_name=engine_name,
    document_ids=[entity_guid]))
entity = AppSearchDocument(**(doc[0]))

body = {
    "query":"",
    "filters":{"all":[
            { "parentguid": [ entity_guid ]}
            ]}
}

breadcrumb_guid_list = list(send_query(app_search=app_search, body = body, engine_name = engine_name))

doc2 = list(app_search.get_documents(
    engine_name=engine_name,
    document_ids=breadcrumb_guid_list))
df = pd.DataFrame(doc2)
df['breadcrumbguid_length'] = df['breadcrumbguid'].apply(lambda x: len(x))
relevant_guids = df[df['breadcrumbguid_length']==1]

getattr(entity, "breadcrumbguid")
#%%
#

oc = OperationChange(propagate=True, propagate_down=True, operations = [{"hello": "workld"}])
ocj = json.loads(oc.to_json())

oe = OperationEvent(id=str(uuid.uuid4()), 
                    creation_time=int(datetime.now().timestamp()*1000),
                    entity_guid="d56db187-2627-41a6-8698-f74d4b76227e",
                    changes=[oc])
oej = json.loads(oe.to_json())

oe2 = OperationEvent.from_json(oe.to_json())

for change in oe.changes:
    print(change.to_json())
#%%
oc = OperationChange(propagate=True, propagate_down=True, operations = [{"hello": "workld"}])
ocj = json.loads(oc.to_json())

oe = OperationEvent(id=str(uuid.uuid4()), 
                    creation_time=int(datetime.now().timestamp()*1000),
                    entity_guid="d56db187-2627-41a6-8698-f74d4b76227e",
                    changes=[])
for change in oe.changes:
    print(change.to_json())
import copy
oe2 = copy.deepcopy(oe)
oe2.id="15"
oe.id