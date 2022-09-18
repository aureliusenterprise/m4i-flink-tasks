
from elasticsearch import Elasticsearch
from elastic_enterprise_search import AppSearch

from m4i_atlas_core import ConfigStore

config_store = ConfigStore.get_instance()


def make_elastic_connection() -> Elasticsearch:
    """
    Returns a connection with the ElasticSearch database
    """

    elastic_search_endpoint, username, password = config_store.get_many(
        "elastic.search.endpoint",
        "elastic.cloud.username",
        "elastic.cloud.password"
    )

    connection = Elasticsearch(elastic_search_endpoint, basic_auth=(username, password))

    return connection

def make_elastic_app_search_connect() -> AppSearch:
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

    return app_search

def get_document(entity_guid : str, app_search : AppSearch) -> dict:
    """This function returns a document corresponding to the entity guid from elastic app search."""
    
    engine_name = config_store.get("elastic.app.search.engine.name")

    doc_list = app_search.get_documents(
        engine_name=engine_name, document_ids=[entity_guid])
    if len(doc_list) > 0:
        return doc_list[0]


def list_all_documents(app_search : AppSearch, engine_name : str = None, current_page: int = 1, page_size: int = 1000) -> list:
    """This function lists all documents and returns the result"""
    all_doc_list = []

    doc_list = app_search.list_documents(
        engine_name=engine_name, current_page = current_page, page_size = page_size)["results"]

    while len(doc_list) != 0:
        all_doc_list = all_doc_list + doc_list

        current_page = current_page + 1
        doc_list = app_search.list_documents(
            engine_name=engine_name, current_page=current_page)["results"]

    return all_doc_list

def send_query(app_search : AppSearch, body: dict, engine_name: str = None, current_page: int = 1, page_size: int = 1000) -> list:
    """This function sends a query to the app search and returns a list of retrieved document ids."""
    if not engine_name:
        engine_name = config_store.get("elastic.app.search.engine.name")
    
    documents = app_search.search(engine_name = engine_name, body = body, current_page = current_page, page_size = page_size).body.get("results")
    
    result =  [document["id"].get("raw") for document in documents]

    if len(result) >= page_size:
        result += send_query(
            app_search = app_search,
            engine_name = engine_name,
            body = body,
            current_page = current_page + 1,
            page_size = page_size
        )
    return result

def get_documents(app_search : AppSearch, engine_name : str, entity_guid_list: list) -> list:
    """This function returns a list of documents having the input guids as ids."""
    if entity_guid_list == []:
        return []
        
    documents_list = app_search.get_documents(
        engine_name=engine_name, document_ids=entity_guid_list)
    return list(documents_list)

def delete_document(entity_guid : str, app_search: AppSearch) -> None:
    "This function deleted all documents corresponding to the given entity guid. No output is returned."
    engine_name = config_store.get("elastic.app.search.engine.name")
    app_search.delete_documents(
        engine_name=engine_name, document_ids=[entity_guid])

def get_source_type_from_app_search(input_document: dict) -> list:
    """This function returns the m4i source types (e.g. m4i_data_attribute, m4i_field, m4i_dataset, etc.) of the app search document."""
    return input_document.get("m4isourcetype")

def get_child_entity_docs(entity_guid : str, app_search : AppSearch, engine_name : str = None):

    engine_name = config_store.get("elastic.app.search.engine.name")

    body = {
        "query":"",
        "filters":{
        "breadcrumbguid":[ entity_guid ]
        }
    }

    breadcrumb_guid_list = send_query(app_search=app_search, body = body, engine_name = engine_name)
    return get_documents(app_search, engine_name, breadcrumb_guid_list)

def get_child_entity_guids(entity_guid : str, app_search : AppSearch, engine_name : str = None):
    if engine_name==None:
        engine_name = config_store.get("elastic.app.search.engine.name")

    body = {
        "query":"",
        "filters":{
        "parentguid":[ entity_guid ]
        }
    }

    guid_list = send_query(app_search=app_search, body = body, engine_name = engine_name)
    return guid_list
