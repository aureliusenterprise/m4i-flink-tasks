# -*- coding: utf-8 -*-
import datetime
import json
import logging
import uuid

import jsonpickle

from m4i_flink_tasks.operation.core_operation import WorkflowEngine
from m4i_flink_tasks.operation.OperationEvent import (OperationChange,
                                                      OperationEvent)
from m4i_flink_tasks.synchronize_app_search.elastic import (
    get_child_entity_guids, make_elastic_app_search_connect)

class MissingEntityGuidException(Exception):
    pass
# end of class MissingEntityGuidException

class AppSearchDocumentNotFoundException(Exception):
    pass
# end of class AppSearchDocumentNotFoundException

class AppSearchUpdateStorageException(Exception):
    pass
# end of class AppSearchUpdateStorageException


class LocalOperationLocal(object):

    app_search = None

    def open_local(self, config, credentials, config_store):
        self.config_store = config_store
        self.config_store.load({**config, **credentials})
        self.app_search_engine_name = config_store.get("operations.appsearch.engine.name")
        self.engine_name = config_store.get("operations.appsearch.engine.name")
        self.app_search = make_elastic_app_search_connect()


    def map_local(self, kafka_notification: str):
        logging.info("start local operation local")
        events = []
        logging.warning("kafka notification: "+repr(kafka_notification))

        oe = OperationEvent.from_json(kafka_notification)
        entity_guid = oe.entity_guid
        if entity_guid==None or len(entity_guid)==0:
            raise MissingEntityGuidException(f"Missing entity guid in local operation event with id {oe.id} at {oe.creation_time}")
        # retrieve the app search document
        retry = 0
        entity = None
        success_retrieve = False
        retrieved_empty_document = True
        while (retry<3) and not success_retrieve:
            try:
                doc = list(self.app_search.get_documents(
                                            engine_name=self.engine_name,
                                            document_ids=[entity_guid]))
                # doc = list(app_search.get_documents(engine_name=engine_name,document_ids=[entity_guid]))

                # keep it on the dictionary and not the object, beasue then it is applicabel to variouse app search documents
                # and the sync elastic job is independent of a specific data model
                logging.info("received documents")
                logging.info(doc)
                # handling of missing guids
                if len(doc)>0:
                    entity = (doc[0])
                    if entity!=None:
                        retrieved_empty_document = False
                success_retrieve = True
            except Exception as e:
                logging.error("connection to app search could not be established "+str(e))
                self.app_search = make_elastic_app_search_connect()
            retry = retry+1
        logging.info(f"retrieved entity {entity}")
        if not success_retrieve:
            raise AppSearchDocumentNotFoundException(f"Could not find document with guid {entity_guid} for event id {oe.id}")

        # execute the different changes
        new_changes={}
        errors = []
        for change in oe.changes:
            # change = oe.changes[0]
            # first propagation has to be determined before local changes are applied.
            # Otherwise propagation can not be determined properly anymore
            # propagation required?
            # apply local changes
            operation = change.operation
            engine = WorkflowEngine(json.dumps(operation))
            entity = engine.run(entity, self.app_search)
            logging.info(f"modified entity {entity}")

        for change in oe.changes:
            # this transformation takes care of the transformation and the propagation..
            new_changes_returned = change.transform(entity,self.app_search,entity_guid,self.app_search_engine_name)
            for id_ in new_changes_returned.keys():
                if id_ not in new_changes.keys():
                    new_changes[id_] = new_changes_returned[id_]
                else:
                    new_changes[id_] = new_changes[id_]  + new_changes_returned  [id_]


        # write back the entity into appsearch
        retry_ = 0
        success_update = False
        while not success_update and retry_<3:
            try:
                logging.info("writing back data")
                res = None
                if entity==None:
                    res = self.app_search.delete_documents(engine_name=self.engine_name, document_ids=[entity_guid])
                    # res = app_search.delete_documents(engine_name=engine_name, document_ids=[entity_guid])
                    logging.info(f"removed entity {entity_guid} from app search with result {repr(res)}")
                elif retrieved_empty_document:
                    res = self.app_search.index_documents(engine_name=self.engine_name, documents=entity)
                    # res = app_search.index_documents(engine_name=engine_name, documents=entity)
                    logging.info(f"inserted entity {entity_guid} to app search with result {repr(res)}")
                else:
                    res = self.app_search.put_documents(engine_name=self.engine_name, documents=entity)
                    # res = app_search.put_documents(engine_name=engine_name, documents=entity)
                    logging.info(f"updated entity {entity_guid} from app search with result {repr(res)}")
                if res==None or len(res)==0:
                    logging.warning(f"no updates performed for event {kafka_notification}")
                elif len(res)>0:
                    for res_ in res:
                        # res_ = res[0]
                        error = res_['errors']
                        if len(error)>0:
                            errors.extend(error)
                if len(errors)==0:
                    success_update = True
            except Exception as e:
                logging.error("connection to app search could not be established "+str(e))
                self.app_search = make_elastic_app_search_connect()
                retry_ = retry_+1
        if success_update:
            # calculate the resulting events to be propagated
            logging.info("calculate the resulting events to be propagated")
            for id_ in new_changes.keys():
                op = OperationEvent(id=str(uuid.uuid4()),
                               creation_time=int(datetime.datetime.now().timestamp()*1000),
                               entity_guid=id_,
                               changes=new_changes[id_])
                logging.warn(f"propagated event for id {id_}: {op}")
                events.append(json.dumps(json.loads(op.to_json())))
        else:
            raise AppSearchUpdateStorageException(f"Upodating the documents from app search failed for entity_guid {entity_guid} with the following errors: {repr(errors)}")


        logging.info("return events:")
        logging.info(events)
        return events
# end of class LocalOperationLocal
