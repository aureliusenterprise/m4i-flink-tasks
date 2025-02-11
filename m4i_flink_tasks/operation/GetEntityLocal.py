# -*- coding: utf-8 -*-
import asyncio
import json
import logging
from typing import Callable, Dict, List, Optional, Union
import time

#from config import config
#from credentials import credentials
from m4i_atlas_core import (AtlasChangeMessage, EntityAuditAction,
                            get_entity_by_guid, get_keycloak_token)
from m4i_atlas_core import get_entity_audit, get_entity_audit_events
from m4i_atlas_core import (ConfigStore, Entity, EntityDef, get_type_def)
#from m4i_flink_tasks.DeadLetterBoxMessage import DeadLetterBoxMesage
#store = ConfigStore.get_instance()


class WrongOperationTypeException(Exception):
    pass
# end of class WrongOperationTypeException

class SourceEntityTypeException(Exception):
    pass
# end of class SourceEntityTypeException

class NotFoundEntityException(Exception):
    pass
# end of class NotFoundEntityException

class AtlasAuditRetrieveException(Exception):
    pass
# end of class AtlasAuditRetrieveException

class GetEntityLocal(object):
    access_token = None

    def get_access_token(self):
        if self.access_token==None:
            try:
                self.access_token = get_keycloak_token()
            except:
                pass
        return self.access_token

    def get_audit(self, entity_guid: str):
        retry = 0
        while retry < 100:
            try:
                access__token = self.get_access_token()
                #logging.info(f"access tokenL: {access__token}")
                asyncio.run(get_entity_audit.cache.clear())
                entity_audit =  asyncio.run(get_entity_audit_events(entity_guid = entity_guid, access_token = access__token))
                logging.info(entity_audit)
                if entity_audit:
                    # atlas_entiy = Entity.from_json(re.search(r"{.*}", entity_audit.details).group(0))
                    # logging.info(atlas_entiy.to_json())
                    # logging.info(atlas_entiy.relationship_attributes)
                    # logging.info(f"derived atlas_entity relationship attributes : {atlas_entiy.relationship_attributes!=None}")
                    # return atlas_entiy.relationship_attributes != None
                    return entity_audit
                else:
                    logging.info("was not able to determine audit trail")
                    return {}
            except Exception as e:
                logging.error("failed to retrieve entity audit from atlas - retry - retry "+str(retry))
                logging.error(str(e))
                self.access_token = None
                time.sleep(retry)
            retry = retry+1
        raise AtlasAuditRetrieveException(f"Failed to lookup entity audit for entity guid {entity_guid}")

    def get_super_types(self, input_type: str) -> List[EntityDef]:
        """This function returns all supertypes of the input type given"""
        access_token = get_keycloak_token()
        entity_def =  asyncio.run(get_type_def(input_type, access_token=access_token))
        # logging.info(f"entity_def {entity_def}")
        if len(entity_def.super_types) == 0:
            return [entity_def]

        responses = [
            self.get_super_types(super_type)
            for super_type in entity_def.super_types
        ]
        # responses =  asyncio.gather(*requests)

        super_types = [
            super_type
            for response in responses
            for super_type in response
        ]

        return [entity_def, *super_types]
    # END get_super_types

    def get_super_types_names(self, input_type: str) -> List[str]:
        """This function returns all supertype names of the input type given with the given type included."""
        super_types =  self.get_super_types(input_type)
        logging.info(f"supertypenames: {super_types}")
        return  [super_type.name for super_type in super_types]

    def map_local(self, kafka_notification: str):

        logging.info(repr(kafka_notification))
        kafka_notification_obj = AtlasChangeMessage.from_json(kafka_notification)
        #logging.info(access_token_)

        msg_creation_time = kafka_notification_obj.msg_creation_time

        if kafka_notification_obj.message.operation_type in [EntityAuditAction.ENTITY_DELETE, EntityAuditAction.ENTITY_CREATE, EntityAuditAction.ENTITY_UPDATE] and kafka_notification_obj.message.entity.type_name == 'm4i_source':
                logging.info("This is an entity of type m4i_source ")
                raise SourceEntityTypeException(f"This is an entity of type m4i_source")

        if kafka_notification_obj.message.operation_type in [EntityAuditAction.ENTITY_CREATE, EntityAuditAction.ENTITY_UPDATE]:
            entity_guid = kafka_notification_obj.message.entity.guid
            entity_type = kafka_notification_obj.message.entity.type_name
            entity_ts = kafka_notification_obj.message.event_time
            retry = 0
            event_entity = None
            while retry < 100 and event_entity==None:
                try:
                    access_token_ = self.get_access_token()
                    asyncio.run(get_entity_by_guid.cache.clear())
                    event_entity = asyncio.run(get_entity_by_guid(guid=entity_guid, ignore_relationships=False, access_token=access_token_))
                except Exception as e:
                    logging.error("failed to retrieve entity from atlas - retry "+str(retry))
                    logging.error(str(e))
                    self.access_token = None
                    time.sleep(retry)
                retry = retry+1

            # event_entity =  get_entity_by_guid(guid=entity_guid, ignore_relationships=False)
            if not event_entity:
                logging.warning(f"No entity could be retreived from Atlas with guid {entity_guid}")
                raise NotFoundEntityException(f"No entity could be retreived from Atlas with guid {entity_guid}")

            logging.warning(repr(kafka_notification_obj))
            logging.warning(repr(event_entity))
            kafka_notification_json = json.loads(kafka_notification_obj.to_json())
            entity_json = json.loads(event_entity.to_json())
            audit_json_arr = self.get_audit(entity_guid)
            # find the right audit entry
            logging.warning("entity_ts: "+str(entity_ts))
            audit_json = {}
            for ev in audit_json_arr:
                ev_details = ev.details[ev.details.find(':')+1:]
                #logging.warning(ev_details)
                ev_details = json.loads(ev_details)
                ts = None
                if "updateTime" in ev_details.keys():
                    ts = ev_details['updateTime']
                elif "createTime" in ev_details.keys():
                    ts = ev_details['createTime']
                #logging.warning(ts)
                if ts == entity_ts:
                    audit_json = ev_details
            logging.warning("step 2: "+str(audit_json))
            supertypes = self.get_super_types_names(entity_type)
        elif kafka_notification_obj.message.operation_type == EntityAuditAction.ENTITY_DELETE:
            entity_type = kafka_notification_obj.message.entity.type_name
            kafka_notification_json = json.loads(kafka_notification_obj.to_json())
            entity_ts = kafka_notification_obj.message.event_time
            #logging.warning(json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : {}}))
            entity_json = {}
            audit_json = {}
            supertypes = self.get_super_types_names(entity_type)
            #return json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : {}, "msg_creation_time": msg_creation_time})
        else:
            logging.warning("message with an unexpected message operation type")
            raise WrongOperationTypeException(f"message with an unexpected message operation type received from Atlas")
        #logging.warning(json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : entity_json}))
        return json.dumps({"kafka_notification" : kafka_notification_json,
                            "atlas_entity" : entity_json,
                            "msg_creation_time": msg_creation_time,
                            "event_time": entity_ts,
                            "atlas_entity_audit": audit_json,
                            "supertypes": supertypes})
    # END func

            # retry = 0
            # while retry < 100:
            #     try:
            #         access__token = self.get_access_token()
            #         #logging.info(f"access tokenL: {access__token}")
            #         return (get_entity(kafka_notification, access__token))
# end of class GetEntityLocal
