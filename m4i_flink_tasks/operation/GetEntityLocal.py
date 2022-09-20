# -*- coding: utf-8 -*-
import asyncio
import json
import logging
from config import config
from credentials import credentials
from m4i_atlas_core import AtlasChangeMessage, ConfigStore, EntityAuditAction, get_entity_by_guid, get_keycloak_token
from m4i_flink_tasks.DeadLetterBoxMessage import DeadLetterBoxMesage
#store = ConfigStore.get_instance()


class WrongOperationTypeException(Exception):
    pass
# end of class WrongOperationTypeException

class GetEntityLocal(object):
    access_token = None

    def get_access_token(self):
        if self.access_token==None:
            try:
                self.access_token = get_keycloak_token()
            except:
                pass
        return self.access_token

    def map_local(self, kafka_notification: str):

        async def get_entity(kafka_notification, access_token):

            logging.info(repr(kafka_notification))
            kafka_notification_obj = AtlasChangeMessage.from_json(kafka_notification)
            logging.info(access_token)

            if kafka_notification_obj.message.operation_type in [EntityAuditAction.ENTITY_CREATE, EntityAuditAction.ENTITY_UPDATE]:
                msg_creation_time = kafka_notification_obj.msg_creation_time
                entity_guid = kafka_notification_obj.message.entity.guid
                await get_entity_by_guid.cache.clear()
                event_entity = await get_entity_by_guid(guid=entity_guid, ignore_relationships=False, access_token=access_token)
                # event_entity = await get_entity_by_guid(guid=entity_guid, ignore_relationships=False)
                if not event_entity:
                    raise Exception(f"No entity could be retreived from Atlas with guid {entity_guid}")

                logging.warning(repr(kafka_notification_obj))
                logging.warning(repr(event_entity))
                kafka_notification_json = json.loads(kafka_notification_obj.to_json())
                entity_json = json.loads(event_entity.to_json())

                logging.warning(json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : entity_json}))
                return json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : entity_json, "msg_creation_time": msg_creation_time})

            elif kafka_notification_obj.message.operation_type == EntityAuditAction.ENTITY_DELETE:
                kafka_notification_json = json.loads(kafka_notification_obj.to_json())
                logging.warning(json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : {}}))
                return json.dumps({"kafka_notification" : kafka_notification_json, "atlas_entity" : {}, "msg_creation_time": msg_creation_time})

            else:
                logging.warning("message with an unexpected message operation type")
                raise WrongOperationTypeException(f"message with an unexpected message operation type  received from Atlas with guid {kafka_notification_obj.message.entity.guid} and operation type {kafka_notification.message.operation_type}")
        # END func

        retry = 0
        while retry < 3:
            try:
                access__token = self.get_accress_token()
                return asyncio.run(get_entity(kafka_notification, access__token))
            except WrongOperationTypeException as e:
                raise e
            except Exception as e:
                logging.error("failed to retrieve entity from atlas - retry")
                logging.error(str(e))
                self.access_token = None
            retry = retry+1
        raise Exception(f"Failed to lookup entity for kafka notification {kafka_notification}")
# end of class GetEntityLocal
