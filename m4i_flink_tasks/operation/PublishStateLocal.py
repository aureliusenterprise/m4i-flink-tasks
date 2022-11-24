import json
import logging

from m4i_atlas_core import Entity

#from config import config
#from credentials import credentials
#from m4i_atlas_core import AtlasChangeMessage, EntityAuditAction, get_entity_by_guid, get_keycloak_token
from m4i_flink_tasks.synchronize_app_search import make_elastic_connection

class EventParsingException(Exception):
    pass
# end of class EventParsingException

class ElasticPersistingException(Exception):
    pass
# end of class ElasticPersistingException

class ElasticPreviouseStateRetrieveException(Exception):
    pass
# end of class ElasticPreviouseStateRetrieveException

class PublishStateLocal(object):
    elastic = None
    elastic_search_index = None
    config_store = None
    doc_id = 0

    def get_previous_atlas_entity(self, entity_guid, msg_creation_time):
        query = {
            "bool": {
                "filter": [
                {
                    "match": {
                    "body.guid.keyword": entity_guid
                    }
                },
                {
                    "range": {
                    "msgCreationTime": {
                        "lt": msg_creation_time
                    }
                    }
                }
                ]
            }
        }

        sort = {
            "msgCreationTime": {"numeric_type" : "long", "order": "desc"}
        }

        retry = 0
        while retry<3:
            try:
                # there is still potential to improve by using search_templayes instead of index search
                result = self.elastic.search(index = self.elastic_search_index, query = query, sort = sort, size = 1)

                if result["hits"]["total"]["value"] >= 1:
                    return result["hits"]["hits"][0]["_source"]["body"]
                if result["hits"]["total"]["value"] == 0:
                    return None
            except Exception as e:
                logging.warning("failed to retrieve document")
                logging.warning(str(e))
                try:
                    self.elastic = make_elastic_connection()
                except:
                    pass
            retry = retry + 1
        raise ElasticPreviouseStateRetrieveException(f"Failed to retrieve perviouse state for guid {entity_guid} at msg_creation_time {msg_creation_time}")

    def get_doc_id(self):
        self.doc_id = self.doc_id + 1
        return self.doc_id

    def open_local(self, config, credentials, config_store):
        self.config_store = config_store
       # self.config_store.load({**config, **credentials})
        self.elastic_search_index = self.config_store.get("elastic.search.index")
        self.elastic = make_elastic_connection()

    def map_local(self, kafka_notification: str):
        logging.info(kafka_notification)
        kafka_notification_json = json.loads(kafka_notification)

        if "kafka_notification" not in kafka_notification_json.keys() or "atlas_entity" not in kafka_notification_json.keys() \
            or "msg_creation_time" not in kafka_notification_json.keys() or "atlas_entity_audit" not in kafka_notification_json.keys() \
            or "supertypes" not in kafka_notification_json.keys() or "event_time" not in kafka_notification_json.keys() :
            raise EventParsingException("Kafka event does not match the predefined structure: {\"kafka_notification\" : {}, \"atlas_entity\" : {}, \"msg_creation_time\":8887123, \"event_time\":8887123, \"atlas_entity_audit\":{}, \"supertypes\":[] }")

        if not kafka_notification_json.get("kafka_notification"):
            logging.warning(kafka_notification)
            logging.warning("No kafka notification.")
            raise EventParsingException("Original Kafka notification which is produced by Atlas is missing")

        if not kafka_notification_json.get("atlas_entity"):
            logging.warning(kafka_notification)
            logging.warning("No atlas entity.")
            kafka_notification_json['previouse_version'] = None
            return json.dumps(kafka_notification_json)

        event_time = kafka_notification_json.get("event_time")
        msg_creation_time = kafka_notification_json.get("msg_creation_time")

        atlas_entity_json = kafka_notification_json["atlas_entity"]
        atlas_entity = json.dumps(atlas_entity_json)
        logging.warning(atlas_entity)

        #atlas_entity = Entity.from_json(atlas_entity)
        entity_guid = kafka_notification_json["kafka_notification"]["message"]["entity"]["guid"]

        # turns out update_time for an import of data into atlas is the same for all events. Does not work for us!
        # doc_id = "{}_{}".format(atlas_entity.guid, atlas_entity.update_time)

        doc_id_ = "{}_{}".format(entity_guid, msg_creation_time)
        doc = json.loads(json.dumps({"msgCreationTime": msg_creation_time, "eventTime": event_time, "body": atlas_entity_json }))

        retry = 0
        success = False
        while not success and retry<3:
            try:
                res = self.elastic.index(index=self.elastic_search_index, id = doc_id_, document=doc)
                logging.warning(str(res))
                if res['result'] in ['updated','created','deleted']:
                    success = True
                    logging.info("successfully submitted the document")
                else:
                    logging.error(f"errornouse result state {res['result']}")
            except Exception as e:
                logging.error("failed to submit the document")
                logging.warning(str(e))
                try:
                    self.elastic = make_elastic_connection()
                except:
                    pass
            retry = retry + 1
        # elastic.close()
        if not success:
            raise ElasticPersistingException(f"Storing state with doc_id {doc_id_} failed 3 times")

        kafka_notification_json['previouse_version'] = self.get_previous_atlas_entity(entity_guid, msg_creation_time)
        return json.dumps(kafka_notification_json)

# end of class PublishStateLocal




