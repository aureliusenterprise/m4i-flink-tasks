# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 13:47:33 2022

@author: andre
"""
from m4i_atlas_core import AtlasChangeMessage, Entity
import uuid
from datetime import datetime
from m4i_flink_tasks import EntityMessage
import json

def test__create_entityMessage():
    atlas_entity_change_message = EntityMessage(
                        type_name = "m4i_data_domain",
                        qualified_name = "hallo-test",
                        guid = str(uuid.uuid4()),
                        msg_creation_time = int(datetime.now().timestamp())*1000,
                        old_value = {},
                        new_value = {},
                        original_event_type = "type",
                        direct_change = False,
                        event_type = "test",
    
                        inserted_attributes = [],
                        changed_attributes = [],
                        deleted_attributes = [],
    
                        inserted_relationships = {},
                        changed_relationships = {},
                        deleted_relationships = {}
    
                        )
    assert(atlas_entity_change_message!=None)

def test__parsing_json_create_entityMessage():
    s =  "{\"kafka_notification\": {\"version\": {\"version\": \"1.0.0\", \"versionParts\": [1]}, \"msgCompressionKind\": \"NONE\", \"msgSplitIdx\": 1, \"msgSplitCount\": 1, \"msgSourceIP\": \"10.244.2.188\", \"msgCreatedBy\": \"\", \"msgCreationTime\": 1663244060284, \"message\": {\"eventTime\": 1663244059792, \"operationType\": \"ENTITY_CREATE\", \"type\": \"ENTITY_NOTIFICATION_V2\", \"entity\": {\"typeName\": \"hdfs_path\", \"attributes\": {\"path\": \"asdf\", \"createTime\": 1663192800000, \"qualifiedName\": \"sdf\", \"name\": \"te\"}, \"classifications\": [], \"createTime\": null, \"createdBy\": null, \"customAttributes\": null, \"guid\": \"c9697497-d348-41d7-9763-14ee31f8e57b\", \"homeId\": null, \"isIncomplete\": false, \"labels\": [], \"meanings\": [], \"provenanceType\": null, \"proxy\": null, \"relationshipAttributes\": null, \"status\": null, \"updateTime\": null, \"updatedBy\": null, \"version\": null}, \"relationship\": null}}, \"atlas_entity\": {\"typeName\": \"hdfs_path\", \"attributes\": {\"owner\": null, \"modifiedTime\": 1663192800000, \"replicatedTo\": [], \"userDescription\": null, \"isFile\": false, \"numberOfReplicas\": 0, \"replicatedFrom\": [], \"qualifiedName\": \"sdf\", \"displayName\": null, \"description\": null, \"extendedAttributes\": null, \"nameServiceId\": null, \"path\": \"asdf\", \"posixPermissions\": null, \"createTime\": 1663192800000, \"fileSize\": 0, \"clusterName\": null, \"name\": \"te\", \"isSymlink\": false, \"group\": null}, \"classifications\": [], \"createTime\": 1663244059792, \"createdBy\": \"atlas\", \"customAttributes\": null, \"guid\": \"c9697497-d348-41d7-9763-14ee31f8e57b\", \"homeId\": null, \"isIncomplete\": false, \"labels\": [], \"meanings\": [], \"provenanceType\": null, \"proxy\": null, \"relationshipAttributes\": {\"inputToProcesses\": [], \"pipeline\": null, \"schema\": [], \"hiveDb\": null, \"model\": null, \"meanings\": [], \"outputFromProcesses\": []}, \"status\": \"ACTIVE\", \"updateTime\": 1663244059792, \"updatedBy\": \"atlas\", \"version\": 0}, \"msg_creation_time\": 1663244060284}"
    
    kafka_notification_json = json.loads(s)
    msg_creation_time = kafka_notification_json.get("msg_creation_time")
    
    atlas_kafka_notification_json = kafka_notification_json["kafka_notification"]
    atlas_kafka_notification = AtlasChangeMessage.from_json(json.dumps(atlas_kafka_notification_json))
    
    atlas_entity_json = kafka_notification_json["atlas_entity"]
    atlas_entity_parsed = Entity.from_json(json.dumps(atlas_entity_json))
    
    
    atlas_entity_change_message = EntityMessage(
                        type_name = atlas_entity_parsed.type_name,
                        qualified_name = atlas_entity_parsed.attributes.unmapped_attributes["qualifiedName"],
                        guid = atlas_entity_parsed.guid,
                        msg_creation_time = msg_creation_time,
                        old_value = {},
                        new_value = atlas_entity_parsed,
                        original_event_type = atlas_kafka_notification.message.operation_type,
                        direct_change = False,
                        event_type = "EntityCreated",
    
                        inserted_attributes = list((atlas_entity_json["attributes"]).keys()),
                        changed_attributes = [],
                        deleted_attributes = [],
    
                        inserted_relationships = (atlas_entity_json["relationshipAttributes"]),
                        changed_relationships = {},
                        deleted_relationships = {}
    
                    )
    assert(atlas_entity_change_message!=None)

