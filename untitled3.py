# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 22:44:01 2022

@author: andre
"""

kafka_notification = """
{
	"typeName": "m4i_person",
	"qualifiedName": "7cc1532e-f138-4371-8c57-2cbd63c3e36a",
	"guid": "b596b30e-afa8-4d55-a3a3-eb673357cef6",
	"msgCreationTime": 1664915920494,
	"originalEventType": "ENTITY_UPDATE",
	"directChange": true,
	"eventType": "EntityRelationshipAudit",
	"insertedAttributes": [],
	"changedAttributes": [],
	"deletedAttributes": [],
	"insertedRelationships": {},
	"changedRelationships": {},
	"deletedRelationships": {
		"domainLead": [
			{
				"guid": "79f38aa8-344b-4671-9b20-3612b0b55a14",
				"typeName": "m4i_data_domain",
				"entityStatus": "ACTIVE",
				"displayText": "domain",
				"relationshipType": "m4i_domainLead_assignment",
				"relationshipGuid": "5eac1e93-e5d8-4a23-8206-d6540557f256",
				"relationshipStatus": "ACTIVE",
				"relationshipAttributes": {
					"typeName": "m4i_domainLead_assignment"
				}
			}
		]
	},
	"oldValue": {
		"typeName": "m4i_person",
		"attributes": {
			"archimateReference": [],
			"replicatedTo": null,
			"replicatedFrom": null,
			"qualifiedName": "7cc1532e-f138-4371-8c57-2cbd63c3e36a",
			"name": "asd",
			"source": [],
			"email": "a@h.i"
		},
		"classifications": [],
		"createTime": 1664915774978,
		"createdBy": "atlas",
		"customAttributes": null,
		"guid": "b596b30e-afa8-4d55-a3a3-eb673357cef6",
		"homeId": null,
		"isIncomplete": false,
		"labels": [],
		"meanings": [],
		"provenanceType": null,
		"proxy": null,
		"relationshipAttributes": {
			"indexPatterns": [],
			"stewardAttribute": [],
			"ArchiMateReference": [],
			"visualizations": [],
			"domainLead": [
				{
					"guid": "79f38aa8-344b-4671-9b20-3612b0b55a14",
					"typeName": "m4i_data_domain",
					"entityStatus": "ACTIVE",
					"displayText": "domain",
					"relationshipType": "m4i_domainLead_assignment",
					"relationshipGuid": "5eac1e93-e5d8-4a23-8206-d6540557f256",
					"relationshipStatus": "ACTIVE",
					"relationshipAttributes": {
						"typeName": "m4i_domainLead_assignment"
					}
				}
			],
			"businessOwnerAttribute": [],
			"source": [],
			"dashboards": [],
			"stewardEntity": [
				{
					"guid": "936f4e68-d8a8-4a6e-b007-97d7bcaf0640",
					"typeName": "m4i_data_entity",
					"entityStatus": "ACTIVE",
					"displayText": "entity",
					"relationshipType": "m4i_data_entity_steward_assignment",
					"relationshipGuid": "cac3bbea-7f61-4711-a0c8-baf44347ed84",
					"relationshipStatus": "ACTIVE",
					"relationshipAttributes": {
						"typeName": "m4i_data_entity_steward_assignment"
					}
				}
			],
			"meanings": [],
			"businessOwnerEntity": [],
			"ProcessOwner": []
		},
		"status": "ACTIVE",
		"updateTime": 1664915810844,
		"updatedBy": "atlas",
		"version": 0
	},
	"newValue": {
		"typeName": "m4i_person",
		"attributes": {
			"archimateReference": [],
			"replicatedTo": null,
			"replicatedFrom": null,
			"qualifiedName": "7cc1532e-f138-4371-8c57-2cbd63c3e36a",
			"name": "asd",
			"source": [],
			"email": "a@h.i"
		},
		"classifications": [],
		"createTime": 1664915774978,
		"createdBy": "atlas",
		"customAttributes": null,
		"guid": "b596b30e-afa8-4d55-a3a3-eb673357cef6",
		"homeId": null,
		"isIncomplete": false,
		"labels": [],
		"meanings": [],
		"provenanceType": null,
		"proxy": null,
		"relationshipAttributes": {
			"indexPatterns": [],
			"stewardAttribute": [],
			"ArchiMateReference": [],
			"visualizations": [],
			"domainLead": [],
			"businessOwnerAttribute": [],
			"source": [],
			"dashboards": [],
			"stewardEntity": [
				{
					"guid": "936f4e68-d8a8-4a6e-b007-97d7bcaf0640",
					"typeName": "m4i_data_entity",
					"entityStatus": "ACTIVE",
					"displayText": "entity",
					"relationshipType": "m4i_data_entity_steward_assignment",
					"relationshipGuid": "cac3bbea-7f61-4711-a0c8-baf44347ed84",
					"relationshipStatus": "ACTIVE",
					"relationshipAttributes": {
						"typeName": "m4i_data_entity_steward_assignment"
					}
				}
			],
			"meanings": [],
			"businessOwnerEntity": [],
			"ProcessOwner": []
		},
		"status": "ACTIVE",
		"updateTime": 1664915920363,
		"updatedBy": "atlas",
		"version": 0
	}
}
"""