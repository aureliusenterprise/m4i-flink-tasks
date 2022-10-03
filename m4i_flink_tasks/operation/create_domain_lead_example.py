# -*- coding: utf-8 -*-
"""
Created on Sun Oct  2 12:46:17 2022

@author: andre
"""

domain_lead="""
{
	"typeName": "m4i_data_domain",
	"qualifiedName": "d889ed0f-b106-40c0-9576-08b668f03c79",
	"guid": "8e12e377-1b4e-45e6-95e0-79ef733d9145",
	"msgCreationTime": 1664707534606,
	"originalEventType": "ENTITY_UPDATE",
	"directChange": true,
	"eventType": "EntityRelationshipAudit",
	"insertedAttributes": [],
	"changedAttributes": [],
	"deletedAttributes": [],
	"insertedRelationships": {
		"domainLead": [
			{
				"guid": "5078a818-f015-4c45-9d4b-92ab51ef5a6a",
				"typeName": "m4i_person",
				"entityStatus": "ACTIVE",
				"displayText": "test",
				"relationshipType": "m4i_domainLead_assignment",
				"relationshipGuid": "a949dd22-a010-400e-84e9-c3a9163fc5f5",
				"relationshipStatus": "ACTIVE",
				"relationshipAttributes": {
					"typeName": "m4i_domainLead_assignment"
				}
			}
		]
	},
	"changedRelationships": {},
	"deletedRelationships": {
		"dataEntity": [
			{
				"guid": "93a2025e-eddf-46c1-8cea-3f27f8a4c3eb",
				"typeName": "m4i_data_entity",
				"entityStatus": "ACTIVE",
				"displayText": "test entity",
				"relationshipType": "m4i_data_entity_assignment",
				"relationshipGuid": "c29b7598-a916-421a-846e-e03a26747df5",
				"relationshipStatus": "ACTIVE",
				"relationshipAttributes": {
					"typeName": "m4i_data_entity_assignment"
				}
			}
		]
	},
	"oldValue": {
		"typeName": "m4i_data_domain",
		"attributes": {
			"archimateReference": [],
			"replicatedTo": null,
			"replicatedFrom": null,
			"qualifiedName": "d889ed0f-b106-40c0-9576-08b668f03c79",
			"domainLead": [],
			"name": "test domain",
			"dataEntity": [
				{
					"guid": "93a2025e-eddf-46c1-8cea-3f27f8a4c3eb",
					"typeName": "m4i_data_entity",
					"uniqueAttributes": {
						"qualifiedName": "3b42d9cc-e37a-4a83-8d92-df389f250766"
					}
				}
			],
			"definition": null,
			"source": []
		},
		"classifications": [],
		"createTime": 1664706668887,
		"createdBy": "atlas",
		"customAttributes": null,
		"guid": "8e12e377-1b4e-45e6-95e0-79ef733d9145",
		"homeId": null,
		"isIncomplete": false,
		"labels": [],
		"meanings": [],
		"provenanceType": null,
		"proxy": null,
		"relationshipAttributes": {
			"ArchiMateReference": [],
			"domainLead": [],
			"dataEntity": [
				{
					"guid": "93a2025e-eddf-46c1-8cea-3f27f8a4c3eb",
					"typeName": "m4i_data_entity",
					"entityStatus": "ACTIVE",
					"displayText": "test entity",
					"relationshipType": "m4i_data_entity_assignment",
					"relationshipGuid": "c29b7598-a916-421a-846e-e03a26747df5",
					"relationshipStatus": "ACTIVE",
					"relationshipAttributes": {
						"typeName": "m4i_data_entity_assignment"
					}
				}
			],
			"source": [],
			"meanings": []
		},
		"status": "ACTIVE",
		"updateTime": 1664706746602,
		"updatedBy": "atlas",
		"version": 0
	},
	"newValue": {
		"typeName": "m4i_data_domain",
		"attributes": {
			"archimateReference": [],
			"replicatedTo": null,
			"replicatedFrom": null,
			"qualifiedName": "d889ed0f-b106-40c0-9576-08b668f03c79",
			"domainLead": [
				{
					"guid": "5078a818-f015-4c45-9d4b-92ab51ef5a6a",
					"typeName": "m4i_person",
					"uniqueAttributes": {
						"qualifiedName": "89e5b435-93bd-4c96-8fd4-4f52f5654465",
						"email": "a@h.i"
					}
				}
			],
			"name": "test domain",
			"dataEntity": [],
			"definition": null,
			"source": []
		},
		"classifications": [],
		"createTime": 1664706668887,
		"createdBy": "atlas",
		"customAttributes": null,
		"guid": "8e12e377-1b4e-45e6-95e0-79ef733d9145",
		"homeId": null,
		"isIncomplete": false,
		"labels": [],
		"meanings": [],
		"provenanceType": null,
		"proxy": null,
		"relationshipAttributes": {
			"ArchiMateReference": [],
			"domainLead": [
				{
					"guid": "5078a818-f015-4c45-9d4b-92ab51ef5a6a",
					"typeName": "m4i_person",
					"entityStatus": "ACTIVE",
					"displayText": "test",
					"relationshipType": "m4i_domainLead_assignment",
					"relationshipGuid": "a949dd22-a010-400e-84e9-c3a9163fc5f5",
					"relationshipStatus": "ACTIVE",
					"relationshipAttributes": {
						"typeName": "m4i_domainLead_assignment"
					}
				}
			],
			"dataEntity": [],
			"source": [],
			"meanings": []
		},
		"status": "ACTIVE",
		"updateTime": 1664707534418,
		"updatedBy": "atlas",
		"version": 0
	}
}
"""