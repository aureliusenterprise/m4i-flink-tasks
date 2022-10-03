# -*- coding: utf-8 -*-
"""
Created on Sun Oct  2 12:43:19 2022

@author: andre
"""

entity_delete="""
{
	"typeName": "m4i_data_entity",
	"qualifiedName": "3b42d9cc-e37a-4a83-8d92-df389f250766",
	"guid": "93a2025e-eddf-46c1-8cea-3f27f8a4c3eb",
	"msgCreationTime": 1664706959905,
	"originalEventType": "ENTITY_DELETE",
	"directChange": true,
	"eventType": "EntityDeleted",
	"insertedAttributes": [],
	"changedAttributes": [],
	"deletedAttributes": [
		"qualifiedName",
		"name"
	],
	"insertedRelationships": {},
	"changedRelationships": {},
	"deletedRelationships": {
		"ArchiMateReference": [],
		"steward": [],
		"dataDomain": [
			{
				"guid": "8e12e377-1b4e-45e6-95e0-79ef733d9145",
				"typeName": "m4i_data_domain",
				"entityStatus": "ACTIVE",
				"displayText": "test domain",
				"relationshipType": "m4i_data_entity_assignment",
				"relationshipGuid": "c29b7598-a916-421a-846e-e03a26747df5",
				"relationshipStatus": "ACTIVE",
				"relationshipAttributes": {
					"typeName": "m4i_data_entity_assignment"
				}
			}
		],
		"parentEntity": [],
		"childEntity": [],
		"attributes": [
			{
				"guid": "92e5d535-7d85-4839-8bf6-89b155a26f1c",
				"typeName": "m4i_data_attribute",
				"entityStatus": "ACTIVE",
				"displayText": "test att",
				"relationshipType": "m4i_data_entity_attribute_assignment",
				"relationshipGuid": "aace76ba-cf76-47dd-8152-1fc28f5b396a",
				"relationshipStatus": "ACTIVE",
				"relationshipAttributes": {
					"typeName": "m4i_data_entity_attribute_assignment"
				}
			}
		],
		"source": [],
		"businessOwner": [],
		"meanings": []
	},
	"oldValue": {
		"typeName": "m4i_data_entity",
		"attributes": {
			"archimateReference": [],
			"replicatedTo": null,
			"replicatedFrom": null,
			"steward": [],
			"qualifiedName": "3b42d9cc-e37a-4a83-8d92-df389f250766",
			"parentEntity": [],
			"source": [],
			"dataDomain": [
				{
					"guid": "8e12e377-1b4e-45e6-95e0-79ef733d9145",
					"typeName": "m4i_data_domain",
					"uniqueAttributes": {
						"qualifiedName": "d889ed0f-b106-40c0-9576-08b668f03c79"
					}
				}
			],
			"childEntity": [],
			"name": "test entity",
			"definition": null,
			"attributes": [
				{
					"guid": "92e5d535-7d85-4839-8bf6-89b155a26f1c",
					"typeName": "m4i_data_attribute",
					"uniqueAttributes": {
						"qualifiedName": "f3d9e6f9-2fdb-45ce-bf3e-f83fd2140b54"
					}
				}
			],
			"businessOwner": []
		},
		"classifications": [],
		"createTime": 1664706682868,
		"createdBy": "atlas",
		"customAttributes": null,
		"guid": "93a2025e-eddf-46c1-8cea-3f27f8a4c3eb",
		"homeId": null,
		"isIncomplete": false,
		"labels": [],
		"meanings": [],
		"provenanceType": null,
		"proxy": null,
		"relationshipAttributes": {
			"ArchiMateReference": [],
			"steward": [],
			"dataDomain": [
				{
					"guid": "8e12e377-1b4e-45e6-95e0-79ef733d9145",
					"typeName": "m4i_data_domain",
					"entityStatus": "ACTIVE",
					"displayText": "test domain",
					"relationshipType": "m4i_data_entity_assignment",
					"relationshipGuid": "c29b7598-a916-421a-846e-e03a26747df5",
					"relationshipStatus": "ACTIVE",
					"relationshipAttributes": {
						"typeName": "m4i_data_entity_assignment"
					}
				}
			],
			"parentEntity": [],
			"childEntity": [],
			"attributes": [
				{
					"guid": "92e5d535-7d85-4839-8bf6-89b155a26f1c",
					"typeName": "m4i_data_attribute",
					"entityStatus": "ACTIVE",
					"displayText": "test att",
					"relationshipType": "m4i_data_entity_attribute_assignment",
					"relationshipGuid": "aace76ba-cf76-47dd-8152-1fc28f5b396a",
					"relationshipStatus": "ACTIVE",
					"relationshipAttributes": {
						"typeName": "m4i_data_entity_attribute_assignment"
					}
				}
			],
			"source": [],
			"businessOwner": [],
			"meanings": []
		},
		"status": "ACTIVE",
		"updateTime": 1664706781466,
		"updatedBy": "atlas",
		"version": 0
	},
	"newValue": {}
}
"""