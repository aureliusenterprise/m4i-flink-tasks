# -*- coding: utf-8 -*-

kafka_notification="""
{
	"typeName": "m4i_data_attribute",
	"qualifiedName": "f0ecc58c-0781-4f5c-ba68-77886f6c3366",
	"guid": "ecda0435-4c5e-474e-b247-0d3292f19477",
	"msgCreationTime": 1664877319597,
	"originalEventType": "ENTITY_UPDATE",
	"directChange": true,
	"eventType": "EntityRelationshipAudit",
	"insertedAttributes": [],
	"changedAttributes": [],
	"deletedAttributes": [],
	"insertedRelationships": {
		"steward": [
			{
				"guid": "047e1688-6ca2-4693-9769-3a4a742f881a",
				"typeName": "m4i_person",
				"entityStatus": "ACTIVE",
				"displayText": "steward",
				"relationshipType": "m4i_data_attribute_steward_assignment",
				"relationshipGuid": "758046fb-e3ef-4acb-96ee-be176678fea6",
				"relationshipStatus": "ACTIVE",
				"relationshipAttributes": {
					"typeName": "m4i_data_attribute_steward_assignment"
				}
			}
		]
	},
	"changedRelationships": {},
	"deletedRelationships": {},
	"oldValue": {
		"typeName": "m4i_data_attribute",
		"attributes": {
			"archimateReference": [],
			"replicatedTo": null,
			"replicatedFrom": null,
			"steward": [],
			"qualifiedName": "f0ecc58c-0781-4f5c-ba68-77886f6c3366",
			"dataEntity": [],
			"source": [],
			"isKeyData": null,
			"hasPII": null,
			"attributeType": null,
			"name": "test wperson",
			"definition": null,
			"riskClassification": null,
			"fields": [],
			"businessOwner": [
				{
					"guid": "7291bea9-b759-411f-b83f-87a6d69f9732",
					"typeName": "m4i_person",
					"uniqueAttributes": {
						"qualifiedName": "310364ef-c5ae-4c3a-978b-9af84fe971fb",
						"email": "o@s.g"
					}
				}
			]
		},
		"classifications": [],
		"createTime": 1664876851452,
		"createdBy": "atlas",
		"customAttributes": null,
		"guid": "ecda0435-4c5e-474e-b247-0d3292f19477",
		"homeId": null,
		"isIncomplete": false,
		"labels": [],
		"meanings": [],
		"provenanceType": null,
		"proxy": null,
		"relationshipAttributes": {
			"ArchiMateReference": [],
			"steward": [],
			"dataEntity": [],
			"source": [],
			"fields": [],
			"businessOwner": [
				{
					"guid": "7291bea9-b759-411f-b83f-87a6d69f9732",
					"typeName": "m4i_person",
					"entityStatus": "ACTIVE",
					"displayText": "owner",
					"relationshipType": "m4i_data_attribute_business_owner_assignment",
					"relationshipGuid": "5c39339f-2218-4396-8d70-9f3f84236ebc",
					"relationshipStatus": "ACTIVE",
					"relationshipAttributes": {
						"typeName": "m4i_data_attribute_business_owner_assignment"
					}
				}
			],
			"meanings": []
		},
		"status": "ACTIVE",
		"updateTime": 1664876934341,
		"updatedBy": "atlas",
		"version": 0
	},
	"newValue": {
		"typeName": "m4i_data_attribute",
		"attributes": {
			"archimateReference": [],
			"replicatedTo": null,
			"replicatedFrom": null,
			"steward": [
				{
					"guid": "047e1688-6ca2-4693-9769-3a4a742f881a",
					"typeName": "m4i_person",
					"uniqueAttributes": {
						"qualifiedName": "9c3f8749-2003-4062-b679-f6b018d62a4b",
						"email": "s@a.g"
					}
				}
			],
			"qualifiedName": "f0ecc58c-0781-4f5c-ba68-77886f6c3366",
			"dataEntity": [],
			"source": [],
			"isKeyData": null,
			"hasPII": null,
			"attributeType": null,
			"name": "test wperson",
			"definition": null,
			"riskClassification": null,
			"fields": [],
			"businessOwner": [
				{
					"guid": "7291bea9-b759-411f-b83f-87a6d69f9732",
					"typeName": "m4i_person",
					"uniqueAttributes": {
						"qualifiedName": "310364ef-c5ae-4c3a-978b-9af84fe971fb",
						"email": "o@s.g"
					}
				}
			]
		},
		"classifications": [],
		"createTime": 1664876851452,
		"createdBy": "atlas",
		"customAttributes": null,
		"guid": "ecda0435-4c5e-474e-b247-0d3292f19477",
		"homeId": null,
		"isIncomplete": false,
		"labels": [],
		"meanings": [],
		"provenanceType": null,
		"proxy": null,
		"relationshipAttributes": {
			"ArchiMateReference": [],
			"steward": [
				{
					"guid": "047e1688-6ca2-4693-9769-3a4a742f881a",
					"typeName": "m4i_person",
					"entityStatus": "ACTIVE",
					"displayText": "steward",
					"relationshipType": "m4i_data_attribute_steward_assignment",
					"relationshipGuid": "758046fb-e3ef-4acb-96ee-be176678fea6",
					"relationshipStatus": "ACTIVE",
					"relationshipAttributes": {
						"typeName": "m4i_data_attribute_steward_assignment"
					}
				}
			],
			"dataEntity": [],
			"source": [],
			"fields": [],
			"businessOwner": [
				{
					"guid": "7291bea9-b759-411f-b83f-87a6d69f9732",
					"typeName": "m4i_person",
					"entityStatus": "ACTIVE",
					"displayText": "owner",
					"relationshipType": "m4i_data_attribute_business_owner_assignment",
					"relationshipGuid": "5c39339f-2218-4396-8d70-9f3f84236ebc",
					"relationshipStatus": "ACTIVE",
					"relationshipAttributes": {
						"typeName": "m4i_data_attribute_business_owner_assignment"
					}
				}
			],
			"meanings": []
		},
		"status": "ACTIVE",
		"updateTime": 1664877319361,
		"updatedBy": "atlas",
		"version": 0
	}
}
"""

kafka_notification = """
{
	"typeName": "m4i_person",
	"qualifiedName": "9c3f8749-2003-4062-b679-f6b018d62a4b",
	"guid": "047e1688-6ca2-4693-9769-3a4a742f881a",
	"msgCreationTime": 1664883633722,
	"originalEventType": "ENTITY_UPDATE",
	"directChange": true,
	"eventType": "EntityRelationshipAudit",
	"insertedAttributes": [],
	"changedAttributes": [],
	"deletedAttributes": [],
	"insertedRelationships": {},
	"changedRelationships": {},
	"deletedRelationships": {
		"stewardEntity": [
			{
				"guid": "237d88dc-2c92-4a8b-b3fa-27743a8c4001",
				"typeName": "m4i_data_entity",
				"entityStatus": "ACTIVE",
				"displayText": "entity 2",
				"relationshipType": "m4i_data_entity_steward_assignment",
				"relationshipGuid": "771251fe-6969-40b4-ab66-740f0713db03",
				"relationshipStatus": "ACTIVE",
				"relationshipAttributes": {
					"typeName": "m4i_data_entity_steward_assignment"
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
			"qualifiedName": "9c3f8749-2003-4062-b679-f6b018d62a4b",
			"name": "steward",
			"source": [],
			"email": "s@a.go"
		},
		"classifications": [],
		"createTime": 1664876877768,
		"createdBy": "atlas",
		"customAttributes": null,
		"guid": "047e1688-6ca2-4693-9769-3a4a742f881a",
		"homeId": null,
		"isIncomplete": false,
		"labels": [],
		"meanings": [],
		"provenanceType": null,
		"proxy": null,
		"relationshipAttributes": {
			"indexPatterns": [],
			"stewardAttribute": [
				{
					"guid": "ecda0435-4c5e-474e-b247-0d3292f19477",
					"typeName": "m4i_data_attribute",
					"entityStatus": "ACTIVE",
					"displayText": "test wperson",
					"relationshipType": "m4i_data_attribute_steward_assignment",
					"relationshipGuid": "758046fb-e3ef-4acb-96ee-be176678fea6",
					"relationshipStatus": "ACTIVE",
					"relationshipAttributes": {
						"typeName": "m4i_data_attribute_steward_assignment"
					}
				}
			],
			"ArchiMateReference": [],
			"visualizations": [],
			"domainLead": [],
			"businessOwnerAttribute": [],
			"source": [],
			"dashboards": [],
			"stewardEntity": [
				{
					"guid": "237d88dc-2c92-4a8b-b3fa-27743a8c4001",
					"typeName": "m4i_data_entity",
					"entityStatus": "ACTIVE",
					"displayText": "entity 2",
					"relationshipType": "m4i_data_entity_steward_assignment",
					"relationshipGuid": "771251fe-6969-40b4-ab66-740f0713db03",
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
		"updateTime": 1664883396735,
		"updatedBy": "atlas",
		"version": 0
	},
	"newValue": {
		"typeName": "m4i_person",
		"attributes": {
			"archimateReference": [],
			"replicatedTo": null,
			"replicatedFrom": null,
			"qualifiedName": "9c3f8749-2003-4062-b679-f6b018d62a4b",
			"name": "steward",
			"source": [],
			"email": "s@a.go"
		},
		"classifications": [],
		"createTime": 1664876877768,
		"createdBy": "atlas",
		"customAttributes": null,
		"guid": "047e1688-6ca2-4693-9769-3a4a742f881a",
		"homeId": null,
		"isIncomplete": false,
		"labels": [],
		"meanings": [],
		"provenanceType": null,
		"proxy": null,
		"relationshipAttributes": {
			"indexPatterns": [],
			"stewardAttribute": [
				{
					"guid": "ecda0435-4c5e-474e-b247-0d3292f19477",
					"typeName": "m4i_data_attribute",
					"entityStatus": "ACTIVE",
					"displayText": "test wperson",
					"relationshipType": "m4i_data_attribute_steward_assignment",
					"relationshipGuid": "758046fb-e3ef-4acb-96ee-be176678fea6",
					"relationshipStatus": "ACTIVE",
					"relationshipAttributes": {
						"typeName": "m4i_data_attribute_steward_assignment"
					}
				}
			],
			"ArchiMateReference": [],
			"visualizations": [],
			"domainLead": [],
			"businessOwnerAttribute": [],
			"source": [],
			"dashboards": [],
			"stewardEntity": [],
			"meanings": [],
			"businessOwnerEntity": [],
			"ProcessOwner": []
		},
		"status": "ACTIVE",
		"updateTime": 1664883633559,
		"updatedBy": "atlas",
		"version": 0
	}
}
"""