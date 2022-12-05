# -*- coding: utf-8 -*-
"""
Created on Fri Nov 18 21:23:37 2022

@author: andre
"""

msg = """
{
	"typeName": "m4i_data_domain",
	"attributes": {
		"archimateReference": [],
		"replicatedTo": null,
		"replicatedFrom": null,
		"qualifiedName": "a1f469ee-77b5-4efa-a54e-8bed7fa259c2",
		"name": "HR",
		"definition": "This is the HR department",
		"typeAlias": "Department"
	},
	"guid": "7069b2cf-b7bb-4954-a858-b80a7702e85b",
	"isIncomplete": false,
	"provenanceType": 0,
	"status": "ACTIVE",
	"createdBy": "atlas",
	"updatedBy": "atlas",
	"createTime": 1668766221174,
	"updateTime": 1668782882951,
	"version": 0,
	"relationshipAttributes": {
		"ArchiMateReference": [],
		"domainLead": [],
		"dataEntity": [],
		"source": [],
		"meanings": []
	},
	"classifications": [],
	"labels": [],
	"proxy": false
}
"""

ev_details = json.loads(msg)
ts = None
if "updateTime" in ev_details.keys():
    ts = ev_details['updateTime']
elif "createTime" in ev_details.keys():
    ts = ev_details['createTime']
