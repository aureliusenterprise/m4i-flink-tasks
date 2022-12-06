# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 00:26:31 2022

@author: andre
"""

import pandas as pd
import json
from pandas.io.json import json_normalize



#%%
# read dead letter box data
print("analyze deadletterbox")
print("--------------------------------")
with open('../dump_dead_letter.log') as f:
    lines = f.readlines()

print('loaded file...')
res = []
for line in lines:
    res.append(json.loads(line))

dlb = pd.json_normalize(res)

dlb.columns
dlb_agg = dlb.groupby(['job','exceptionClass']).size().rename('cnt').reset_index()
print(dlb_agg)

#%%
# read atlas entities data
print()
print("analyze atlas entities")
print("--------------------------------")
with open('../dump_atlas.log') as f:
    lines = f.readlines()

print('loaded file...')
res = []
for line in lines:
    res.append(json.loads(line))

atlas = pd.json_normalize(res)

atlas.columns

atlas_entities = atlas[~atlas['message.entity.guid'].isnull()].copy()
atlas_entities_agg = atlas_entities.groupby(['message.entity.guid','message.entity.typeName','message.operationType']).size().rename('cnt').reset_index()
atlas_unique = list(atlas_entities['message.entity.guid'].unique())

atlas_entities_agg2 = atlas_entities.groupby(['message.entity.guid','message.entity.typeName','message.operationType']).size().rename('cnt').reset_index()
atlas_entities_agg2_agg = atlas_entities_agg2.groupby(['message.entity.typeName','message.operationType']).size().rename('freq').reset_index()
print("unique ids: "+str(len(atlas_unique)))
print(atlas_entities_agg2_agg)

#%%
# read enriched data
print()
print("analyze enriched data")
print("--------------------------------")
with open('../dump_enriched.log') as f:
    lines = f.readlines()

print('loaded file...')
res = []
for line in lines:
    res.append(json.loads(line))

ed = pd.json_normalize(res)

ll= ed.columns
ed[['kafka_notification.message.operationType','kafka_notification.message.entity.typeName','kafka_notification.message.entity.guid']]

ed_agg = ed.groupby(['kafka_notification.message.operationType','kafka_notification.message.entity.typeName','kafka_notification.message.entity.guid']).size().rename('cnt').reset_index()
ed_agg_agg = ed_agg.groupby(['kafka_notification.message.operationType','kafka_notification.message.entity.typeName']).size().rename('freq').reset_index()

ed_unique = list(ed['kafka_notification.message.entity.guid'].unique())
print("unique ids: "+str(len(ed_unique)))
print(ed_agg_agg)

#%%
# read enriched saved data
print()
print("anayze enriched change saved")
print("--------------------------------")
with open('../dump_enriched_save.log') as f:
    lines = f.readlines()

print('loaded file...')
res = []
for line in lines:
    res.append(json.loads(line))

eds = pd.json_normalize(res)

ll = eds.columns
#eds['kafka_notification.message.operationType','kafka_notification.message.entity.typeName','kafka_notification.message.entity.guid']
eds_agg = eds.groupby(['kafka_notification.message.operationType','kafka_notification.message.entity.typeName','kafka_notification.message.entity.guid']).size().rename('cnt').reset_index()
eds_agg_agg = eds_agg.groupby(['kafka_notification.message.operationType','kafka_notification.message.entity.typeName']).size().rename('freq').reset_index()

eds_unique = list(eds['kafka_notification.message.entity.guid'].unique())
print("unique ids: "+str(len(eds_unique)))
print(eds_agg_agg)

#%%
# read determine change data
print()
print("analyze determine change")
print("--------------------------------")
with open('../dump_determined_change.log') as f:
    lines = f.readlines()

print('loaded file...')
res = []
for line in lines:
    res.append(json.loads(line))

dc = pd.json_normalize(res)

dc.columns
dc_agg = dc.groupby(['guid','eventType','directChange']).size().rename('cnt').reset_index()
dc_agg_agg = dc_agg.groupby(['eventType','directChange']).size().rename('freq').reset_index()

dc_unique = dc[dc['directChange']]['guid'].unique()
print("unique ids: "+str(len(dc_unique)))
print(dc_agg_agg)

#%%
# comparison of the different topics
print()
print("start comparison")
print("--------------------------------")
atlas_only = [x for x in atlas_unique if x not in ed_unique]
print(f"atlas only: {atlas_only}")
print()
ed_only = [x for x in ed_unique if x not in eds_unique]
print(f"enriched data only: {ed_only}")
print()
eds_only = [x for x in eds_unique if x not in dc_unique]
print(f"enriched data saved only: {eds_only}")
print()
dc_only = [x for x in dc_unique if x not in eds_unique]
print(f"determine change only: {dc_only}")
print()
atlas_not_processed = atlas_entities[atlas_entities['message.entity.guid'].isin(atlas_only)]
atlas_not_processed_agg = atlas_not_processed.groupby(['message.entity.guid','message.entity.typeName']).size().rename('cnt').reset_index()

atlas_not_processed_agg = atlas_not_processed_agg[atlas_not_processed_agg['message.entity.typeName']!='m4i_source']
# find relevant deadletter messages
msgs = []
for ind, row in atlas_not_processed_agg.iterrows():
    res = dlb[dlb['originalNotification'].apply(lambda x: row['message.entity.guid'] in x)]
    if len(res)>0:
        msgs.append(res)
dlb_missing = pd.DataFrame()
if len(msgs)>0:
    dlb_missing = pd.concat(msgs)