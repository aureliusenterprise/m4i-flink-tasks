# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 00:26:31 2022

@author: andre
"""

import pandas as pd
import json
from pandas.io.json import json_normalize

with open('../helm-governance/output.txt') as f:
    lines = f.readlines()

print('loaded file...')
res = []
for line in lines:
    res.append(json.loads(line))

df = pd.json_normalize(res)

df_agg = df.groupby('guid').size().rename('cnt').reset_index()
#%%
with open('../helm-governance/deadletter_output.txt') as f:
    lines2 = f.readlines()

print('loaded file...')
res2 = []
for line in lines2[:-1]:
    res2.append(json.loads(line))

df2 = pd.json_normalize(res2)
df2_agg = df2.groupby(['job','exceptionClass']).size().rename('cnt').reset_index()
