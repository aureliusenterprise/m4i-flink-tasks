# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 14:26:15 2022

@author: andre
"""

mapping = {'1. Idea': 'Implementing',
           '2. Scope': 'Implementing',
           '3. Technical Design': 'Implementing'}

df1['Lifecycle Phase'] = df1['Lifecycle Phase'].apply(lambda x: mapping[x] if x in mapping.keys() else '')


mapping
cols = df.columns

new_cols = [mapping[x] for x in cols if x in mapping.keys() else x]
df.columns = new_cols

df['new column'] = 'Trivium'

df1 = df[['Manager','Target path']]
df1['type'] = 'Manager'
df1.columns=['Person', 'target path','type']

df2 = df[['Sponsor','Target path']]
df2['type'] = 'Sponsor'
df2.columns=['Person', 'target path', 'type']

df3 = pd.concat([df1,df2])


'hallo'+str(12)
