from core_operation import UpdateLocalAttributeProcessor, Sequence

def test__specify_local_update():
    op = UpdateLocalAttributeProcessor(name="update data entity with value hallo",
                                        key="entity",
                                        value="hallo")
    seq = Sequence("seq",[op])
    spec = jsonpickle.encode(seq)
    
    obj_ = jsonpickle.decode(spec)
    data = {"entity":"unknown", "test":815}
    res_data = obj_.process(data)
    assert(res_data['entity']=="hallo")
    assert(res_data['test']==815)
    assert(len(res_data.keys())==2)
    
def test__specify_local_insert():
    op = UpdateLocalAttributeProcessor(name="insert data entity2 with value hallo",
                                        key="entity2",
                                        value="hallo")
    seq = Sequence("seq",[op])
    spec = jsonpickle.encode(seq)
    
    obj_ = jsonpickle.decode(spec)
    data = {"entity":"unknown", "test":815}
    res_data = obj_.process(data)
    assert(res_data['entity']=="unknown")
    assert(res_data['entity2']=="hallo")
    assert(res_data['test']==815)
    assert(len(res_data.keys())==3)
    
def test__specify_local_unset():
    op = UpdateLocalAttributeProcessor(name="unset data entity",
                                        key="entity",
                                        value=None)
    seq = Sequence("seq",[op])
    spec = jsonpickle.encode(seq)
    
    obj_ = jsonpickle.decode(spec)
    data = {"entity":"unknown", "test":815}
    res_data = obj_.process(data)
    assert(res_data['entity']==None)
    assert(res_data['test']==815)
    assert(len(res_data.keys())==2)
    
def test__update_dq_score():
    doc = [{"name":"id","value":"ff846a87-3c0c-432b-90f5-1b4d3472e270","type":null},{"name":"qualityguid_validity","value":[],"type":"string"},{"name":"definition","value":"","type":"string"},{"name":"qualityguid_accuracy","value":[],"type":"string"},{"name":"dqscoresum_completeness","value":"0.0","type":"float"},{"name":"deriveddatasetguid","value":[],"type":"string"},{"name":"derivedfieldguid","value":[],"type":"string"},{"name":"sourcetype","value":"Business","type":"string"},{"name":"qualityguid_timeliness","value":[],"type":"string"},{"name":"breadcrumbguid","value":["4051f8be-5fd3-44ca-b421-81e90a8fc18b","d540aa10-bd3d-4a22-a8c9-59ef7a5de890","3b769ee2-e3e2-4965-abbd-18c159c6dffc"],"type":"string"},{"name":"breadcrumbtype","value":["m4i_data_domain","m4i_data_entity","m4i_data_entity"],"type":"string"},{"name":"derivedsystemguid","value":[],"type":"string"},{"name":"dqscorecnt_timeliness","value":"0.0","type":"float"},{"name":"deriveddatadomain","value":["Engineering & Estimating"],"type":"string"},{"name":"derivedpersonguid","value":["4eb5ea8b-5a7a-4e9a-b79a-dbf1b4e24f14"],"type":"string"},{"name":"deriveddataattributeguid","value":[],"type":"string"},{"name":"guid","value":"ff846a87-3c0c-432b-90f5-1b4d3472e270","type":"string"},{"name":"parentguid","value":"3b769ee2-e3e2-4965-abbd-18c159c6dffc","type":"string"},{"name":"dqscorecnt_overall","value":"0.0","type":"float"},{"name":"qualityguid_completeness","value":[],"type":"string"},{"name":"deriveddataentity","value":["ee properties","Property"],"type":"string"},{"name":"referenceablequalifiedname","value":"ship-management-department--equipment--own-equipment--property--own-equipment","type":"string"},{"name":"derivedfield","value":[],"type":"string"},{"name":"dqscoresum_overall","value":"0.0","type":"float"},{"name":"dqscore_completeness","value":"0.0","type":"float"},{"name":"derivedcollectionguid","value":[],"type":"string"},{"name":"deriveddatadomainguid","value":["4051f8be-5fd3-44ca-b421-81e90a8fc18b"],"type":"string"},{"name":"deriveddataset","value":[],"type":"string"},{"name":"derivedsystem","value":[],"type":"string"},{"name":"deriveddataownerguid","value":"4eb5ea8b-5a7a-4e9a-b79a-dbf1b4e24f14","type":"string"},{"name":"dqscore_validity","value":"0.0","type":"float"},{"name":"dqscore_overall","value":"0.0","type":"float"},{"name":"supertypenames","value":["Referenceable","m4i_referenceable","m4i_data_entity"],"type":"string"},{"name":"dqscorecnt_accuracy","value":"0.0","type":"float"},{"name":"dqscoresum_uniqueness","value":"0.0","type":"float"},{"name":"deriveddomainleadguid","value":null,"type":"string"},{"name":"dqscore_uniqueness","value":"0.0","type":"float"},{"name":"dqscoresum_accuracy","value":"0.0","type":"float"},{"name":"derivedperson","value":["Carlo Solleveld"],"type":"string"},{"name":"dqscoresum_timeliness","value":"0.0","type":"float"},{"name":"breadcrumbname","value":["Engineering & Estimating","ee properties","Property"],"type":"string"},{"name":"name","value":"Own Equipment","type":"string"},{"name":"deriveddataentityguid","value":["d540aa10-bd3d-4a22-a8c9-59ef7a5de890","3b769ee2-e3e2-4965-abbd-18c159c6dffc"],"type":"string"},{"name":"dqscorecnt_validity","value":"0.0","type":"float"},{"name":"derivedcollection","value":[],"type":"string"},{"name":"dqscoresum_validity","value":"0.0","type":"float"},{"name":"email","value":null,"type":"string"},{"name":"m4isourcetype","value":["m4i_data_entity"],"type":"string"},{"name":"qualityguid_uniqueness","value":[],"type":"string"},{"name":"dqscorecnt_uniqueness","value":"0.0","type":"float"},{"name":"dqscore_timeliness","value":"0.0","type":"float"},{"name":"deriveddatastewardguid","value":null,"type":"string"},{"name":"typename","value":"m4i_data_entity","type":"string"},{"name":"dqscorecnt_completeness","value":"0.0","type":"float"},{"name":"dqscore_accuracy","value":"0.0","type":"float"},{"name":"deriveddataattribute","value":[],"type":"string"}]}