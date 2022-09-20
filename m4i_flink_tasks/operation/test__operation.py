from symbol import pass_stmt
from core_operation import UpdateLocalAttributeProcessor, Sequence, WorkflowEngine, UpdateDqScoresProcessor, UpdateListEntryProcessor, InsertPrefixToList, DeletePrefixFromList, ComputeDqScoresProcessor
from core_operation import *
import jsonpickle

def test__specify_local_update():
    op = UpdateLocalAttributeProcessor(name="update data entity with value hallo",
                                        key="entity",
                                        value="hallo")
    seq = Sequence("seq",[op])
    spec = jsonpickle.encode(seq)
    
    obj_ = jsonpickle.decode(spec)
    #APP SEARCH DOCUMENT
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
    doc = {'derivedfield': ['JOB_NAME'], 'deriveddataentity': ['Cost Centre'], 'dqscoresum_timeliness': 0.0, 'qualityguid_completeness': '[]', 'sourcetype': 'Business', 'id': 'c20a7e7b-652b-48c7-9413-cf6f7683206d', 'dqscore_accuracy': 0.0, 'deriveddataentityguid': ['114af845-1ccc-4534-a1f2-45f136959c6e'], 'dqscore_timeliness': 0.0, 'dqscoresum_accuracy': 0.0, 'dqscore_validity': 0.0, 'qualityguid_timeliness': '[]', 'deriveddomainleadguid': None, 'dqscoresum_completeness': 1.0, 'dqscore_uniqueness': 0.0, 'breadcrumbname': ['Finance', 'Cost Centre'], 'dqscoresum_uniqueness': 0.0, 'dqscorecnt_completeness': 1.0, 'breadcrumbguid': ['6f3a7542-9f15-4753-bb19-65d29fcdc330', '114af845-1ccc-4534-a1f2-45f136959c6e'], 'name': 'Job Name', 'dqscore_overall': 1.0, 'guid': 'c20a7e7b-652b-48c7-9413-cf6f7683206d', 'dqscore_completeness': 1.0, 'dqscorecnt_uniqueness': 0.0, 'referenceablequalifiedname': 'finance--cost-centre--job-name', 'parentguid': '114af845-1ccc-4534-a1f2-45f136959c6e', 'dqscoresum_validity': 0.0, 'derivedperson': ['Gerrit Lorenz', 'Ava Ross'], 'dqscorecnt_overall': 1.0, 'dqscorecnt_timeliness': 0.0, 'derivedfieldguid': ['a88127e7-07dc-4d96-8a07-f245c8896740'], 'm4isourcetype': "['m4i_data_attribute']", 'deriveddatastewardguid': '899e3e6a-134f-40ec-b9ab-a95282fa8afc', 'qualityguid_uniqueness': '[]', 'supertypenames': ['Referenceable', 'm4i_referenceable', 'm4i_data_attribute'], 'classificationstext': '', 'deriveddataownerguid': '2b4f65a9-53ce-468f-9568-51c52c13ecf4', 'derivedpersonguid': ['899e3e6a-134f-40ec-b9ab-a95282fa8afc', '2b4f65a9-53ce-468f-9568-51c52c13ecf4'], 'definition': 'This attribute describes the name of the function in which the tasks in the functional area are executed. Including the level, which is a reference to the Job Grade. ', 'dqscorecnt_validity': 0.0, 'qualityguid_accuracy': '[]', 'email': None, 'dqscorecnt_accuracy': 0.0, 'deriveddatadomainguid': ['6f3a7542-9f15-4753-bb19-65d29fcdc330'], 'qualityguid_validity': '[]', 'dqscoresum_overall': 1.0, 'deriveddatadomain': ['Finance'], 'breadcrumbtype': ['m4i_data_domain', 'm4i_data_entity'], 'typename': 'm4i_data_attribute'}
    
    op = UpdateDqScoresProcessor("update dqscores")
    seq = Sequence("seq",[op])
    spec = jsonpickle.encode(seq)
    
    
def test__workflowengine():
    op = UpdateLocalAttributeProcessor(name="update data entity with value hallo",
                                        key="entity",
                                        value="hallo")
    seq = Sequence("seq",[op])
    spec = jsonpickle.encode(seq)
    
    engine = WorkflowEngine(spec)
    
    #APP SEARCH DOCUMENT
    data = {"entity":"unknown", "test":815}
    res_data = engine.run(data)
    
    assert(res_data['entity']=="hallo")
    assert(res_data['test']==815)
    assert(len(res_data.keys())==2)

def test__UpdateListEntryProcessor():
    data = {"breadcrumbname":["Finance", "entity"], "test":815}
    op = UpdateListEntryProcessor(name="update list entry", key="breadcrumbname", old_value="Finance", new_value = "Finance an Control")
    seq = Sequence("seq",[op])
    spec = jsonpickle.encode(seq)
    
    engine = WorkflowEngine(spec)

    res_data = engine.run(data)
    
    assert(res_data['breadcrumbname']==["Finance an Control", "entity"])
    assert(res_data['test']==815)
    assert(len(res_data.keys())==2)

def test__InsertPrefixToList():
    op = InsertPrefixToList(name="",
                                    key="test",
                                    input_list=["hello"])
    seq = Sequence("seq",[op])
    spec = jsonpickle.encode(seq)
    
    engine = WorkflowEngine(spec)
    
    #APP SEARCH DOCUMENT
    data = {"entity":"unknown", "test":["world"]}
    res_data = engine.run(data)
    
    assert(res_data['entity']=="unknown")
    assert(res_data['test']==["hello", "world"])
    assert(len(res_data.keys())==2)

def test__DeletePrefixFromList():
    op = DeletePrefixFromList(name="",
                                    key="test",
                                    index=["hello"])
    seq = Sequence("seq",[op])
    spec = jsonpickle.encode(seq)
    
    engine = WorkflowEngine(spec)
    
    #APP SEARCH DOCUMENT
    data = {"entity":"unknown", "test":["world"]}
    res_data = engine.run(data)
    
    assert(res_data['entity']=="unknown")
    assert(res_data['test']==["hello", "world"])
    assert(len(res_data.keys())==2)

def test__ComputeDqScoresProcessor():
    pass

def test__ResetDQScoresProcessor():
    op = ResetDqScoresProcessor(name="")
    seq = Sequence("seq",[op])
    spec = jsonpickle.encode(seq)
    
    engine = WorkflowEngine(spec)
    
    #APP SEARCH DOCUMENT

    # Charif: This input is wrong: has lists as strings. This is adjusted in the app_search code 
    data = {"derivedfield": ["JOB_NAME"], "deriveddataentity": ["Cost Centre"], "dqscoresum_timeliness": 0.90, "qualityguid_completeness": [], "sourcetype": "Business", "id": "c20a7e7b-652b-48c7-9413-cf6f7683206d", "dqscore_accuracy": 0.0, "deriveddataentityguid": ["114af845-1ccc-4534-a1f2-45f136959c6e"], "dqscore_timeliness": 0.0, "dqscoresum_accuracy": 0.0, "dqscore_validity": 0.0, "qualityguid_timeliness": [], "deriveddomainleadguid": None, "dqscoresum_completeness": 1.0, "dqscore_uniqueness": 0.0, "breadcrumbname": ["Finance", "Cost Centre"], "dqscoresum_uniqueness": 0.0, "dqscorecnt_completeness": 1.0, "breadcrumbguid": ["6f3a7542-9f15-4753-bb19-65d29fcdc330", "114af845-1ccc-4534-a1f2-45f136959c6e"], "name": "Job Name", "dqscore_overall": 1.0, "guid": "c20a7e7b-652b-48c7-9413-cf6f7683206d", "dqscore_completeness": 1.0, "dqscorecnt_uniqueness": 0.0, "referenceablequalifiedname": "finance--cost-centre--job-name", "parentguid": "114af845-1ccc-4534-a1f2-45f136959c6e", "dqscoresum_validity": 0.0, "derivedperson": ["Gerrit Lorenz", "Ava Ross"], "dqscorecnt_overall": 1.0, "dqscorecnt_timeliness": 0.0, "derivedfieldguid": ["a88127e7-07dc-4d96-8a07-f245c8896740"], "m4isourcetype": ["m4i_data_attribute"], "deriveddatastewardguid": "899e3e6a-134f-40ec-b9ab-a95282fa8afc", "qualityguid_uniqueness": [], "supertypenames": ["Referenceable", "m4i_referenceable", "m4i_data_attribute"], "classificationstext": "", "deriveddataownerguid": "2b4f65a9-53ce-468f-9568-51c52c13ecf4", "derivedpersonguid": ["899e3e6a-134f-40ec-b9ab-a95282fa8afc", "2b4f65a9-53ce-468f-9568-51c52c13ecf4"], "definition": "This attribute describes the name of the function in which the tasks in the functional area are executed. Including the level, which is a reference to the Job Grade. ", "dqscorecnt_validity": 0.0, "qualityguid_accuracy": [], "email": null, "dqscorecnt_accuracy": 0.0, "deriveddatadomainguid": ["6f3a7542-9f15-4753-bb19-65d29fcdc330"], "qualityguid_validity": [], "dqscoresum_overall": 1.0, "deriveddatadomain": ["Finance"], "breadcrumbtype": ["m4i_data_domain", "m4i_data_entity"], "typename": "m4i_data_attribute"}
    res_data = engine.run(data)
    
    assert(res_data['"dqscoresum_timeliness"']==0)

def test__InsertElementInList():
    op = InsertElementInList(name="", key="", index="", value="")
    seq = Sequence("seq",[op])
    spec = jsonpickle.encode(seq)
    
    engine = WorkflowEngine(spec)
    
    #APP SEARCH DOCUMENT

    # Charif: This input is wrong: has lists as strings. This is adjusted in the app_search code 
    data = {"derivedfield": ["JOB_NAME"], "deriveddataentity": ["Cost Centre"], "dqscoresum_timeliness": 0.90, "qualityguid_completeness": [], "sourcetype": "Business", "id": "c20a7e7b-652b-48c7-9413-cf6f7683206d", "dqscore_accuracy": 0.0, "deriveddataentityguid": ["114af845-1ccc-4534-a1f2-45f136959c6e"], "dqscore_timeliness": 0.0, "dqscoresum_accuracy": 0.0, "dqscore_validity": 0.0, "qualityguid_timeliness": [], "deriveddomainleadguid": None, "dqscoresum_completeness": 1.0, "dqscore_uniqueness": 0.0, "breadcrumbname": ["Finance", "Cost Centre"], "dqscoresum_uniqueness": 0.0, "dqscorecnt_completeness": 1.0, "breadcrumbguid": ["6f3a7542-9f15-4753-bb19-65d29fcdc330", "114af845-1ccc-4534-a1f2-45f136959c6e"], "name": "Job Name", "dqscore_overall": 1.0, "guid": "c20a7e7b-652b-48c7-9413-cf6f7683206d", "dqscore_completeness": 1.0, "dqscorecnt_uniqueness": 0.0, "referenceablequalifiedname": "finance--cost-centre--job-name", "parentguid": "114af845-1ccc-4534-a1f2-45f136959c6e", "dqscoresum_validity": 0.0, "derivedperson": ["Gerrit Lorenz", "Ava Ross"], "dqscorecnt_overall": 1.0, "dqscorecnt_timeliness": 0.0, "derivedfieldguid": ["a88127e7-07dc-4d96-8a07-f245c8896740"], "m4isourcetype": ["m4i_data_attribute"], "deriveddatastewardguid": "899e3e6a-134f-40ec-b9ab-a95282fa8afc", "qualityguid_uniqueness": [], "supertypenames": ["Referenceable", "m4i_referenceable", "m4i_data_attribute"], "classificationstext": "", "deriveddataownerguid": "2b4f65a9-53ce-468f-9568-51c52c13ecf4", "derivedpersonguid": ["899e3e6a-134f-40ec-b9ab-a95282fa8afc", "2b4f65a9-53ce-468f-9568-51c52c13ecf4"], "definition": "This attribute describes the name of the function in which the tasks in the functional area are executed. Including the level, which is a reference to the Job Grade. ", "dqscorecnt_validity": 0.0, "qualityguid_accuracy": [], "email": null, "dqscorecnt_accuracy": 0.0, "deriveddatadomainguid": ["6f3a7542-9f15-4753-bb19-65d29fcdc330"], "qualityguid_validity": [], "dqscoresum_overall": 1.0, "deriveddatadomain": ["Finance"], "breadcrumbtype": ["m4i_data_domain", "m4i_data_entity"], "typename": "m4i_data_attribute"}
    res_data = engine.run(data)
    
    assert(res_data["dqscoresum_timeliness"]==0)

def test__DeleteElementFromList():
    op = DeleteElementFromList(name="", key="entity", index = 0)

    seq = Sequence("seq",[op])
    spec = jsonpickle.encode(seq)
    
    engine = WorkflowEngine(spec)
    
    #APP SEARCH DOCUMENT

    # Charif: This input is wrong: has lists as strings. This is adjusted in the app_search code 
    data = {"entity":"unknown", "test":["world"]}
    res_data = engine.run(data)

    assert(res_data["test"]==[])

