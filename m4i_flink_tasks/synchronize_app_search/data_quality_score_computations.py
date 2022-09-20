#from .parameters import *
#from synchronize_app_search import *
#from elastic import *

def fill_in_dq_scores(input_document : dict) -> dict:
    """This function fills in and sets all dq scores to zero in the given document and returns the  updated document."""
    for key in input_document.keys():
        if key.startswith("dqscore"):
            input_document[key] = 0
    return input_document

def update_dq_score_fields(doc, direct_child_entity_docs):
    """This function updates a data quality score of the necessary fields of the document"""
    
    for key in doc.keys():
        if key.startswith("dqscoresum"):
            aggregated_sum = 0
            for child_doc in direct_child_entity_docs:

                
                aggregated_sum += float(child_doc.get(key))

            doc[key] = aggregated_sum

        if key.startswith("dqscorecnt"):
            aggregated_cnt = 0
            for child_doc in direct_child_entity_docs:
                aggregated_cnt += float(child_doc.get(key))

            doc[key] = aggregated_cnt

    for key in doc.keys():
        if key.startswith("dqscore_"):
            if len(key.split("_")) != 2:
                print(
                    "unexpected key is encountered. A data quality score key should always have the following structure: [prefix_suffix]")
            else:
                suffix = key.split("_")[1]
                if doc["dqscoresum_" + suffix] == 0 or doc["dqscorecnt_" + suffix] == 0:
                    doc["dqscore_" + suffix] = 0
                else:
                    doc["dqscore_" + suffix] = doc["dqscoresum_" +
                                                   suffix] / doc["dqscorecnt_" + suffix]

    return doc


def define_attribute_data_quality(doc, attribute_guid, field_guid, app_search, engine_name):
    """This function defines the data quality of the data attribute based on its derived fields."""
    if doc[guid] == attribute_guid:
        attribute_doc = doc

    if doc[guid] == field_guid:
        attribute_doc = get_document(attribute_guid, app_search)

    if type(attribute_doc.get("derivedfieldguid")) != list:
        return attribute_doc

    field_docs = get_documents(
        app_search, engine_name, attribute_doc.get("derivedfieldguid"))

    for key in doc.keys():

        if key.startswith("dqscoresum"):
            aggregated_sum = 0
            for field_doc in field_docs:
                aggregated_sum += float(field_doc.get(key))

            attribute_doc[key] = aggregated_sum

        if key.startswith("dqscorecnt"):
            aggregated_cnt = 0
            for field_doc in field_docs:
                aggregated_cnt += float(field_doc.get(key))

            attribute_doc[key] = aggregated_cnt

    for key in doc.keys():
        if key.startswith("dqscore_"):
            if len(key.split("_")) != 2:
                print(
                    "unexpected key is encountered. A data quality score key should always have the following structure: [prefix_suffix]")
            else:
                suffix = key.split("_")[1]
                if attribute_doc["dqscoresum_" + suffix] == 0 or attribute_doc["dqscorecnt_" + suffix] == 0:
                    attribute_doc["dqscore_" + suffix] = 0
                else:
                    attribute_doc["dqscore_" + suffix] = attribute_doc["dqscoresum_" +
                                                                       suffix] / attribute_doc["dqscorecnt_" + suffix]

    return attribute_doc


