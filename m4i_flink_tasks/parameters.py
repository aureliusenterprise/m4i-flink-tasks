
from enum import Enum

guid = "guid"
type_name = "typename"
definition = "definition"
name = "name"
email = "email"

breadcrumb_guid = "breadcrumbguid"
breadcrumb_name = "breadcrumbname"

# Conceptual base types 

data_domain = "m4i_data_domain"
data_entity = "m4i_data_entity"
data_attribute = "m4i_data_attribute"
person = "m4i_person"

# Technical base types 

field = "m4i_field"
dataset = "m4i_dataset"
collection = "m4i_collection"
system = "m4i_system"

# relationships 

domain_lead_assignment  = "m4i_domainLead_assignment" 
data_entity_steward_assignment = "m4i_data_entity_steward_assignment" 
data_entity_business_owner_assignment = "m4i_data_entity_business_owner_assignment" 
data_attribute_steward_assignment = "m4i_data_attribute_steward_assignment"  
data_attribute_business_owner_assignment = "m4i_data_attribute_business_owner_assignment" 

data_entity_assignment = "m4i_data_entity_assignment" 
data_entity_attribute_assignment = "m4i_data_entity_attribute_assignment"
data_attribute_field_assignment = "m4i_data_attribute_field_assignment"
parent_entity_assignment = "m4i_parent_entity_assignment"

# technical base type relationships 

field_assignment = "m4i_field_assignment" 
dataset_assignment = "m4i_dataset_assignment" 
collection_assignment = "m4i_collection_assignment" 
system_parent_assignment = "m4i_system_parent_assignment"
dataset_parent_assignment = "m4i_dataset_parent_assignment"
field_parent_assignment = "m4i_field_parent_assignment"

# process relationships 

process_owner_assignment = "m4i_process_owner_assignment" 
dataset_process_inputs = "dataset_process_inputs" 
process_dataset_outputs = "process_dataset_outputs" 

# elastic relationships 

confluent_environment_assignment = "m4i_confluent_environment_assignment"
kibana_space_assignment = "m4i_kibana_space_assignment"
elastic_field_parent_assignment = "m4i_elastic_field_parent_assignment"
kibana_space_dashboard_assignment = "m4i_kibana_space_dashboard_assignment"

# kafka relationships

kafka_cluster_assignment = "m4i_kafka_cluster_assignment"
kafka_field_parent_assignment = "m4i_kafka_field_parent_assignment"

# kubernetes relationships 
kubernetes_environment_cluster_assignment = "m4i_kubernetes_environment_cluster_assignment"
kubernetes_namespace_deployment_assignment = "m4i_kubernetes_namespace_deployment_assignment"
kubernetes_namespace_cronjob_assignment = "m4i_kubernetes_namespace_cronjob_assignment"
kubernetes_cluster_namespace_assignment = "m4i_kubernetes_cluster_namespace_assignment"
kubernetes_deployment_pod_assignment = "m4i_kubernetes_deployment_pod_assignment"
kubernetes_cronjob_pod_assignment = "m4i_kubernetes_cronjob_pod_assignment" 

system_microservice_assignment = "m4i_system_microservice_assignment" # system process
namespace_kubernetes_service_assignment = "m4i_namespace_kubernetes_service_assignment" # system proces
namespace_ingress_object_assignment = "m4i_namespace_ingress_object_assignment" # system process
cluster_ingress_controller_assignment = "m4i_cluster_ingress_controller_assignment" # system process

ingress_controller_ingress_object_assignment = "m4i_ingress_controller_ingress_object_assignment" # process process
kubernetes_service_microservice_assignment = "m4i_kubernetes_service_microservice_assignment" # process process 
ingress_object_kubernetes_service_assignment = "m4i_ingress_object_kubernetes_service_assignment" # process process
microservice_api_operation_assignment = "m4i_microservice_api_operation_assignment" # process process


# derived entity names

derived_data_domain = "deriveddatadomain"
derived_data_entity = "deriveddataentity"
derived_system = "derivedsystem"
derived_collection = "derivedcollection"
derived_dataset = "deriveddataset"
derived_data_attribute = "deriveddataattribute"


derived_field = "derivedfield"
derived_person = "derivedperson"
derived_dataset_names = "deriveddatasetnames"
derived_entity_names = "derivedentitynames"

# derived guid names

derived_data_domain_guid = "deriveddatadomainguid"
derived_data_entity_guid = 'deriveddataentityguid'
derived_entity_guids = 'derivedentityguids'
derived_data_attribute_guid = 'deriveddataattributeguid'
derived_system_guid = 'derivedsystemguid'
derived_collection_guid = 'derivedcollectionguid'
derived_dataset_guid = 'deriveddatasetguid'
derived_dataset_guids = 'deriveddatasetguids'
derived_field_guid = 'derivedfieldguid'
derived_person_guid  = 'derivedpersonguid'




parent_guid = "parentguid"

breadcrumb_type = "breadcrumbtype"

derived_data_owner_guid = "deriveddataownerguid"
derived_data_steward_guid = "deriveddatastewardguid"
derived_domain_lead_guid = "deriveddomainleadguid"


class SourceType(Enum):
    def __str__(self):
        return str(self.value)

    BUSINESS = "Business"
    TECHNICAL = "Technical"

hierarchical_derived_entity_fields_mapping = {derived_data_domain_guid : derived_data_domain, derived_data_entity_guid : derived_data_entity, derived_data_attribute_guid : derived_data_attribute, derived_system_guid : derived_system, derived_collection_guid : derived_collection, derived_dataset_guid : derived_dataset, derived_field_guid : derived_field}

conceptual_hierarchical_derived_entity_fields_mapping = {derived_data_domain_guid : derived_data_domain, derived_data_entity_guid : derived_data_entity, derived_data_attribute_guid : derived_data_attribute}
technical_hierarchical_derived_entity_fields_mapping = {derived_system_guid : derived_system, derived_collection_guid : derived_collection, derived_dataset_guid : derived_dataset, derived_field_guid : derived_field}

conceptual_hierarchical_derived_entity_guid_fields_list= list(conceptual_hierarchical_derived_entity_fields_mapping.keys())
technical_hierarchical_derived_entity_guid_fields_list= list(technical_hierarchical_derived_entity_fields_mapping.keys())