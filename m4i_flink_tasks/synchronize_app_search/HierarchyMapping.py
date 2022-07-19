from .parameters import *

hierarchy_mapping = {
    data_entity: data_domain,
    data_attribute: data_entity,
    collection : system,
    dataset : collection,
    field : dataset
}
