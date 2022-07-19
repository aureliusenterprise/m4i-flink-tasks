config = {
    "atlas.credentials.username": "admin", # only relevant if used for a local docker instance
    "atlas.server.url": "127.0.0.1:21000/api/atlas",
    "kafka.bootstrap.server.hostname": "127.0.0.1",
    "kafka.bootstrap.server.port": "9027",
    "kafka.consumer.group.id": None,
    "atlas.audit.events.topic.name": "ATLAS_ENTITIES",
    "enriched.events.topic.name": "ENRICHED_ENTITIES",
    "determined.events.topic.name": "DETERMINED_CHANGE",
    "exception.events.topic.name": "DEAD_LETTER_BOX",

    "elastic_search_index" : "atlas-dev-test",

    "elastic_cloud_username": "elastic",
    "elastic_cloud_id": "YOUR CLOUD ID",
    "elastic_base_endpoint" : "APP-SEACRH-HOSTNAME/api/as/v1",

    "keycloak.server.url" : "http://127.0.0.1:9100/auth/",
    "keycloak.client.id" : "m4i_public",
    "keycloak.realm.name": "m4i",
}



