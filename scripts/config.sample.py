config = {
    "atlas.server.url": "127.0.0.1:21000/api/atlas",
    "kafka.bootstrap.server.hostname": "127.0.0.1",
    "kafka.bootstrap.server.port": "9027",
    "kafka.consumer.group.id": None,
    "atlas.audit.events.topic.name": "ATLAS_ENTITIES",
    "enriched.events.topic.name": "ENRICHED_ENTITIES",
    "determined.events.topic.name": "DETERMINED_CHANGE",
    "sync_elastic.events.topic.name": "SYNC_ELASTIC",
    "operations.events.topic.name": "LOCAL_OPERATION",
    "exception.events.topic.name": "DEAD_LETTER_BOX",

    "elastic.search.index" : "atlas-dev-test",
    "elastic.app.search.engine.name" : "atlas-dev-test",

    "operations.appsearch.engine.name": "atlas-dev",
    
    "elastic.cloud.username": "elastic",
    "elastic.cloud.id": "YOUR CLOUD ID",
    "elastic.base.endpoint" : "APP-SEACRH-HOSTNAME/api/as/v1",
    "elastic.search.endpoint" : "YOUR_ELASTIC_ENDPOINT",
    "elastic.enterprise.search.endpoint": "YOUR_ELASTIC_SEARCH_ENDPOINT",
    
    "keycloak.server.url" : "http://127.0.0.1:9100/auth/",
    "keycloak.client.id" : "m4i_public",
    "keycloak.realm.name": "m4i",
}



