# m4i-flink-tasks

Follow the instructions in Apache Flink Installation:

https://dev.azure.com/AureliusEnterprise/Data%20Governance/_wiki/wikis/Data-Governance.wiki/83/Apache-Flink-Installation

Start the virtual python environment which contains the Apache-Flink library by navigating to the environemnt folder and run:
```
source venv/bin/activate
```
Start the Apache Flink server by navigating to folder "flink" and run the following command:
```
./flink-1.15.0/bin/start-cluster.sh
```
Run the batch processing example by running the following command:
```
./flink-1.15.0/bin/flink run -py batch_processing_example_1_15_0.py
```
The output will be printed in the console

Run the stream processing examply by running the following command:
```
./flink-1.15.0/bin/flink run -py stream_processing_example_1_15_0.py
```
Go the user interface of Apache flink in http://[::1]:8081/ and check the Stdout.

https://dev.azure.com/AureliusEnterprise/Data%20Governance/_wiki/wikis/Data-Governance.wiki/83/Apache-Flink-Installation

This repository currently contains the implementation of 3 Apache Flink jobs: get_entity, publish_state, and determine_change. The python scripts coresponding to the working versions of these jobs do all have 1_15_0 as a suffix.

```

The kafka jars needed to run these scripts can be found here:

https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/1.15.0/

https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/2.2.1/



Creating additional users in keycloak
=====================================
examples of using the keycloak part
https://github.com/marcospereirampj/python-keycloak/blob/master/README.md

Debugging flink tasks
======================

extract the content of the kafka topics into files

```
python dump_kafka_topic.py ATLAS_ENTITIES > dump_atlas.log
python dump_kafka_topic.py DEAD_LETTER_BOX > dump_dead_letter.log
python dump_kafka_topic.py ENRICHED_ENTITIES > dump_enriched.log
python dump_kafka_topic.py ENRICHED_ENTITIES_SAVED > dump_enriched_save.log
python dump_kafka_topic.py DETERMINED_CHANGE > dump_determined_change.log
```

then read in the files you can copy them on the local machine with

```
kubectl cp anwo/flink-jobmanager-64cd955bdd-flc6f:/opt/flink/py_libs/m4i-flink-tasks/scripts/dump_atlas.log ../dump_atlas.log
kubectl cp anwo/flink-jobmanager-64cd955bdd-flc6f:/opt/flink/py_libs/m4i-flink-tasks/scripts/dump_determined_change.log ../dump_determined_change.log
kubectl cp anwo/flink-jobmanager-64cd955bdd-flc6f:/opt/flink/py_libs/m4i-flink-tasks/scripts/dump_dead_letter.log ../dump_dead_letter.log
kubectl cp anwo/flink-jobmanager-64cd955bdd-flc6f:/opt/flink/py_libs/m4i-flink-tasks/scripts/dump_enriched.log ../dump_enriched.log
kubectl cp anwo/flink-jobmanager-64cd955bdd-flc6f:/opt/flink/py_libs/m4i-flink-tasks/scripts/dump_enriched_save.log ../dump_enriched_save.log
```

alternatively you can analyse the dumps with
```
python compare_dump_files.py
```
on the kubernetes machine.
