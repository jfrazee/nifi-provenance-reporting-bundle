# nifi-provenance-reporting-bundle

NiFi provenance reporting tasks.

## Table of Contents

- [Installation](#installation)
- [Tasks](#tasks)
    - [ElasticsearchProvenanceReporter](#elasticsearchprovenancereporter)
    - [HttpProvenanceReporter](#httpprovenancereporter)
- [Todo](#todo)

## Installation

```sh
$ mvn clean package
$ cp nifi-provenance-reporting-nar/target/nifi-provenance-reporting-nar-0.0.2-SNAPSHOT.nar $NIFI_HOME/lib
$ nifi restart
```

## Tasks

### ElasticsearchProvenanceReporter

Reporting task to write provenance events to an Elasticsearch index.

#### Reporting Task Properties

<img src="elasticsearch_provenance_reporter_properties.png" width=600 />

#### Example Event

<img src="elasticsearch_provenance_reporter_event.png" width=600 />

### HttpProvenanceReporter

Reporting task to POST provenance events to an HTTP web service.

#### Reporting Task Properties

This reporting task can be configured to POST provenance events to an arbitrary web service. Here is an example of using it with Solr (10s commits):

<img src="http_provenance_reporter_properties.png" width=600 />

## Todo

- Add batching support.
- Additional adapters:
    - HDFS
    - Tinkerpop
    - NiFi site-to-site
- Optional inclusion of FlowFile contents.
- Create provenance event for runs of the reporting task.
- Example schemas/mappings for data sources (Elasticsearch mapping, Solr schema, JSON schema).
- Add testing.

## License

Copyright (c) 2016 Joey Frazee. nifi-provenance-reporting-bundle is released under the Apache License Version 2.0.
