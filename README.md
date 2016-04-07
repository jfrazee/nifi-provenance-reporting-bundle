# nifi-elasticsearch-reporting-bundle

NiFi Elasticsearch reporting tasks.

## Table of Contents

- [Installation](#installation)
- [Tasks](#tasks)
    - [ElasticsearchProvenanceReporter](#elasticsearchprovenancereporter)
        - [Processor Properties](#processor-properties)
        - [Example Event](#example-event)
- [Todo](#todo)

## Installation

```sh
$ mvn clean package
$ cp nifi-elasticsearch-reporting-nar/target/nifi-elasticsearch-reporting-nar-0.0.1-SNAPSHOT.nar $NIFI_HOME/lib
$ nifi restart
```

## Tasks

### ElasticsearchProvenanceReporter

Reporting task to write provenance events to an Elasticsearch index.

#### Processor Properties

![](elasticsearch_provenance_reporter_properties.png)

#### Example Event

![](elasticsearch_provenance_reporter_event.png)

## Todo

- Use state management API to keep track of the most recent event indexed by ElasticsearchProvenanceReporter.
- Use event id + timestamp for document id instead of event id on its own

## License

Copyright (c) 2016 Joey Frazee. nifi-elasticsearch-reporting-bundle is released under the Apache License Version 2.0.
