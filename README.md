# Linked Data Store (LDS) GSIM client

[![Build Status](https://drone.prod-bip-ci.ssb.no/api/badges/statisticsnorway/lds-gsim-client/status.svg?ref=refs/heads/develop)](https://drone.prod-bip-ci.ssb.no/statisticsnorway/lds-gsim-client)

A LSD client library for GSIM model

## Usage 

Add the module to your project:

```xml
<dependency>
    <groupId>no.ssb.lds</groupId>
    <artifactId>lds-gsim-client</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Instantiate the client: 

```java

// Set the client settings
GsimClient.Configuration clientConfiguration = new GsimClient.Configuration();
clientConfiguration.setLdsUrl(new URL("https://example.com/graphql"));

DataClient dataClient = /* ... */

client = GsimClient.builder()
    .withDataClient(dataClient)
    .withConfiguration(clientConfiguration)
    .build();
```

Read and write data:

```java
List<GenericRecord> records = getGenericRecords();

// Write data
Completable written = client.writeData(datasetID, Flowable.fromIterable(records), "token");

// Wait.
written.blockingAwait();

// Read data
Flowable<GenericRecord> recordsFlowable = client.readDatasetData(datasetID, "token");

// Wait.
List<GenericRecord> recordsList = recordsFlowable.toList().blockingGet();
```

## Schema update

Download the updated schema from the LDS `/graphql` endpoint.
 
```
apollo schema:download --endpoint=http://[lds.server]/graphql src/main/graphql/no/ssb/gsim/client/graphql/schema.json
```

Recompile the project 

```
gradle build
```

Fix the compilation errors if any.

## Class diagram

![Class diagram](http://www.plantuml.com/plantuml/proxy?src=https://raw.githubusercontent.com/statisticsnorway/lds-gsim-client/master/src/main/resources/class-diagram.puml)
