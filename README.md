# Linked Data Store (LDS) GSIM client

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

## Class diagram

![Class diagram](http://www.plantuml.com/plantuml/proxy?src=https://raw.githubusercontent.com/statisticsnorway/lds-gsim-client/master/src/main/resources/class-diagram.puml)
