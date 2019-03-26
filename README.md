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
 