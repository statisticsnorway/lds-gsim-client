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

## Class diagram

![Class diagram](http://www.plantuml.com/plantuml/proxy?src=https://raw.githubusercontent.com/statisticsnorway/lds-gsim-client/19fc00184b1645696491704c2481decb11b0d2bc/src/main/resources/class-diagram.puml)
