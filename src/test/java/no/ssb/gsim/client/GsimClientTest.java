package no.ssb.gsim.client;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import no.ssb.lds.data.client.DataClient;
import no.ssb.lds.data.client.LocalBackend;
import no.ssb.lds.data.client.ParquetProvider;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.memory.MemoryRawdataClientInitializer;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

public class GsimClientTest {

    private GsimClient client;
    private MockWebServer mockWebServer;
    private MockResponse unitDatasetResponse;


    public static RawdataClient createRawDataClient() {
        ServiceLoader<RawdataClientInitializer> loader = ServiceLoader.load(RawdataClientInitializer.class);
        return StreamSupport.stream(loader.spliterator(), false)
                .filter(rawdataClientInitializer -> rawdataClientInitializer instanceof MemoryRawdataClientInitializer)
                .findFirst()
                .orElseThrow(IllegalArgumentException::new)
                .initialize(new LinkedHashMap<>());
    }

    public static GsimClient createGsimClient(MockWebServer mockWebServer) throws IOException {
        ParquetProvider.Configuration parquetConfiguration = new ParquetProvider.Configuration();
        parquetConfiguration.setPageSize(8 * 1024 * 1024);
        parquetConfiguration.setRowGroupSize(64 * 1024 * 1024);

        DataClient.Configuration dataClientConfiguration = new DataClient.Configuration();

        String prefix = Files.createTempDirectory("lds-data-client").toString();
        dataClientConfiguration.setLocation(prefix);

        DataClient dataClient = DataClient.builder()
                .withParquetProvider(new ParquetProvider(parquetConfiguration))
                .withBinaryBackend(new LocalBackend(prefix))
                .withConfiguration(dataClientConfiguration)
                .build();

        GsimClient.Configuration clientConfiguration = new GsimClient.Configuration();
        clientConfiguration.setLdsUrl(mockWebServer.url("graphql").url());
        return GsimClient.builder()
                .withDataClient(dataClient)
                .withConfiguration(clientConfiguration)
                .build();
    }

    public static MockResponse createUnitDatasetResponse() throws IOException {
        InputStream in = GsimClientTest.class.getResourceAsStream("avro/simpleUnitDataset.json");
        return new MockResponse()
                .setBody(new Buffer().readFrom(in))
                .setResponseCode(200);
    }

    @Before
    public void setUp() throws Exception {

        mockWebServer = new MockWebServer();
        client = createGsimClient(mockWebServer);
        unitDatasetResponse = createUnitDatasetResponse();
        ParquetProvider.Configuration parquetConfiguration = new ParquetProvider.Configuration();
        parquetConfiguration.setPageSize(8 * 1024 * 1024);
        parquetConfiguration.setRowGroupSize(64 * 1024 * 1024);

        DataClient.Configuration dataClientConfiguration = new DataClient.Configuration();

        String prefix = Files.createTempDirectory("lds-data-client").toString();
        dataClientConfiguration.setLocation(prefix);

        DataClient dataClient = DataClient.builder()
                .withParquetProvider(new ParquetProvider(parquetConfiguration))
                .withBinaryBackend(new LocalBackend(prefix))
                .withConfiguration(dataClientConfiguration)
                .build();

        GsimClient.Configuration clientConfiguration = new GsimClient.Configuration();
        clientConfiguration.setLdsUrl(mockWebServer.url("graphql").url());
        client = GsimClient.builder()
                .withDataClient(dataClient)
                .withConfiguration(clientConfiguration)
                .build();
    }

    @Test
    public void testWriteData() {

        mockWebServer.enqueue(unitDatasetResponse);
        mockWebServer.enqueue(unitDatasetResponse);
        mockWebServer.enqueue(unitDatasetResponse);
        mockWebServer.enqueue(unitDatasetResponse);
        mockWebServer.enqueue(unitDatasetResponse);
        mockWebServer.enqueue(unitDatasetResponse);

        String datasetID = "b9c10b86-5867-4270-b56e-ee7439fe381e";
        Schema schema = client.getSchema(datasetID).blockingGet();

        List<GenericRecord> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            records.add(new GenericData.Record(schema));
        }
        // Fill one row at a time for pipeline.
        for (Schema.Field field : schema.getFields()) {
            Schema.Type type = field.schema().getType();
            switch (type) {
                case STRING:
                    for (GenericRecord record : records) {
                        record.put(field.name(), "string");
                    }
                    break;
                case INT:
                    for (GenericRecord record : records) {
                        record.put(field.name(), 1);
                    }
                    break;
            }
        }

        // Write data
        Completable written = client.writeData(datasetID, Flowable.fromIterable(records), "token");

        // Wait.
        written.blockingAwait();

        // Read data
        Flowable<GenericRecord> recordsFlowable = client.readDatasetData(datasetID, "token");

        // Wait.
        List<GenericRecord> recordsList = recordsFlowable.toList().blockingGet();

        assertThat(recordsList).containsExactlyElementsOf(records);
    }
}