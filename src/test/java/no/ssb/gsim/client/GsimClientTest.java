package no.ssb.gsim.client;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import no.ssb.lds.data.client.DataClient;
import no.ssb.lds.data.client.LocalBackend;
import no.ssb.lds.data.client.ParquetProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class GsimClientTest {

    private GsimClient client;

    @Before
    public void setUp() throws Exception {
        ParquetProvider.Configuration parquetConfiguration = new ParquetProvider.Configuration();
        parquetConfiguration.setPageSize(8 * 1024 * 1024);
        parquetConfiguration.setRowGroupSize(64 * 1024 * 1024);

        DataClient.Configuration dataClientConfiguration = new DataClient.Configuration();

        var prefix = Files.createTempDirectory("lds-data-client").toString();
        dataClientConfiguration.setLocation(prefix);

        var dataClient = DataClient.builder()
                .withParquetProvider(new ParquetProvider(parquetConfiguration))
                .withBinaryBackend(new LocalBackend(prefix))
                .withConfiguration(dataClientConfiguration)
                .build();

        GsimClient.Configuration clientConfiguration = new GsimClient.Configuration();
        clientConfiguration.setLdsUrl(new URL("http://localhost:9090/graphql/"));
        client = GsimClient.builder()
                .withDataClient(dataClient)
                .withConfiguration(clientConfiguration)
                .build();
    }

    @Test
    public void testWriteData() {

        String datasetID = "b9c10b86-5867-4270-b56e-ee7439fe381e";
        Schema schema = client.getSchema(datasetID).blockingGet();

        List<GenericRecord> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            records.add(new GenericData.Record(schema));
        }
        for (Schema.Field field : schema.getFields()) {
            // Pipeline friendly.
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