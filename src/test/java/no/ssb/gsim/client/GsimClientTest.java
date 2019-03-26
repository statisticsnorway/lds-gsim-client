package no.ssb.gsim.client;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import no.ssb.lds.data.client.DataClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class GsimClientTest {

    private GsimClient client;

    @Before
    public void setUp() throws Exception {
        DataClient.Configuration configuration = new DataClient.Configuration(null);
        DataClient noOpDataClient = new DataClient(configuration) {

            Map<String, List<GenericRecord>> data = new LinkedHashMap<>();

            @Override
            public Completable writeData(String id, Schema schema, Flowable<GenericRecord> records, String token) {
                return records.toList().flatMapCompletable(genericRecords -> {
                    this.data.put(id, genericRecords);
                    return Completable.complete();
                });
            }

            @Override
            public Completable convertAndWrite(String s, Schema schema, InputStream inputStream, String s1, String s2) {
                throw new UnsupportedOperationException();
            }

            @Override
            public OutputStream readAndConvert(String s, Schema schema, String s1, String s2) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Flowable<GenericRecord> readData(String id, Schema schema, String token) {
                return Flowable.fromIterable(this.data.get(id));
            }
        };

        GsimClient.Configuration clientConfiguration = new GsimClient.Configuration();
        clientConfiguration.setLdsUrl(new URL("http://35.228.232.124/lds/graphql/"));
        client =         GsimClient.builder()
                .withDataClient(noOpDataClient)
                .withConfiguration(clientConfiguration)
                .build();
    }

    @Test
    public void testWriteData() {

        String datasetID = "b9c10b86-5867-4270-b56e-ee7439fe381e";
        Schema schema = client.getSchema(datasetID).blockingGet();

        List<GenericRecord> records = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
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