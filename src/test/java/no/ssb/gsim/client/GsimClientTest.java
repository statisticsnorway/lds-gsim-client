package no.ssb.gsim.client;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import no.ssb.lds.data.client.DataClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;

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
            public Completable writeData(String id, Schema schema, String token, Flowable<GenericRecord> records) {
                return records.toList().flatMapCompletable(genericRecords -> {
                    this.data.put(id, genericRecords);
                    return Completable.complete();
                });
            }

            @Override
            public Flowable<GenericRecord> readData(String id, Schema schema, String token) {
                return Flowable.fromIterable(this.data.get(id));
            }
        };
        client = new GsimClient(
                noOpDataClient, new URL("http://35.228.232.124/lds/graphql/")
        );
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

        client.writeData(datasetID, Flowable.fromIterable(records), "").blockingAwait();
        List<GenericRecord> readRecords = client.readDatasetData(datasetID, null).toList().blockingGet();

        assertThat(readRecords).containsExactlyElementsOf(records);
    }

    @Test
    public void testReadData() {

    }
}