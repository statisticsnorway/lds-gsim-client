package no.ssb.gsim.client;

import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RawDataIntegrationTest {

    private RawdataClient rawdataClient;
    private GsimClient gsimClient;
    private MockWebServer mockWebServer;
    private MockResponse unitDatasetResponse;

    @Before
    public void setUp() throws Exception {
        mockWebServer = new MockWebServer();
        rawdataClient = GsimClientTest.createRawDataClient();
        gsimClient = GsimClientTest.createGsimClient(mockWebServer);
        unitDatasetResponse = GsimClientTest.createUnitDatasetResponse();
    }

    @Test
    public void testRawDataIntegration() {

        // We make two calls (unit and dimensional)
        mockWebServer.enqueue(unitDatasetResponse);
        mockWebServer.enqueue(unitDatasetResponse);
        mockWebServer.enqueue(unitDatasetResponse);
        mockWebServer.enqueue(unitDatasetResponse);

        RawdataConsumer consumer = rawdataClient.consumer("test-topic", "consumer");
        RawdataProducer producer = rawdataClient.producer("test-topic");

        // Convert the consumer to flowable.
        Flowable<RawdataMessage> data = Flowable.<Single<RawdataMessage>>generate(emitter -> {
            emitter.onNext(Single.fromFuture(consumer.receiveAsync()));
        }).concatMapSingle(single -> single).doOnNext(rawdataMessage -> {
            System.out.printf("[%s]\t\tGot raw message %s\n", Thread.currentThread(), rawdataMessage.id());
        }).subscribeOn(Schedulers.io());

        // Get the schema so we can create the records.
        Schema schema = gsimClient.getSchema("b9c10b86-5867-4270-b56e-ee7439fe381e").blockingGet();

        // Convert the RawdataMessage to GenericRecord.
        Flowable<GenericRecordWithId> recordData = data.map(rawdataMessage -> {
            System.out.printf("[%s]\t\tConverting %s\n", Thread.currentThread(), rawdataMessage.id());

            GenericData.Record record = new GenericData.Record(schema);

            String person_id = new String(rawdataMessage.content().get("id"));
            record.put("PERSON_ID", person_id);
            record.put("DATA_QUALITY", person_id);
            record.put("MARITAL_STATUS", person_id);
            record.put("MUNICIPALITY", person_id);
            record.put("GENDER", person_id);

            Integer income = Integer.parseInt(new String(rawdataMessage.content().get("income")));
            record.put("INCOME", income);

            return new GenericRecordWithId(record, rawdataMessage.id());
        });

        // Write in unbounded dataset.
        Observable<GenericRecordWithId> feedBack = gsimClient.writeDatasetUnbounded(
                "b9c10b86-5867-4270-b56e-ee7439fe381e",
                recordData, 1, TimeUnit.HOURS, 10, ""
        );

        // Add some data from a separate thread
        new Thread(() -> {
            try {
                List<String> ids = new ArrayList<>();
                for (int i = 1; i <= 1000; i++) {
                    producer.buffer(producer.builder()
                            .externalId("external-id-" + i)
                            .put("id", ("id-" + i).getBytes())
                            .put("income", Integer.toString(i).getBytes())
                    );
                    ids.add("external-id-" + i);
                    if (i % 100 == 0) {
                        producer.publish(ids);
                        ids.clear();
                    }
                }
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                // Ignore
            } finally {
                try {
                    rawdataClient.close();
                } catch (Exception e) {
                    // Ignore.
                }
            }
        }).start();

        feedBack.observeOn(Schedulers.newThread())
                .blockingForEach(record -> {
                    System.out.printf("[%s]\t\tAck up to %s\n", Thread.currentThread(), record.getId());
                    consumer.acknowledgeAccumulative(record.getId());
                });

    }

}
