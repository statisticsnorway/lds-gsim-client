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

        // Does the dataset has data?
        // If yes, get last


        RawdataConsumer consumer = rawdataClient.consumer("test-topic");
        RawdataProducer producer = rawdataClient.producer("test-topic");

        // Convert the consumer to flowable.
        Flowable<RawdataMessage> data = Flowable.<Single<RawdataMessage>>generate(emitter -> {
            emitter.onNext(Single.fromFuture(consumer.receiveAsync()));
        }).concatMapSingle(single -> single).doOnNext(rawdataMessage -> {
            System.out.printf("[%s]\t\tGot raw message %s\n", Thread.currentThread(), rawdataMessage);
        });//.subscribeOn(Schedulers.trampoline());

        // Get the schema so we can create the records.
        Schema schema = gsimClient.getSchema("b9c10b86-5867-4270-b56e-ee7439fe381e")
                .observeOn(Schedulers.trampoline())
                .blockingGet();

        // Convert the RawdataMessage to GenericRecord.
        Flowable<GenericRecordWithId> recordData = data.map(rawdataMessage -> {
            System.out.printf("[%s]\t\tConverting %s\n", Thread.currentThread(), rawdataMessage);

            // This is the conversion process. One could extract this in a class
            // to make it statefull/clearer.
            // RawdataMessage -> GenericRecordWithId

            GenericData.Record record = new GenericData.Record(schema);

            String person_id = new String(rawdataMessage.get("person_id"));
            record.put("PERSON_ID", person_id);
            record.put("DATA_QUALITY", person_id);
            record.put("MARITAL_STATUS", person_id);
            record.put("MUNICIPALITY", person_id);
            record.put("GENDER", person_id);

            Integer income = Integer.parseInt(new String(rawdataMessage.get("income")));
            record.put("INCOME", income);

            return new GenericRecordWithId(record, rawdataMessage.position());
        });

        // Write unbounded in data set.
        // Writes to disk every hour of every 10 elements, whichever comes first.
        Observable<GenericRecordWithId> feedBack = gsimClient.writeDatasetUnbounded(
                "b9c10b86-5867-4270-b56e-ee7439fe381e", recordData,
                1, TimeUnit.HOURS,
                10, "token"
        );

        // Start producing data from a separate thread. This would typically on the other
        // end of a message queue.
        startProducingData(producer);

        // Start the conversion process. This will continue until the consumer stops or an exception is
        // thrown somewhere in the conversion process. Use subscribe()/doOnError() methods to handle exceptions.
        feedBack.blockingForEach(record -> {
            System.out.printf("[%s]\t\tWritten up to %s\n", Thread.currentThread(), record.getPosition());
        });

    }

    private void startProducingData(RawdataProducer producer) {
        new Thread(() -> {
            try {
                List<String> ids = new ArrayList<>();
                for (int i = 1; i <= 1000; i++) {
                    producer.buffer(producer.builder()
                            .position("external-id-" + i)
                            .put("person_id", ("id-" + i).getBytes())
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
    }

}
