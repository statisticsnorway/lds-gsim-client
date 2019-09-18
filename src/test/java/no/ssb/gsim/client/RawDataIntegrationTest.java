package no.ssb.gsim.client;

import io.reactivex.*;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;
import no.ssb.rawdata.api.*;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static no.ssb.gsim.client.GsimClientTest.setupMockServer;

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
    public void testRawDataIntegration() throws IOException {

        // We make two calls (unit and dimensional)
        setupMockServer(mockWebServer);

        // Does the dataset has data?
        // If yes, get last

        RawdataConsumer consumer = rawdataClient.consumer("test-topic");
        RawdataProducer producer = rawdataClient.producer("test-topic");

        // Convert the consumer to flowable.
        Flowable<RawdataMessage> data = Flowable
                .<Single<RawdataMessage>>generate(emitter -> emitter.onNext(Single.fromFuture(consumer.receiveAsync())))
                .concatMapSingle(single -> single)
                .doOnNext(message -> System.out.printf("[%s]\t\tGot raw message %s\n", Thread.currentThread(), message))
                .onErrorResumeNext(throwable -> {
                    return throwable.getCause() instanceof RawdataClosedException ? Flowable.empty() : Flowable.error(throwable);
                });

        // Get the schema so we can create the records.
        Schema schema = gsimClient.getSchema("d7f1a566-b906-4561-92cb-4758b766335c")
                .observeOn(Schedulers.trampoline())
                .blockingGet();

        System.out.println(schema);

        // Convert the RawdataMessage to GenericRecord.
        Flowable<GenericRecordWithId> recordData = data.map(rawdataMessage -> {
            System.out.printf("[%s]\t\tConverting %s\n", Thread.currentThread(), rawdataMessage);

            // This is the conversion process. One could extract this in a class
            // to make it statefull/clearer.
            // RawdataMessage -> GenericRecordWithId

            GenericData.Record record = new GenericData.Record(schema);

            String person_id = new String(rawdataMessage.get("family_id"));
            Integer num_children = Integer.parseInt(new String(rawdataMessage.get("num_children")));
            record.put("DATA_QUALITY", person_id);
            record.put("FAMILY_ID", person_id);
            record.put("FAMILY_TYPE", person_id);
            record.put("NUM_CHILDREN", num_children);

            return new GenericRecordWithId(record, rawdataMessage.position());
        });

        // Write unbounded in data set.
        // Writes to disk every hour of every 10 elements, whichever comes first.
        Observable<GenericRecordWithId> feedBack = gsimClient.writeDatasetUnbounded(
                "d7f1a566-b906-4561-92cb-4758b766335c", recordData,
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
                            .put("family_id", ("id-" + i).getBytes())
                            .put("num_children", Integer.toString(i).getBytes())
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
