package no.ssb.gsim.client;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import no.ssb.lds.data.client.DataClient;
import no.ssb.lds.data.client.LocalBackend;
import no.ssb.lds.data.client.ParquetProvider;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.memory.MemoryRawdataClientInitializer;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.*;
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

    static MockResponse createResponse(String path) {
        InputStream in = GsimClientTest.class.getResourceAsStream("/no/ssb/gsim/client/data/" + path);
        try {
            return new MockResponse().setBody(new Buffer().readFrom(in));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }


    @Test
    public void testWriteData() throws IOException {

        setupMockServer(mockWebServer);
        setupMockServer(mockWebServer);
        setupMockServer(mockWebServer);
        setupMockServer(mockWebServer);
        setupMockServer(mockWebServer);

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
                        record.put(field.name(), new Utf8("string"));
                    }
                    break;
                case LONG:
                    for (GenericRecord record : records) {
                        record.put(field.name(), 1L);
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

        assertThat(recordsList).usingElementComparator((r1, r2) -> {
            for (Schema.Field field : r1.getSchema().getFields()) {
                if (!r1.get(field.pos()).equals(r2.get(field.pos()))) {
                    return -1;
                }
            }
            return 0;
        }).containsExactlyElementsOf(records);
    }

    static void setupMockServer(MockWebServer server) throws IOException {

        final Dispatcher dispatcher = new Dispatcher() {

            @Override
            public MockResponse dispatch (RecordedRequest request) throws InterruptedException {
                String path = request.getPath();
                String id = path.substring(path.lastIndexOf("/"));
                switch (id) {
                    case "/d7f1a566-b906-4561-92cb-4758b766335c":
                        return createResponse("UnitDataSet_Family_1.json");
                    case "/b33b094b-9250-44ba-8647-d24b53b88ded":
                        return  createResponse("UnitDataStructure_Family_1.json");
                    case "/52264328-76be-4ea3-9d86-a0853751e7f4":
                        return  createResponse("LogicalRecord_Family_1.json");
                    case "/e2288748-7d26-4690-b07b-30a58c4a41f4":
                        return  createResponse("InstanceVariable_FamilyIdentityNumber.json");
                    case "/98bf9718-3964-4eb7-a966-3be58d3b9e55":
                        return  createResponse("InstanceVariable_FamilyType.json");
                    case "/9ce825c9-ff8e-4e64-89a9-8d066c9349e7":
                        return  createResponse("InstanceVariable_NumOfChildren.json");
                    case "/8d51f269-4216-4351-94e5-94602e704694":
                        return  createResponse("InstanceVariable_DataQuality.json");
                    case "/950b417a-0b3c-4909-aa4b-5689a1378b8e":
                        return  createResponse("RepresentedVariable_FamilyIdentifierNumber.json");
                    case "/49c54ebc-e742-488a-8a8a-c6fe63410e4f":
                        return  createResponse("DescribedValueDomain_NationalFamilyIdentityNumber.json");
                    case "/d3648cc7-4a62-4da0-a168-5287949fbf56":
                        return  createResponse("RepresentedVariable_FamilyType.json");
                    case "/a6f6d20d-e5eb-449b-8d52-b65c1b22c8ed":
                        return  createResponse("EnumeratedValueDomain_Family.json");
                    case "/d0dbb7eb-e0ca-44d3-909f-1dd7a9cb3249":
                        return  createResponse("RepresentedVariable_NumOfChildren.json");
                    case "/42c799a6-952f-4504-abe2-92f2bd478273":
                        return  createResponse("DescribedValueDomain_Integer.json");
                    case "/51d14a9d-cf08-4d12-a680-0e97a7288ef6":
                        return  createResponse("RepresentedVariable_DataQuality.json");
                    case "/49796e15-26a2-420a-aa3b-35c9f72cd125":
                        return  createResponse("EnumeratedValueDomain_Quality.json");
                }
                return new MockResponse().setResponseCode(404);
            }
        };

        server.setDispatcher(dispatcher);
    }
}