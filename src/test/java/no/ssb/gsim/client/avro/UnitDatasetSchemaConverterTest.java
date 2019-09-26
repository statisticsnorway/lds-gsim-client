package no.ssb.gsim.client.avro;


import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.lds.gsim.okhttp.UnitDataset;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class UnitDatasetSchemaConverterTest {

    private MockWebServer server;

    private MockResponse createResponse(String path) throws IOException {
        InputStream in = this.getClass().getResourceAsStream("/no/ssb/gsim/client/data/" + path);
        return new MockResponse().setBody(new Buffer().readFrom(in));
    }

    @Before
    public void setUp() throws Exception {
        server = new MockWebServer();
    }

    @Test
    public void convert() throws SchemaConverter.StructureConversionException, IOException {

        server.enqueue(createResponse("UnitDataSet_Family_1.json"));
        server.enqueue(createResponse("UnitDataStructure_Family_1.json"));
        server.enqueue(createResponse("LogicalRecord_Family_1.json"));
        server.enqueue(createResponse("InstanceVariable_FamilyIdentityNumber.json"));
        server.enqueue(createResponse("InstanceVariable_FamilyType.json"));
        server.enqueue(createResponse("InstanceVariable_NumOfChildren.json"));
        server.enqueue(createResponse("InstanceVariable_DataQuality.json"));

        server.enqueue(createResponse("RepresentedVariable_FamilyIdentifierNumber.json"));
        server.enqueue(createResponse("DescribedValueDomain_NationalFamilyIdentityNumber.json"));

        server.enqueue(createResponse("RepresentedVariable_FamilyType.json"));
        server.enqueue(createResponse("EnumeratedValueDomain_Family.json"));

        server.enqueue(createResponse("RepresentedVariable_NumOfChildren.json"));
        server.enqueue(createResponse("DescribedValueDomain_Integer.json"));

        server.enqueue(createResponse("RepresentedVariable_DataQuality.json"));
        server.enqueue(createResponse("EnumeratedValueDomain_Quality.json"));

        server.start();
        HttpUrl baseUrl = server.url("/test/");

        UnitDataset.Fetcher fetcher = new UnitDataset.Fetcher();
        fetcher.withPrefix(baseUrl);
        fetcher.withClient(new OkHttpClient());
        fetcher.withMapper(new ObjectMapper());

        UnitDataset dataset = fetcher.fetch("a");

        UnitDatasetSchemaConverter converter = new UnitDatasetSchemaConverter();
        Schema schema = converter.convert(dataset);
        System.out.println(schema);
    }
}