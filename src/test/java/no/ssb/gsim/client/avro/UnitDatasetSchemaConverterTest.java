package no.ssb.gsim.client.avro;

import com.apollographql.apollo.api.Response;
import com.apollographql.apollo.internal.cache.normalized.ResponseNormalizer;
import com.apollographql.apollo.response.OperationResponseParser;
import com.apollographql.apollo.response.ScalarTypeAdapters;
import com.google.common.io.CharStreams;
import no.ssb.gsim.client.graphql.GetUnitDatasetQuery;
import okio.Buffer;
import org.apache.avro.Schema;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;

public class UnitDatasetSchemaConverterTest {

    public static String readFileToString(final Class contextClass,
                                          final String streamIdentifier) throws IOException {

        InputStreamReader inputStreamReader = null;
        try {
            inputStreamReader = new InputStreamReader(contextClass.getResourceAsStream(streamIdentifier));
            return CharStreams.toString(inputStreamReader);
        } catch (IOException e) {
            throw new IOException();
        } finally {
            if (inputStreamReader != null) {
                inputStreamReader.close();
            }
        }
    }

    @Test
    public void convert() throws SchemaConverter.StructureConversionException, IOException {

        String json = readFileToString(getClass(), "simpleUnitDataset.json");
        GetUnitDatasetQuery query = GetUnitDatasetQuery.builder().id("dataset-id").build();

        Response<GetUnitDatasetQuery.Data> response = new OperationResponseParser<GetUnitDatasetQuery.Data, GetUnitDatasetQuery.Data>(
                query, query.responseFieldMapper(), new ScalarTypeAdapters(Collections.emptyMap()), ResponseNormalizer.NO_OP_NORMALIZER
        ).parse(new Buffer().writeUtf8(json));

        UnitDatasetSchemaConverter converter = new UnitDatasetSchemaConverter();
        Schema schema = converter.convert(response.data());
        System.out.println(schema);
    }
}