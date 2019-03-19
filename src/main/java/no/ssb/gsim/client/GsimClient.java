package no.ssb.gsim.client;

import com.apollographql.apollo.ApolloClient;
import com.apollographql.apollo.ApolloQueryCall;
import com.apollographql.apollo.api.Response;
import com.apollographql.apollo.rx2.Rx2Apollo;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import no.ssb.gsim.client.avro.DimensionalDatasetSchemaConverter;
import no.ssb.gsim.client.avro.UnitDatasetSchemaConverter;
import no.ssb.gsim.client.graphql.GetDimensionalDatasetQuery;
import no.ssb.gsim.client.graphql.GetUnitDatasetQuery;
import no.ssb.lds.data.client.DataClient;
import okhttp3.HttpUrl;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;

/**
 * Gsim Java client.
 */
public class GsimClient {

    private static final UnitDatasetSchemaConverter UNIT_DATASET_SCHEMA_CONVERTER = new UnitDatasetSchemaConverter();
    private static final DimensionalDatasetSchemaConverter DIMENSIONAL_DATASET_SCHEMA_CONVERTER =
            new DimensionalDatasetSchemaConverter();

    private static Logger logger = LoggerFactory.getLogger(GsimClient.class);
    private final ApolloClient client;
    private final DataClient dataClient;

    public GsimClient(DataClient dataClient, URL ldsUrl) {
        logger.debug("setting up with LDS {}", ldsUrl);
        client = ApolloClient.builder().serverUrl(Objects.requireNonNull(HttpUrl.get(ldsUrl))).build();
        this.dataClient = dataClient;
    }

    /**
     * Get a unit dataset by ID.
     */
    public Single<Response<GetUnitDatasetQuery.Data>> getUnitDataset(String id) {

        // GraphQL call.
        ApolloQueryCall<GetUnitDatasetQuery.Data> query = client.query(GetUnitDatasetQuery.builder().id(id).build());

        // Rx2 wrapper.
        Observable<Response<GetUnitDatasetQuery.Data>> responseObservable = Rx2Apollo.from(query);

        return responseObservable.firstOrError();
    }

    /**
     * Get a unit dataset by ID.
     */
    public Single<Response<GetDimensionalDatasetQuery.Data>> getDimensionalDataset(String id) {

        // GraphQL call.
        ApolloQueryCall<GetDimensionalDatasetQuery.Data> query = client.query(GetDimensionalDatasetQuery.builder().id(id).build());

        // Rx2 wrapper.
        Observable<Response<GetDimensionalDatasetQuery.Data>> responseObservable = Rx2Apollo.from(query);

        return responseObservable.firstOrError();
    }

    public Single<Schema> getSchema(String datasetID) {
        return getUnitDataset(datasetID).flatMap(dataResponse -> {
            if (dataResponse.hasErrors()) {
                // TODO: Define error.
                return Single.error(new IllegalArgumentException());
            } else {
                return Single.just(UNIT_DATASET_SCHEMA_CONVERTER.convert(dataResponse.data()));
            }
        });
    }

    /**
     * Write a the {@link GenericRecord}s for a dataset.
     */
    public Completable writeData(String datasetID, Flowable<GenericRecord> data, String token) {
        return getSchema(datasetID).flatMapCompletable(schema -> {
            return dataClient.writeData(datasetID, schema, token, data);
        });
    }

    /**
     * Read the {@link GenericRecord}s of a dataset.
     */
    public Flowable<GenericRecord> readDatasetData(String datasetID, String token) {
        return getSchema(datasetID).flatMapPublisher(schema -> {
            return dataClient.readData(datasetID, schema, token);
        });
    }

    public static class Configuration {

        public URL getLdsUrl() {
            try {
                return new URL("http://35.228.232.124/lds/graphql");
            } catch (MalformedURLException e) {
                throw new AssertionError(e);
            }
        }
    }
}
