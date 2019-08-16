package no.ssb.gsim.client;

import com.apollographql.apollo.ApolloClient;
import com.apollographql.apollo.ApolloQueryCall;
import com.apollographql.apollo.api.Response;
import com.apollographql.apollo.rx2.Rx2Apollo;
import io.reactivex.*;
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

import java.io.IOException;
import java.net.URL;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Gsim Java client.
 */
public class GsimClient {

    private static final Logger logger = LoggerFactory.getLogger(GsimClient.class);

    private static final UnitDatasetSchemaConverter UNIT_DATASET_SCHEMA_CONVERTER = new UnitDatasetSchemaConverter();
    private static final DimensionalDatasetSchemaConverter DIMENSIONAL_DATASET_SCHEMA_CONVERTER =
            new DimensionalDatasetSchemaConverter();

    private final ApolloClient client;
    private final DataClient dataClient;

    private GsimClient(Builder builder) {
        URL ldsUrl = builder.configuration.getLdsUrl();
        logger.debug("setting up with LDS {}", ldsUrl);
        client = ApolloClient.builder().serverUrl(Objects.requireNonNull(HttpUrl.get(ldsUrl))).build();
        this.dataClient = builder.dataClient;

    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Get a unit dataset by ID.
     */
    public Single<GetUnitDatasetQuery.Data> getUnitDataset(String id) {

        // GraphQL call.
        ApolloQueryCall<GetUnitDatasetQuery.Data> query = client.query(GetUnitDatasetQuery.builder().id(id).build());

        // Rx2 wrapper.
        Single<Response<GetUnitDatasetQuery.Data>> response = Rx2Apollo.from(query).singleOrError();
        return response.flatMap(dataResponse -> {
            if (dataResponse.hasErrors()) {
                return Single.error(new GraphQLException(dataResponse.errors()));
            } else {
                return Single.just(dataResponse.data());
            }
        });
    }

    /**
     * Get a dimensional dataset by ID.
     */
    public Single<GetDimensionalDatasetQuery.Data> getDimensionalDataset(String id) {

        // GraphQL call.
        ApolloQueryCall<GetDimensionalDatasetQuery.Data> query = client.query(GetDimensionalDatasetQuery.builder().id(id).build());

        // Rx2 wrapper.
        Single<Response<GetDimensionalDatasetQuery.Data>> response = Rx2Apollo.from(query).singleOrError();
        return response.flatMap(dataResponse -> {
            if (dataResponse.hasErrors()) {
                return Single.error(new GraphQLException(dataResponse.errors()));
            } else {
                return Single.just(dataResponse.data());
            }
        });
    }

    public Single<Schema> getSchema(String datasetID) {
        Maybe<Schema> unitSchema = getUnitDataset(datasetID)
                .toMaybe().onErrorComplete()
                .map(UNIT_DATASET_SCHEMA_CONVERTER::convert);

        Maybe<Schema> dimensionalSchema = getDimensionalDataset(datasetID)
                .toMaybe().onErrorComplete()
                .map(DIMENSIONAL_DATASET_SCHEMA_CONVERTER::convert);

        return Maybe.mergeDelayError(unitSchema, dimensionalSchema).firstOrError();
    }

    /**
     * Write unbounded data to a dataset.
     */
    public <R extends GenericRecord> Observable<R> writeDatasetUnbounded(String datasetID, Flowable<R> data, String token) {
        return writeDatasetUnbounded(datasetID, data, 1, TimeUnit.DAYS, 5_000_000, token);
    }

    public <R extends GenericRecord> Observable<R> writeDatasetUnbounded(
            String datasetID, Flowable<R> data, long timeWindow, TimeUnit unit, long countWindow,
            String token
    ) {
        return getSchema(datasetID).flatMapObservable(schema -> {
            return dataClient.writeDataUnbounded(() -> String.format("%s/%s", datasetID, System.nanoTime()), schema, data, timeWindow, unit, countWindow, token);
        });
    }

    /**
     * Write a the {@link GenericRecord}s for a dataset.
     */
    public Completable writeData(String datasetID, Flowable<GenericRecord> data, String token) {
        return Completable.defer(() -> {
            DataWriter dataWriter = writeData(datasetID, token);
            return data.doAfterNext(record -> dataWriter.save(record))
                    .doOnComplete(() -> dataWriter.close())
                    .doOnError(throwable -> dataWriter.cancel())
                    .ignoreElements();
        });
    }

    /**
     * Write a the {@link GenericRecord}s for a dataset.
     */
    public DataWriter writeData(String datasetID, String token) throws IOException {
        Schema schema = getSchema(datasetID).blockingGet();
        DataClient.DataWriter dataClientWriter = dataClient.writeData(datasetID, schema, token);
        return new DataWriter(datasetID, schema, dataClientWriter);
    }

    /**
     * Read the {@link GenericRecord}s of a dataset.
     */
    public Flowable<GenericRecord> readDatasetData(String datasetID, String token) {
        return getSchema(datasetID).flatMapPublisher(schema -> {
            return dataClient.readData(datasetID, schema, token, null);
        });
    }

    public static class Builder {

        private DataClient dataClient;
        private Configuration configuration;

        public Builder withDataClient(DataClient dataClient) {
            this.dataClient = dataClient;
            return this;
        }

        public Builder withConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public GsimClient build() {
            return new GsimClient(this);
        }
    }

    public static class Configuration {

        private URL ldsUrl;

        public URL getLdsUrl() {
            return ldsUrl;
        }

        public void setLdsUrl(URL ldsUrl) {
            this.ldsUrl = ldsUrl;
        }
    }

    /**
     * Writer abstraction.
     */
    public class DataWriter implements AutoCloseable {

        private final DataClient.DataWriter delegate;

        private DataWriter(String datasetId, Schema schema, DataClient.DataWriter dataClientWriter) throws IOException {
            this.delegate = dataClientWriter;
        }

        /**
         * Push down a generic record.
         * <p>
         * Note that the record might be buffered. Calling {@link #close()} after this method
         * guaranties that the given record is written.
         *
         * @param record the record to save.
         */
        public void save(GenericRecord record) throws IOException {
            delegate.save(record);
        }

        public void cancel() throws IOException {
            delegate.cancel();
        }

        /**
         * Write all buffered records and update the metadata in LDS.
         */
        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
