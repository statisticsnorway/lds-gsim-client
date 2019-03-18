package no.ssb.gsim.client;

import com.apollographql.apollo.ApolloClient;
import com.apollographql.apollo.ApolloQueryCall;
import com.apollographql.apollo.api.Response;
import com.apollographql.apollo.rx2.Rx2Apollo;
import io.reactivex.Observable;
import io.reactivex.Single;
import no.ssb.gsim.client.graphql.GetUnitDatasetQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;

public class GsimClient {

    private static Logger logger = LoggerFactory.getLogger(GsimClient.class);

    private final ApolloClient client;

    private GsimClient(Configuration configuration) {
        logger.debug("setting up with LDS {}", configuration.getLdsUrl());
        client = ApolloClient.builder().serverUrl(configuration.getLdsUrl().toString()).build();
    }

    /**
     * Creates a new GsimClient.
     */
    public static GsimClient newClient(Configuration configuration) {
        return new GsimClient(configuration);
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
