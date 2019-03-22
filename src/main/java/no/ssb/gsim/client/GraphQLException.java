package no.ssb.gsim.client;

import com.apollographql.apollo.api.Error;

import java.util.List;

public class GraphQLException extends Exception {

    private final List<Error> errors;

    public GraphQLException(List<Error> errors) {
        super("GraphQL errors: " + errors.toString());
        this.errors = errors;
    }

}
