package no.ssb.gsim.client.avro;

import no.ssb.gsim.client.graphql.GetDimensionalDatasetQuery;
import org.apache.avro.Schema;

/**
 * Converts the result of the GetUnitDatasetQuery to avro schema.
 */
public class DimensionalDatasetSchemaConverter implements SchemaConverter<GetDimensionalDatasetQuery.Data> {

    @Override
    public Schema convert(GetDimensionalDatasetQuery.Data source) throws StructureConversionException {
        throw new UnsupportedOperationException("Lack examples");
    }
}
