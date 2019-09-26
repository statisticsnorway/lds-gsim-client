package no.ssb.gsim.client.avro;

import no.ssb.lds.gsim.okhttp.DimensionalDataset;
import org.apache.avro.Schema;

/**
 * Converts the result of the GetUnitDatasetQuery to avro schema.
 */
public class DimensionalDatasetSchemaConverter implements SchemaConverter<DimensionalDataset> {

    @Override
    public Schema convert(DimensionalDataset source) throws StructureConversionException {
        throw new UnsupportedOperationException("Lack examples");
    }
}
