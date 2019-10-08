package no.ssb.gsim.client.avro;

import no.ssb.lds.gsim.okhttp.*;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Converts the result of the {@link UnitDataset} to avro {@link Schema}.
 */
public class UnitDatasetSchemaConverter implements SchemaConverter<UnitDataset> {

    private static Schema.Type convertType(ValueDomain valueDomain) throws StructureConversionException {
        String type = valueDomain.getDataType();
        switch (type) {
            case "STRING":
                return Schema.Type.STRING;
            case "INTEGER":
                return Schema.Type.INT;
            case "LONG":
            case "DATETIME":
                return Schema.Type.LONG;
            case "FLOAT":
                return Schema.Type.FLOAT;
            case "DOUBLE":
                return Schema.Type.DOUBLE;
            case "BOOLEAN":
                return Schema.Type.BOOLEAN;
            default:
                throw new StructureConversionException("unsupported data type: " + type);
        }
    }

    @Override
    public Schema convert(UnitDataset source) throws StructureConversionException {
        try {
            UnitDataStructure unitDataStructure = source.fetchUnitDataStructure().get();
            List<LogicalRecord> logicalRecords = unitDataStructure.fetchLogicalRecords().get();

            // The GSIM model does not supports hierarchical structures yet so we only transform the first logical
            // record.
            if (logicalRecords.isEmpty()) {
                throw new StructureConversionException("missing logical record");
            } else if (logicalRecords.size() > 1) {
                throw new StructureConversionException("more than one logical record");
            } else {
                List<InstanceVariable> instanceVariables = logicalRecords.get(0).fetchInstanceVariables().get();
                List<Schema.Field> fields = new ArrayList<>();
                for (InstanceVariable instanceVariable : instanceVariables) {
                    RepresentedVariable representedVariable = instanceVariable.fetchRepresentedVariable().get();
                    ValueDomain valueDomain = representedVariable.fetchInstanceVariables().get();
                    Schema.Field field = new Schema.Field(
                            instanceVariable.getShortName(),
                            Schema.create(convertType(valueDomain)), "", (Object) null
                    );
                    fields.add(field);
                }

                return Schema.createRecord("unitDataset", "GSIM Unit dataset",
                        "no.ssb.gsim.dataset", false, fields);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new StructureConversionException(ie);
        } catch (ExecutionException ee) {
            throw new StructureConversionException(ee);
        }
    }
}
