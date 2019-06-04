package no.ssb.gsim.client.avro;

import no.ssb.gsim.client.graphql.GetUnitDatasetQuery;
import no.ssb.gsim.client.graphql.fragment.UnitComponents;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts the result of the {@link GetUnitDatasetQuery} to avro {@link Schema}.
 */
public class UnitDatasetSchemaConverter implements SchemaConverter<GetUnitDatasetQuery.Data> {

    private static Schema.Type convertType(UnitComponents.SubstantiveValueDomain valueDomain) throws StructureConversionException {
        String type;
        if (valueDomain instanceof UnitComponents.AsEnumeratedValueDomain) {
            type = ((UnitComponents.AsEnumeratedValueDomain) valueDomain).dataType();
        } else if (valueDomain instanceof UnitComponents.AsDescribedValueDomain) {
            type = ((UnitComponents.AsDescribedValueDomain) valueDomain).dataType();
        } else {
            throw new StructureConversionException("unsupported domain type: " + valueDomain.__typename());
        }

        switch (type) {
            case "STRING":
                return Schema.Type.STRING;
            case "INTEGER":
                return Schema.Type.INT;
            case "FLOAT":
                return Schema.Type.DOUBLE;
            case "DATETIME":
                return Schema.Type.INT;
            case "BOOLEAN":
                return Schema.Type.BOOLEAN;
            default:
                throw new StructureConversionException("unsupported data type: " + type);
        }

    }

    @Override
    public Schema convert(GetUnitDatasetQuery.Data source) throws StructureConversionException {
        try {
            GetUnitDatasetQuery.LogicalRecords logicalRecords = source.UnitDataSetById().unitDataStructure()
                    .logicalRecords();

            // The GSIM model does not supports hierarchical structures yet so we only transform the first logical
            // record.
            if (logicalRecords.edges().isEmpty()) {
                throw new StructureConversionException("missing logical record");
            } else if (logicalRecords.edges().size() > 1) {
                throw new StructureConversionException("more than one logical record");
            } else {
                UnitComponents unitComponents = logicalRecords.edges().get(0).node().fragments().unitComponents();
                List<UnitComponents.Edge> instanceVariables = unitComponents.instanceVariables().edges();

                List<Schema.Field> fields = new ArrayList<>();
                for (UnitComponents.Edge instanceVariable : instanceVariables) {
                    UnitComponents.SubstantiveValueDomain substantiveValueDomain = instanceVariable.node().representedVariable().substantiveValueDomain();
                    String shortName = instanceVariable.node().shortName();

                    Schema.Field field = new Schema.Field(
                            shortName,
                            Schema.create(convertType(substantiveValueDomain)), "", (Object) null
                    );

                    fields.add(field);
                }

                return Schema.createRecord("unitDataset", "GSIM Unit dataset",
                        "no.ssb.gsim.dataset", false, fields);
            }
        } catch (RuntimeException re) {
            throw new StructureConversionException(re);
        }

    }
}
