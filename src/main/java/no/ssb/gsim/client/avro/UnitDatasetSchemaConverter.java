package no.ssb.gsim.client.avro;

import no.ssb.gsim.client.graphql.GetUnitDatasetQuery;
import no.ssb.gsim.client.graphql.fragment.UnitComponents;
import no.ssb.gsim.client.graphql.fragment.UnitVariableType;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts the result of the {@link GetUnitDatasetQuery} to avro {@link Schema}.
 */
public class UnitDatasetSchemaConverter implements SchemaConverter<GetUnitDatasetQuery.Data> {

    private static Schema.Type convertType(UnitVariableType variable) throws StructureConversionException {
        UnitVariableType.SubstantiveValueDomain domain = variable.substantiveValueDomain();
        String type;
        if (domain instanceof UnitVariableType.AsDescribedValueDomain) {
            type = ((UnitVariableType.AsDescribedValueDomain) domain).dataType();
        } else if (domain instanceof UnitVariableType.AsEnumeratedValueDomain) {
            type = ((UnitVariableType.AsEnumeratedValueDomain) domain).dataType();
        } else {
            throw new StructureConversionException("unsupported domain type: " + domain.__typename());
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
                List<Schema.Field> fields = new ArrayList<>();

                fields.addAll(convertIdentifiers(unitComponents));
                fields.addAll(convertMeasures(unitComponents));
                fields.addAll(convertAttributes(unitComponents));
                return Schema.createRecord("unitDataset", "GSIM Unit dataset",
                        "no.ssb.gsim.dataset", false, fields);
            }
        } catch (RuntimeException re) {
            throw new StructureConversionException(re);
        }

    }

    private List<Schema.Field> convertAttributes(UnitComponents unitComponents) throws StructureConversionException {
        List<Schema.Field> fields = new ArrayList<>();
        for (UnitComponents.Edge2 attributeEdge : unitComponents.attributeComponents().edges()) {
            UnitComponents.Node2 node = attributeEdge.node();
            Schema.Field field = new Schema.Field(
                    node.shortName(),
                    Schema.create(convertType(node.representedVariable().fragments().unitVariableType())),
                    "...",
                    (Object) null
            );
            fields.add(field);
        }
        return fields;
    }

    private List<Schema.Field> convertMeasures(UnitComponents unitComponents) throws StructureConversionException {
        List<Schema.Field> fields = new ArrayList<>();
        for (UnitComponents.Edge1 measures : unitComponents.measureComponents().edges()) {
            UnitComponents.Node1 node = measures.node();
            Schema.Field field = new Schema.Field(
                    node.shortName(),
                    Schema.create(convertType(node.representedVariable().fragments().unitVariableType())),
                    "", (Object) null
            );
            fields.add(field);
        }
        return fields;
    }

    private List<Schema.Field> convertIdentifiers(UnitComponents unitComponents) throws StructureConversionException {
        List<Schema.Field> fields = new ArrayList<>();
        for (UnitComponents.Edge identifierEdges : unitComponents.identifierComponents().edges()) {
            UnitComponents.Node node = identifierEdges.node();
            Schema.Field field = new Schema.Field(
                    node.shortName(),
                    Schema.create(convertType(node.representedVariable().fragments().unitVariableType())),
                    "", (Object) null
            );
            fields.add(field);
        }
        return fields;
    }
}
