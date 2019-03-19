package no.ssb.gsim.client.avro;

import org.apache.avro.Schema;

/**
 * SchemaConverter
 */
@FunctionalInterface
public interface SchemaConverter<T> {

    public Schema convert(T source) throws StructureConversionException;

    class StructureConversionException extends Exception {

        public StructureConversionException(Exception e) {
            super(e);
        }

        public StructureConversionException(String msg) {
            super(msg);
        }
    }
}
