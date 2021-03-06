package no.ssb.gsim.client;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

class GenericRecordWithId implements GenericRecord {

    private final GenericRecord delegate;
    private final String position;

    GenericRecordWithId(GenericRecord delegate, String position) {
        this.delegate = delegate;
        this.position = position;
    }

    public String getPosition() {
        return position;
    }

    @Override
    public void put(String s, Object o) {
        delegate.put(s, o);
    }

    @Override
    public Object get(String s) {
        return delegate.get(s);
    }

    @Override
    public void put(int i, Object o) {
        delegate.put(i, o);
    }

    @Override
    public Object get(int i) {
        return delegate.get(i);
    }

    @Override
    public Schema getSchema() {
        return delegate.getSchema();
    }
}
