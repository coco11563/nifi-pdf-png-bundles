package org.apache.nifi.processors.ext.pdf.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class AvroUtils {
    @SuppressWarnings("unchecked")
    public static <D> DatumWriter<D> newDatumWriter(Schema schema, Class<D> dClass) {
        return (DatumWriter<D>) GenericData.get().createDatumWriter(schema);
    }

    @SuppressWarnings("unchecked")
    public static <D> DatumReader<D> newDatumReader(Schema schema, Class<D> dClass) {
        return (DatumReader<D>) GenericData.get().createDatumReader(schema);
    }

}
