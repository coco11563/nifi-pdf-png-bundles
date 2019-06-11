package org.apache.nifi.processors.ext.pdf;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ext.pdf.utils.AvroUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Tags({"AVRO","ATTRIBUTE","ADD","Sha0w"})
@CapabilityDescription("adding a attribute by the record convey from front processor")
public class PutAttribute extends AbstractProcessor {

    final static Logger logger = LoggerFactory.getLogger(PutAttribute.class);

    public final static Relationship REL_SUCCESS = new Relationship.Builder()
            .name("sucess")
            .build();
    public final static Relationship REL_ORIGIN = new Relationship.Builder()
            .name("origin")
            .build();

    public final static Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("contain the flowfile file which can not be processed")
            .autoTerminateDefault(true)
            .build();

    public final static PropertyDescriptor MODIFIED_FIELD = new PropertyDescriptor.Builder()
            .name("modified field")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .description("需要添加的attribute字段名")
            .build();

    public final static PropertyDescriptor AVRO_SCHEMA = new PropertyDescriptor.Builder()
            .name("AVRO schema")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .description("输入的Avro的Schema")
            .build();
    public final static PropertyDescriptor USING_FIELD = new PropertyDescriptor.Builder()
            .name("using field")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .description("需要使用的avro字段")
            .build();

    private List<PropertyDescriptor> propertyDescriptors;
    private Set<Relationship> relationships;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> lpd = new ArrayList<>();
        lpd.add(MODIFIED_FIELD);
        lpd.add(AVRO_SCHEMA);
        lpd.add(USING_FIELD);
        propertyDescriptors = Collections.unmodifiableList(lpd);
        Set<Relationship> rs = new HashSet<>();
        rs.add(REL_FAILURE);
        rs.add(REL_SUCCESS);
        rs.add(REL_ORIGIN);
        relationships = Collections.unmodifiableSet(rs);
    }
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile ff = session.get();
        if (ff == null) return;
        String modified_field = context.getProperty(MODIFIED_FIELD).getValue();
        String schemaProperty = context.getProperty(AVRO_SCHEMA).getValue();
        String usingField = context.getProperty(USING_FIELD).getValue();
        final Schema schema = new Schema.Parser().parse(schemaProperty);

        session.read(ff, (in) -> {
            DatumReader<GenericRecord> reader = new GenericDatumReader<>();
            DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, reader);
            while (dataFileReader.hasNext()) {
                GenericRecord rec = dataFileReader.next();
                Object get = rec.get(usingField);
                String path = get == null ? null : get.toString();
                FlowFile attrFile = session.create();
                attrFile = session.putAttribute(attrFile, modified_field, path);
                session.transfer(attrFile, REL_SUCCESS);
            }
        });
        session.transfer(ff, REL_ORIGIN);
    }
}
