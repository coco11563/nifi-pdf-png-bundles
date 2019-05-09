package org.apache.nifi.processors.ext.pdf;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import avro.shaded.com.google.common.collect.ImmutableList;
import avro.shaded.com.google.common.collect.ImmutableSet;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.ext.pdf.utils.AvroUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

@Tags({"CSV","DATA","process","Sha0w"})
@CapabilityDescription("just a simple CSV processor. No quote no ref ,just delimiter.")
public class SimpleCSVToAvro extends AbstractProcessor {
    final static Logger logger = LoggerFactory.getLogger(SimpleCSVToAvro.class);
    public final static Relationship REL_SUCCESS = new Relationship.Builder()
            .name("sucess")
            .build();

    public final static Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("contain the flowfile file which can not be processed")
            .autoTerminateDefault(true)
            .build();

    public static final PropertyDescriptor DELIMITER =
            new PropertyDescriptor.Builder()
            .name("CSV delimiter")
            .description("Delimiter character for CSV records")
            .required(true)
            .addValidator(Validator.VALID)
            .defaultValue("\t")
            .build();

    public static final PropertyDescriptor SCHEMA =
            new PropertyDescriptor.Builder()
            .name("Record schema")
            .description("Outgoing Avro schema for each record created from a CSV row")
            .required(true).addValidator(Validator.VALID)
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
        lpd.add(DELIMITER);
        lpd.add(SCHEMA);
        propertyDescriptors = Collections.unmodifiableList(lpd);
        Set<Relationship> rs = new HashSet<>();
        rs.add(REL_FAILURE);
        rs.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(rs);
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile incomingCSV = session.get();
        if (incomingCSV == null) {
            return;
        }
        String schemaProperty = context.getProperty(SCHEMA)
                .getValue();
        String delimiter = context.getProperty(DELIMITER)
                .getValue();
        final Schema schema = new Schema.Parser().parse(schemaProperty);
        final DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(AvroUtils.newDatumWriter(schema, GenericData.Record.class));
//        BufferedReader bf = new BufferedReader(new InputStreamReader(session.read(incomingCSV)));
        FlowFile outgoingAvro = session.write(incomingCSV, (in, out) -> {
            BufferedReader bf = new BufferedReader(new InputStreamReader(in));
            try (DataFileWriter<GenericData.Record> w = writer.create(schema, out)) {
                String head = bf.readLine();
                String[] schemaL = head.split(delimiter);
                int len = schemaL.length;
                String str = null;
                while ((str = bf.readLine()) != null) {
                    try {
                        GenericData.Record rec = new GenericData.Record(schema);
                        int j = 0;
                        int tf = 0;
                        int t = 0;
                        while (j < len) {
                            tf = t;
                            t = str.indexOf("\t", t) + 1;
                            if (t == 0) t = str.length() - 1;
                            String schemaName = schemaL[j];
                            List<Schema> type = schema.getField(schemaName).schema().getTypes();
                            Object value = null;
                            String subStr = str.substring(tf, t - 1);
                            if (subStr != "") {
                                Schema.Type secType = type.get(0).getType().equals(Schema.Type.NULL)? type.get(1).getType() : type.get(0).getType();
                                switch (secType){
                                   case LONG : value = Long.parseLong(str.substring(tf, t - 1)); break;
                                    case INT : value = Integer.parseInt(subStr); break;
                                   default : value = subStr; break;
                            }
                            }

                            rec.put(schemaL[j],value);
                            j++;
                        }
                        w.append(rec);
                        w.flush();
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        });
        session.transfer(outgoingAvro, REL_SUCCESS);
        session.commit();
    }


}
