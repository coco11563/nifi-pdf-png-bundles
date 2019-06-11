package attr;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.ext.pdf.PutAttribute;
import org.apache.nifi.processors.ext.pdf.utils.AvroUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.List;

public class attriTest {
    final private TestRunner testRunnerSPX = TestRunners.newTestRunner(new PutAttribute());
    Schema schema = null;
    String path = "C:\\Users\\coco1\\IdeaProjects\\nifi-pdf-png-bundles\\nifi-pdf-png-processors\\src\\test\\java\\res\\test.avro";
    DataFileStream<GenericData.Record> gd = null;
    String schemaString = "{\"type\":\"record\",\"name\":\"NiFi_SelectHiveQL_Record\",\"namespace\":\"any.data\",\"fields\":[{\"name\":\"path\",\"type\":[\"null\",\"string\"]}]}";
    @Before
    public void initAvro() throws IOException {
        testRunnerSPX.setProperty(PutAttribute.AVRO_SCHEMA, schemaString);
        testRunnerSPX.setProperty(PutAttribute.MODIFIED_FIELD, "ftp.path");
        testRunnerSPX.setProperty(PutAttribute.USING_FIELD, "path");
        gd = new DataFileStream<>(new FileInputStream(new File(path)), AvroUtils.newDatumReader(schema, GenericData.Record.class));
        schema = new Schema.Parser().parse(schemaString);
    }
    @Test
    public void testAttribute() throws FileNotFoundException {
        ProcessSession ps = testRunnerSPX.getProcessSessionFactory().createSession();
        InputStreamReader is = new FileReader((new File(path)));
        GenericDatumWriter<GenericData.Record> w = new GenericDatumWriter<>(schema);
        BufferedReader fis = new BufferedReader(new FileReader(new File(path)));
        FlowFile ff = ps.create();
//        GenericRecord r = new GenericData.Record(schema);
        ff = ps.write(ff, out -> {
//            ByteArrayOutputStream os = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
            dataFileWriter.create(schema, out);
            while (gd.hasNext()) {
                dataFileWriter.append(gd.next());
            }
            dataFileWriter.close();
//            char[] buffer = new char[1024];
//            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
//            while (fis.read(buffer) != -1) {
//                bw.write(buffer);
//            }
//            bw.flush();
//            Encoder e = EncoderFactory.get().binaryEncoder(out, null);
//            Encoder eos = EncoderFactory.get().binaryEncoder(os, null);
//            while (gd.hasNext()) {
//                w.write(gd.next(), eos);
////                w.write(gd.next(), out);
//            }
//            e.flush();
//            GenericData reflectData = GenericData.get();
//            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
//            Decoder decoder = DecoderFactory.get().binaryDecoder(os.toByteArray(), null);
//            while () {
//                GenericRecord decodedEmployee = reader.read(null, decoder);
//                System.out.println("SSN: "+decodedEmployee.get("path").toString());
//            }
        });
        testRunnerSPX.enqueue(ff);
        testRunnerSPX.run();
        List<MockFlowFile> ffs =
                testRunnerSPX.getFlowFilesForRelationship(PutAttribute.REL_SUCCESS);
        for (MockFlowFile f : ffs) {
                System.out.println(f.getAttribute("ftp.path"));
        }

    }
}
