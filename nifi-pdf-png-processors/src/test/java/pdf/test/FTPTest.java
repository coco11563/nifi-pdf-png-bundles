package pdf.test;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.ext.pdf.FTPUploader;
import org.apache.nifi.processors.ext.pdf.SimpleCSVToAvro;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.*;
import java.util.List;

public class FTPTest {
    final private TestRunner testRunnerSPX = TestRunners.newTestRunner(new FTPUploader());
    @Test
    public void testRunnerSPX() throws IOException {
        testRunnerSPX.setProperty(FTPUploader.HOST_NAME, "192.168.3.126");
        testRunnerSPX.setProperty(FTPUploader.PASSWORD,"");
        testRunnerSPX.setProperty(FTPUploader.USER_NAME,"ftp");
        testRunnerSPX.setProperty(FTPUploader.PORT,"21");
        testRunnerSPX.setProperty(FTPUploader.PATH_NAME,"/pngstore");
//        ProcessSession ps = testRunnerSPX.getProcessSessionFactory().createSession();
        ProcessSession ps = testRunnerSPX.getProcessSessionFactory().createSession();
        FlowFile test = ps.write(ps.create(), out -> {
            BufferedReader fis =
                    new BufferedReader(new FileReader
                            (new File("C:\\Users\\coco1\\IdeaProjects\\nifi-pdf-png-bundles\\nifi-pdf-png-processors\\src\\main\\resources\\csv\\combine_red.csv")));
            String str = null;
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
            while ((str = fis.readLine()) != null) {
                bw.write(str);
                bw.newLine();
            }
            bw.flush();
        });
        test = ps.putAttribute(test, "prefix", "test_prefix");

        test = ps.putAttribute(test, "filename", "test_filename");
        testRunnerSPX.enqueue(test);
        testRunnerSPX.run();
    }

}
