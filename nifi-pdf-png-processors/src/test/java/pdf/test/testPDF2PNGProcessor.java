package pdf.test;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.ext.pdf.FTPUploader;
import org.apache.nifi.processors.ext.pdf.PDF2PNGByFileName;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class testPDF2PNGProcessor {
    final private TestRunner testRunnerSPX = TestRunners.newTestRunner(new PDF2PNGByFileName());
    @Test
    public void testRunnerSPX() throws IOException {
        testRunnerSPX.setProperty(PDF2PNGByFileName.PNG_OUTPUT_PATH,"null");
        ProcessSession ps = testRunnerSPX.getProcessSessionFactory().createSession();
        FlowFile test = ps.write(ps.create(), out -> {
            InputStream fis =
                    new FileInputStream
                            (new File(
                                    "C:\\Users\\coco1\\IdeaProjects\\nifi-pdf-png-bundles\\nifi-pdf-png-processors\\src\\main\\resources\\pdf\\Chowdhury-2005-Natural-language-processing.pdf"));
            byte[] buffer = new byte[1024];
            while ((fis.read(buffer)) != -1) {
                out.write(buffer);
//                bw.newLine();
            }
            out.flush();
            fis.close();
            out.close();
        });
        testRunnerSPX.enqueue(test);
        testRunnerSPX.run();
        List<MockFlowFile> ffs =
                testRunnerSPX.getFlowFilesForRelationship(PDF2PNGByFileName.REL_SUCCESS);
        for (FlowFile ff : ffs) {
            System.out.println(ff.getAttribute("prefix"));
        }
    }

}
