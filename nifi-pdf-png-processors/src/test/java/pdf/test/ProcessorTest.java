package pdf.test;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.ext.pdf.PDF2PNGByFileName;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ProcessorTest {
    final private TestRunner testRunnerSPX = TestRunners.newTestRunner(new PDF2PNGByFileName());
    @Test
    public void testRunnerSPX() throws IOException {
        testRunnerSPX.setProperty(PDF2PNGByFileName.PROCESS_PDF_PATH, "C:\\Users\\coco1\\IdeaProjects\\nifi-pdf-png-bundles\\nifi-pdf-png-processors\\src\\main\\resources\\pdf");
        testRunnerSPX.setProperty(PDF2PNGByFileName.PNG_OUTPUT_PATH,"C:\\Users\\coco1\\IdeaProjects\\nifi-pdf-png-bundles\\nifi-pdf-png-processors\\src\\main\\resources\\out");
        testRunnerSPX.run();
        List<MockFlowFile> ff = testRunnerSPX.getFlowFilesForRelationship(PDF2PNGByFileName.REL_FAILURE);
        int i = 1;
        for (MockFlowFile f : ff) {
            f.assertContentEquals("C:\\Users\\coco1\\IdeaProjects\\nifi-pdf-png-bundles\\nifi-pdf-png-processors\\src\\main\\resources\\pdf\\ddd" + i + ".pdf", StandardCharsets.UTF_8);
            i += 1;
        }
    }

}
