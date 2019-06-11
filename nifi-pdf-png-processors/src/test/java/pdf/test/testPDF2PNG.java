package pdf.test;

import org.apache.nifi.processors.ext.pdf.utils.PNGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.Vector;

import static org.apache.nifi.processors.ext.pdf.PDF2PNGByFileName.listFile;

public class testPDF2PNG {
    private static Logger logger = LoggerFactory.getLogger(testPDF2PNG.class);
    public static void main(String[] args) {
//        PNGUtils.pdf2Image("C:\\Users\\coco1\\IdeaProjects\\nifi-pdf-png-bundles\\nifi-pdf-png-processors\\src\\main\\resources\\pdf\\Chowdhury-2005-Natural-language-processing.pdf"
//        ,"C:\\Users\\coco1\\IdeaProjects\\nifi-pdf-png-bundles\\nifi-pdf-png-processors\\src\\main\\resources\\out",
//                200, logger, new HashSet<String>());

        for (String f : listFile("C:\\Users\\coco1\\Documents\\WeChat Files\\zhehuoaizhuangbi1995\\FileStorage\\File\\2019-04")) {
//            PNGUtils.pdf2Image(new File(f), 200, logger, new Vector<String>())
            System.out.println(f);
        }
    }
}
