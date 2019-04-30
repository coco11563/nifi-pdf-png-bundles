package org.apache.nifi.processors.ext.pdf;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

@Tags({"FTP","UPLOAD","process","Sha0w"})
@CapabilityDescription("通过解析输入的FLOWFILE内的attribute来写入到FTP中，一般用于PDF2PNG的过程" +
        "至少需要一个attribute \"filename\"")
public class FTPUploader extends AbstractProcessor {
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    }
}
