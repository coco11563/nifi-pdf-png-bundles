package org.apache.nifi.processors.ext.pdf;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;

@Tags({"PDF","PNG","process","Sha0w"})
@CapabilityDescription("通过解析输入的Avro包含的文件地址，将对应的PDF文件转换成PNG文件")
public class PDF2PNGByFileName extends AbstractProcessor {
    Logger logger = LoggerFactory.getLogger(PDF2PNGByFileName.class);

    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

    }
}
