package org.apache.nifi.processors.ext.pdf;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"FTP","UPLOAD","process","Sha0w"})
@CapabilityDescription("通过解析输入的FLOWFILE内的attribute来写入到FTP中，一般用于PDF2PNG的过程" +
        "至少需要一个attribute \"filename\"")
public class FTPUploader extends AbstractProcessor {

    public final static Relationship REL_SUCCESS = new Relationship.Builder()
            .name("sucess")
            .build();

    public final static Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("contain the flowfile file which can not be processed")
            .autoTerminateDefault(true)
            .build();

    //String hostname,String username,String password,Integer port, String pathname, String fileName

    public final static PropertyDescriptor HOST_NAME = new PropertyDescriptor.Builder()
            .name("ftp server hostname")
            .required(true)
            .description("ftp服务器地址")
            .build();

    public final static PropertyDescriptor USER_NAME = new PropertyDescriptor.Builder()
            .name("ftp server username")
            .required(true)
            .defaultValue("ftp")
            .description("ftp用户名")
            .build();

    public final static PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("ftp server password")
            .required(false)
            .description("ftp登陆密码")
            .build();

    public final static PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("ftp server port")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("21")
            .description("ftp服务器端口")
            .build();


    public final static PropertyDescriptor PATH_NAME = new PropertyDescriptor.Builder()
            .name("ftp store file path")
            .required(false)
            .description("ftp服务器存储位置")
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
        lpd.add(HOST_NAME);
        lpd.add(USER_NAME);
        lpd.add(PASSWORD);
        lpd.add(PORT);
        propertyDescriptors = Collections.unmodifiableList(lpd);
        Set<Relationship> rs = new HashSet<>();
        rs.add(REL_FAILURE);
        rs.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(rs);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    }
}
