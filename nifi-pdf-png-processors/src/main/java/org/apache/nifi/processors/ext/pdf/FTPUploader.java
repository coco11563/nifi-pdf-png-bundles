package org.apache.nifi.processors.ext.pdf;

import org.apache.commons.net.ftp.FTP;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ext.pdf.utils.FTPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

@Tags({"FTP","UPLOAD","process","Sha0w"})
@CapabilityDescription("通过解析输入的FLOWFILE内的attribute来写入到FTP中，一般用于PDF2PNG的过程" +
        "至少需要一个attribute \"filename\"")
public class FTPUploader extends AbstractProcessor {
    final static Logger logger = LoggerFactory.getLogger(FTPUploader.class);
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
            .addValidator(Validator.VALID)
            .required(true)
            .description("ftp服务器地址")
            .build();

    public final static PropertyDescriptor USER_NAME = new PropertyDescriptor.Builder()
            .name("ftp server username")
            .addValidator(Validator.VALID)
            .required(true)
            .defaultValue("ftp")
            .description("ftp用户名")
            .build();

    public final static PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("ftp server password")
            .addValidator(Validator.VALID)
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
            .addValidator(Validator.VALID)
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
        lpd.add(PATH_NAME);
        propertyDescriptors = Collections.unmodifiableList(lpd);
        Set<Relationship> rs = new HashSet<>();
        rs.add(REL_FAILURE);
        rs.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(rs);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile ff = session.get();
        if (ff == null) return;
        String filename = ff.getAttribute("filename");
        String prefix = ff.getAttribute("prefix");
        String host = context.getProperty(HOST_NAME).getValue();
        String user = context.getProperty(USER_NAME).getValue();
        String passwd = context.getProperty(PASSWORD).getValue();
        String path = context.getProperty(PATH_NAME).getValue();
        int port = context.getProperty(PORT).asInteger();
        try {
            if (FTPUtils.ftpClient == null || FTPUtils.ftpClient.isAvailable())
                FTPUtils.initFtpClient(host, user, passwd, port);
            String putPath = path + "/" + prefix + "/" + filename;
            if (FTPUtils.existFile(putPath)) {
                session.transfer(ff, REL_FAILURE);
                logger.error("file {} is existed in server", new Object[]{putPath});
                return;
            }
            if (!FTPUtils.existFile(path)) {
                FTPUtils.CreateDirecroty(path);
            }
            if (!FTPUtils.existFile(path + File.pathSeparator + prefix)) {
                FTPUtils.CreateDirecroty(path + File.pathSeparator + prefix);
            }
            FTPUtils.uploadFile(host, user, passwd, port, path + "/" + prefix,
                    filename, session.read(ff), logger);
            session.transfer(ff, REL_SUCCESS);
        } catch (IOException e) {
            session.transfer(ff, REL_FAILURE);
            logger.error(e.getMessage());
        } finally {
            session.commit();
        }
    }

}
