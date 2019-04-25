package org.apache.nifi.processors.ext.pdf;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ext.pdf.utils.PNGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Tags({"PDF","PNG","process","Sha0w"})
@CapabilityDescription("通过解析输入的Avro包含的文件地址，将对应的PDF文件转换成PNG文件")
public class PDF2PNGByFileName extends AbstractProcessor {
    final static Logger logger = LoggerFactory.getLogger(PDF2PNGByFileName.class);
    public final static Relationship REL_SUCCESS = new Relationship.Builder()
            .name("sucess")
            .build();

    public final static Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("contain the name of file which can not be processed")
            .autoTerminateDefault(true)
            .build();


    public final static PropertyDescriptor PROCESS_PDF_PATH = new PropertyDescriptor.Builder()
            .name("input path")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .description("需要处理的PDF位置")
            .build();

    public final static PropertyDescriptor PNG_OUTPUT_PATH = new PropertyDescriptor.Builder()
            .name("output path")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .description("生成的PNG文件存放的位置")
            .build();

    public final static PropertyDescriptor DPI_CONFIG = new PropertyDescriptor.Builder()
            .name("png dpi setting")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("200")
            .required(true)
            .description("生成文件的DPI设定")
            .build();

    public final static PropertyDescriptor POOL_SIZE = new PropertyDescriptor.Builder()
            .name("thread pool size")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("10")
            .description("线程池默认大小")
            .required(true)
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
        lpd.add(PROCESS_PDF_PATH);
        lpd.add(PNG_OUTPUT_PATH);
        lpd.add(DPI_CONFIG);
        lpd.add(POOL_SIZE);
        propertyDescriptors = Collections.unmodifiableList(lpd);
        Set<Relationship> rs = new HashSet<>();
        rs.add(REL_FAILURE);
        rs.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(rs);
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        final int poolSize = processContext.getProperty(POOL_SIZE).asInteger();
        final String optPath = processContext.getProperty(PNG_OUTPUT_PATH).getValue();
        final int dpi = processContext.getProperty(DPI_CONFIG).asInteger();
        final Vector<String> fail = new Vector<>();
        final String iptPath = processContext.getProperty(PROCESS_PDF_PATH).getValue();
        List<String> processFile = listFile(iptPath);
        BlockingQueue<Runnable> bq = new LinkedBlockingQueue<>();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(poolSize, poolSize, 0,TimeUnit.MILLISECONDS, bq);
        for (final String s : processFile) {
            pool.submit(() -> PNGUtils.pdf2Image(s, optPath, dpi,logger, fail));
        }
        pool.shutdown();
        try {//等待直到所有任务完成
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.error("本次运行错误的文件数 / 总运行数 : " + fail.size() + " / " + processFile.size());
        if (!fail.isEmpty()) {
            for (String s : fail) {
                FlowFile ff = processSession.create();
                ff = processSession.write(ff, out -> {
                    out.write(s.getBytes(StandardCharsets.UTF_8));
                });
                processSession.transfer(ff, REL_FAILURE);
            }
        }
        processSession.commit();
    }

    public static List<String> listFile(String filePath) {
        List<String> lst = new LinkedList<>();
        File file = new File(filePath);
        File[] fs = file.listFiles();
        if (fs != null) {
            for(File f : fs){
                if(!f.isDirectory())
                    lst.add(f.getPath());
                else {
                    lst.addAll(listFile(f.getPath()));
                }
            }
        } else {
            logger.error("the file path " + filePath + " is empty or in wrong format");
        }
        return lst;
    }

}
