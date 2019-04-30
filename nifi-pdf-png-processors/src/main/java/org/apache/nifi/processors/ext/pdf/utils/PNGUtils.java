package org.apache.nifi.processors.ext.pdf.utils;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.slf4j.Logger;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

public class PNGUtils {
    public static void pdf2Image(String PdfFilePath,
                                 String dstImgFolder,
                                 int dpi,
                                 Logger logger,
                                 Vector<String> failAccumulator) {
        File file = new File(PdfFilePath);
        PDDocument pdDocument;
        try {
            String imgPDFPath = file.getParent();
            int dot = file.getName().lastIndexOf('.');
            String imagePDFName = file.getName().substring(0, dot); // 获取图片文件名
            String imgFolderPath = null;
            if (dstImgFolder.equals("")) {
                imgFolderPath = imgPDFPath + File.separator;// 获取图片存放的文件夹路径
            } else {
                imgFolderPath = dstImgFolder + File.separator;
            }
            String imgFilePathPrefix = imgFolderPath + imagePDFName.substring(0, 2) + File.separator;
            pdDocument = PDDocument.load(file);
            if (createDirectory(imgFilePathPrefix)) {
                PDFRenderer renderer = new PDFRenderer(pdDocument);
                int pages =  pdDocument.getNumberOfPages();
                StringBuffer imgFilePath = null;
                for (int i = 0; i < pages; i++) {
                    imgFilePath = new StringBuffer();
                    imgFilePath.append(imgFilePathPrefix);
                    imgFilePath.append(imagePDFName);
                    imgFilePath.append("_");
                    imgFilePath.append((i + 1));
                    imgFilePath.append(".png");
                    File dstFile = new File(imgFilePath.toString());
                    BufferedImage image = renderer.renderImageWithDPI(i, dpi);
                    ImageIO.write(image, "png", dstFile);
                }
                logger.info("PDF文档\"" + PdfFilePath + "\"转PNG图片成功！");
            } else {
                logger.error("PDF文档\"" + PdfFilePath + "\"转PNG图片失败：" +
                        "创建" + imgFolderPath + "失败");
            }
        } catch (IOException e) {
            failAccumulator.add(PdfFilePath);
            logger.error("无法转换的pdf路径为：" + PdfFilePath + "原因：" + e.getMessage());
        }
    }
    /**
     * this function will process a local file
     * index - the page num of the pdf
     * parent - origin file name
     * filename - this png name
     * prefix - the prefix (first two word of this pdf)
     */
    public static List<FlowFile> pdf2Image(File PdfFilePath,
                                           int dpi,
                                           Logger logger,
                                           ProcessSession session,
                                           Vector<String> failAccumulator) {
        PDDocument pdDocument;
        List<FlowFile> ret = new LinkedList<>();
        try {
            int dot = PdfFilePath.getName().lastIndexOf('.');
            String imagePDFName = PdfFilePath.getName().substring(0, dot); // 获取图片文件名
            String imgFilePathPrefix = imagePDFName.substring(0, 2);
            pdDocument = PDDocument.load(PdfFilePath);
            PDFRenderer renderer = new PDFRenderer(pdDocument);
            int pages =  pdDocument.getNumberOfPages();
            for (int i = 0; i < pages; i++) {
                BufferedImage image = renderer.renderImageWithDPI(i, dpi);
                FlowFile ff = session.create();
                ff = session.write(ff, out -> {
                    ImageIO.write(image, "png", out);
                });
                ff = session.putAttribute(ff, "index", "" + (i + 1)); //index
                ff = session.putAttribute(ff, "parent", PdfFilePath.getName()); //origin name
                ff = session.putAttribute(ff, "filename", imagePDFName + "_" + (i + 1) + ".png");
                ff = session.putAttribute(ff, "prefix", imgFilePathPrefix);
                ret.add(ff);
            }
            logger.info("PDF文档\"" + PdfFilePath + "\"转PNG图片成功！");
        } catch (IOException e) {
            failAccumulator.add(PdfFilePath.getName());
            logger.error("无法转换的pdf路径为：" + PdfFilePath + "原因：" + e.getMessage());
        }
        return ret;
    }
    /**
     * this function will process a Flowfile
     *
     * index - the page num of the pdf
     * parent - origin file name
     * filename - this png name
     * prefix - the prefix (first two word of this pdf)
     */
    public static List<FlowFile> pdf2Image(FlowFile pdfff,
                                           int dpi,
                                           Logger logger,
                                           ProcessSession session,
                                           Vector<FlowFile> failAccumulator) {
        PDDocument pdDocument;
        List<FlowFile> ret = new LinkedList<>();
        String fileName = null;
        try {
            fileName = pdfff.getAttribute("filename");
            int dot = fileName.lastIndexOf('.');
            String imagePDFName = fileName.substring(0, dot); // 获取图片文件名
            String imgFilePathPrefix = fileName.substring(0, 2);
            pdDocument = PDDocument.load(session.read(pdfff));
            PDFRenderer renderer = new PDFRenderer(pdDocument);
            int pages =  pdDocument.getNumberOfPages();
            for (int i = 0; i < pages; i++) {
                BufferedImage image = renderer.renderImageWithDPI(i, dpi);
                FlowFile ff = session.create();
                ff = session.write(ff, out -> {
                    ImageIO.write(image, "png", out);
                });
                ff = session.putAttribute(ff, "index", "" + (i + 1)); //index
                ff = session.putAttribute(ff, "parent", fileName); //origin name
                ff = session.putAttribute(ff, "filename", imagePDFName + "_" + (i + 1) + ".png");
                ff = session.putAttribute(ff, "prefix", imgFilePathPrefix);
                ret.add(ff);
            }
            logger.info("PDF文档\"" + fileName + "\"转PNG图片成功！");
        } catch (IOException e) {
            if (fileName == null) {
                logger.error("上游的FlowFile文件不含filename属性");
                failAccumulator.add(pdfff);
            } else {
                failAccumulator.add(pdfff);
                logger.error("无法转换的pdf路径为：" + fileName + "原因：" + e.getMessage());
            }
        }
        return ret;
    }
    private static boolean createDirectory(String folder) {
        File dir = new File(folder);
        if (dir.exists()) {
            return true;
        } else {
            return dir.mkdirs();
        }
    }

}
