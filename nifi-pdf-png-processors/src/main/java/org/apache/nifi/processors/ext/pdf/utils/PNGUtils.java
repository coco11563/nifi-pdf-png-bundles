package org.apache.nifi.processors.ext.pdf.utils;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.slf4j.Logger;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
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

    private static boolean createDirectory(String folder) {
        File dir = new File(folder);
        if (dir.exists()) {
            return true;
        } else {
            return dir.mkdirs();
        }
    }

}
