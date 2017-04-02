package com.rsclouds.test;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestHadoopImage {

	public void hadooopreadpic() throws IOException {
        Configuration myConf = new Configuration();
        //�������NameNode��ַ
//        myConf.set("fs.default.name", "hdfs://m01.bj.tcb.hdp:9000");
        //ͼƬ�����HDFS�ϵ�·��
        String hdfspath = "hdfs://192.168.2.3:8020/nanlin_root/C0000685f.png";
        FileSystem myFS = FileSystem.get(myConf);
        FSDataInputStream in = myFS.open(new Path(URI.create(hdfspath)));
        //����ͼƬ��С����Buffer�����Ҳû��ϵ
        byte[] Buffer = new byte[1024 * 1024];
        in.read(Buffer);
        ImageIcon[] image = { new ImageIcon(Buffer) };
        BufferedImage img = new BufferedImage((image.length) * 256, 256, BufferedImage.TYPE_INT_RGB);
        Graphics2D gs = (Graphics2D) img.getGraphics();
//        for (int i = 0; i < image.length; i++) {
//            String k = "";
//        }
        gs.drawImage(image[0].getImage(), 0, 0, image[0].getImageObserver());
        int huabuwid = img.getWidth();
        int huabuhid = img.getHeight();
        for (int i = 0; i < huabuwid; i++) {
            for (int j = 0; j < huabuhid; j++) {
                // ��������ȡ�����Ӧ��RGB
                int rgb = img.getRGB(i, j);
 
                int R = (rgb & 0xff0000) >> 16;
                int G = (rgb & 0xff00) >> 8;
                int B = (rgb & 0xff);
                //��ӡͼƬ�����RGB��Ԫɫ
                System.out.println(String.format("i:%d j:%d R:%d G:%d B:%d", i, j, R, G, B));
            }
        }
        // �ͷ�Graphics2D����
        gs.dispose();
    }
 
	public void merge()throws IOException{
		File file3 = new File("D://nanlin//L04//R000017cc//C0000685e.png");
		 InputStream in = null;
		 int length = (int)file3.length();
		 System.out.println(length);
		 byte[] tempbyte = new byte[length];
		 int len = 0;
		 int off = 0;
		 in = new FileInputStream(file3);
		 while ( (len = in.read(tempbyte, off, length)) != -1){
			 off += len;
			 length -= len;
			 if (length == 0)
				 break;
		 }
		 BufferedImage bi=ImageIO.read(new ByteArrayInputStream(tempbyte));
		 OutputStream out  = new FileOutputStream(new File("D://nanlin//C0000685e_1.png"));
		 ImageIO.write(bi, "png", out);
	}
    public static void main(String[] args) throws IOException {
//        new TestHadoopImage().hadooopreadpic();
    	new TestHadoopImage().merge();
    }
}
