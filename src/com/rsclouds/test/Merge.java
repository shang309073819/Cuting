package com.rsclouds.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Merge {

	public static void main(String[] args) {

		File file1 = new File("/Users/chenshang/Downloads/C0001c5d0.png");
		File file2 = new File("/Users/chenshang/Downloads/C0001c5d1.png");
		File file3 = new File("/Users/chenshang/Downloads/C0001c5d2.png");
		File file4 = new File("/Users/chenshang/Downloads/C0001c5d3.png");
		File file = new File("/Users/chenshang/Downloads/C0001c5d4.png");

		InputStream in = null;
		int length;
		byte[] tempbyte = new byte[512];

		try {
			//append - 如果为 true，则将字节写入文件末尾处，而不是写入文件开始处
			OutputStream out = new FileOutputStream(file, true);
			in = new FileInputStream(file1);
			
			while ((length = in.read(tempbyte, 0, 512)) != -1) {
				out.write(tempbyte, 0, length);
			}
			in.close();

			
			in = new FileInputStream(file2);
			while ((length = in.read(tempbyte, 0, 512)) != -1) {
				out.write(tempbyte, 0, length);
			}
			in.close();

			in = new FileInputStream(file3);
			while ((length = in.read(tempbyte, 0, 512)) != -1) {
				out.write(tempbyte, 0, length);
			}
			in.close();
			
			
			in = new FileInputStream(file4);
			while ((length = in.read(tempbyte, 0, 512)) != -1) {
				out.write(tempbyte, 0, length);
			}
			in.close();

			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
