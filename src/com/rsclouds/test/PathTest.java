package com.rsclouds.test;

import java.io.IOException;


public class PathTest {

	public static void main(String[] args) throws IOException{
		String str = "/home/test/";
		if ( str.lastIndexOf('/') != str.length()-1 ){
			System.out.println("true");
		}
		System.out.println(String.valueOf(890));
		byte[] t = "1".getBytes();
		for (int i = 0; i < t.length; i ++){
			System.out.println(t[0]);
		}
//		StringBuilder test = new StringBuilder();
//		String a = "ddddddaa";
//		System.out.println(a.substring(0, a.length() - 2));
//		test.replace(0, test.length(), "aa/nn//cc");
//		test.replace(test.length()-2, test.length(), "dd");
//		int indexof = test.lastIndexOf("/");
//		System.out.println(test.replace(indexof-1, indexof+1, "/"));
//		System.out.println(test.insert(indexof,"/").toString());
//		System.out.println(test.toString());
//		Path path = new Path("hdfs://192.168.2.3:8020/nanlin_root/PyramidCutting/L02/1/11.png");
//		String name = path.getName();
//		System.out.println(name + " " + path.toString() + " " + path.getParent());
//		String tem = "/home/nnali/" + "/";
//		int len = tem.length();
//		StringBuilder str = new StringBuilder(tem);
//		str.replace(len, str.length(), "3");
//		System.out.println(str.toString());
//		str.replace(len, str.length(), "45");
//		System.out.println(str.toString());
//		FileSystem fs = FileSystem.get(new Configuration());
//		fs.copyFromLocalFile(false, new Path("D://nanlin//test1.png"), new Path("/nanlin_root/input"));
	}
}
