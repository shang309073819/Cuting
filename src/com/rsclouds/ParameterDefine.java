package com.rsclouds;

import org.apache.hadoop.hbase.util.Bytes;

public class ParameterDefine {
	public static final String LOCALTEMPDIR = "localTempfilePrefix";
	public static final String LOCALTEMPDIR_DEFAULT = "";
	public static final String HDFSTEMPDIR	 = "hdfstempdir";
	public static final String HDFSTEMPDIR_DEFAULT = "hdfs://192.168.2.3:8020/nanlin_root/temp";
	public static final String GTDATAPATH = "gd-data_outputpath";
	public static final String GTDATAPATH_DEFAULT = "/";
	public static final int WIDTH_DEFAULT = 256;
	public static final int HEIGHT_DEFAULT = 256;
	public static final String IMGBLOCK_WIDTH = "imgblock_width"; // IMGBLOCK_WIDTH * 256
	public static final int IMGBLOCK_WIDTH_DEFAULT = 100;
	
	
	public static final String RESOURCE_TABLENAME = "resource_table";
	public static final String RESOURCE_TABLENAME_DEFAULT = "res_test";//resource
	public static final byte[] RESOURCE_FAMILY = Bytes.toBytes("img");
	public static final byte[] RESOURCE_LINKS = Bytes.toBytes("links");
	public static final byte[] RESOURCE_DATA = Bytes.toBytes("data");
	
	
	public static final String META_TABLENAME = "meta_table";
	public static final String META_TABLENAME_DEFAULT = "meta_test";//metadata	
	public static final byte[] META_FAMILY = Bytes.toBytes("atts");
	public static final byte[] META_URL = Bytes.toBytes("url");
	public static final byte[] META_DFS = Bytes.toBytes("dfs");
	public static final byte[] META_SIZE = Bytes.toBytes("size");
	public static final byte[] META_TIME = Bytes.toBytes("time");
}
