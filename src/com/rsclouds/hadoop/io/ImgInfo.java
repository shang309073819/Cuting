package com.rsclouds.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

//WritableComparable接口是可序列化并且可比较的接口
public class ImgInfo implements WritableComparable<ImgInfo> {

	private Text path;
	private double[] adfGeoTransform;

	public ImgInfo() {
		path = new Text();
		adfGeoTransform = new double[6];
	}

	public ImgInfo(Text path, double[] arrayDouble) {
		this.path = path;
		this.adfGeoTransform = arrayDouble.clone();
	}

	public Text getPath() {
		return path;
	}

	public void setPath(Text path) {
		this.path = path;
	}

	public double[] getAdfGeoTransform() {
		return adfGeoTransform;
	}

	public void setAdfGeoTransform(double[] adfGeoTransform) {
		this.adfGeoTransform = adfGeoTransform.clone();
	}

	@Override
	// 反序列化
	public void readFields(DataInput in) throws IOException {
		//deserialize
		path.readFields(in);
		for (int i = 0; i < 6; i++) {
			adfGeoTransform[i] = in.readDouble();
		}
	}

	@Override
	// 序列化
	public void write(DataOutput out) throws IOException {
		//serialize write this object to out length uses zero-compressed encoding
		path.write(out);
		for (int i = 0; i < 6; i++) {
			out.writeDouble(adfGeoTransform[i]);
		}
	}

	@Override
	// 比较函数
	public int compareTo(ImgInfo o) {
		return this.path.compareTo(o.getPath());
	}
}
