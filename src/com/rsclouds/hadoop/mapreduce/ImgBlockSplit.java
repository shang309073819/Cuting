package com.rsclouds.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ImgBlockSplit extends FileSplit {

	//Storage the affine transformation coefficients
	private double[] adfGeoTransform;

	public ImgBlockSplit() {
		super();
		adfGeoTransform = new double[6];
	}

	public ImgBlockSplit(Path file, long start, long length, String[] hosts,
			double[] adfGeoTransform) {
		super(file, start, length, hosts);
		
		if ( adfGeoTransform.length != 6){
			adfGeoTransform = new double[6];
		}else{
			this.adfGeoTransform = adfGeoTransform.clone();
		}
	}

	public double[] getAdfGeoTransform() {
		return adfGeoTransform;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		for (int i = 0; i < 6; i ++){
			out.writeDouble(adfGeoTransform[i]);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		for (int i = 0; i < 6; i ++){
			adfGeoTransform[i] = in.readDouble();
		}
	}
}
