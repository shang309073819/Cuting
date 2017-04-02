package com.rsclouds.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FileInfo implements WritableComparable<FileInfo>{
	
	private Text filename;
	private int length;

	public FileInfo(){
		filename = new Text();
		length = 0;
	}
	public FileInfo(Text filename, int length){
		this.filename = filename;
		this.length = length;
	}
	
	public Text getFilename() {
		return filename;
	}

	public void setFilename(Text filename) {
		this.filename = filename;
	}
	
	public void setFilename(String str){
		filename.set(str);
	}
	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		filename.readFields(in);
		length = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		filename.write(out);
		out.writeInt(length);
	}

	@Override
	public int compareTo(FileInfo o) {
		return this.filename.compareTo(o.filename);
	}

}
