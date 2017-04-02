package com.rsclouds.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class TestWritable implements WritableComparable<TestWritable>{
	
	private IntWritable value;
	
	public TestWritable(){
		set(new IntWritable());
	}
	
//	public TestWritable(IntWritable tmp){
//		set(tmp);
//	}
	
//	public TestWritable(int tmp){
//		set(new IntWritable(tmp));
//	}
	
	public IntWritable getValue() {
		return value;
	}
	
	public void set(IntWritable tmp){
		this.value = tmp;
	}
	public void setValue(IntWritable value) {
		this.value = value;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
//		value = in.readInt();
		value.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
//		out.writeInt(value);
		value.write(out);
		
	}

	@Override
	public int compareTo(TestWritable o) {
//		int thisValue = this.value;
//	    int thatValue = o.value;
//	    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
		int cmp = value.compareTo(o.getValue());
		return cmp;
	}

}
