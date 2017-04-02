package com.rsclouds.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntArray implements WritableComparable<IntArray>{

	private int[] intarray;
	public int[] getIntarray() {
		return intarray;
	}

	public void setIntarray(int[] intarray) {
		this.intarray = intarray;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		for ( int i = 0; i < length; i ++){
			intarray[i] = in.readInt();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (intarray != null){
			int length = intarray.length;
			for (int i = 0; i < length; i ++){
				out.writeInt(intarray[i]);
			}
		}
	}

	@Override
	public int compareTo(IntArray o) {
		return 0;
	}
	
}
