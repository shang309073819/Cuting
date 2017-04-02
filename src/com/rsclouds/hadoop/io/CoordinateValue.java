package com.rsclouds.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

public class CoordinateValue implements WritableComparable<CoordinateValue>{
	private int row_coor;
	private int col_coor;
	private BytesWritable value;

	public CoordinateValue(){
		value = new BytesWritable();
	}
	public int getRow_coor() {
		return row_coor;
	}

	public void setRow_coor(int row_coor) {
		this.row_coor = row_coor;
	}

	public int getCol_coor() {
		return col_coor;
	}

	public void setCol_coor(int col_coor) {
		this.col_coor = col_coor;
	}

	public BytesWritable getValue() {
		return value;
	}

	public void setValue(BytesWritable value) {
		this.value = value;
	}
	
	public void setValue(byte[] value){
		this.value.set(value, 0, value.length);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		row_coor = in.readInt();
		col_coor = in.readInt();
		value.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(row_coor);
		out.writeInt(col_coor);
		value.write(out);
	}

	@Override
	public int compareTo(CoordinateValue coorValue) {
		if (this.row_coor < coorValue.getRow_coor()){
			return -1;
		}else if( this.row_coor > coorValue.getRow_coor() ){
			return 1;
		}else{
			if (this.col_coor < coorValue.getCol_coor()){
				return -1;
			}else if( this.col_coor > coorValue.getCol_coor() ){
				return 1;
			}
		}
		return 0;
	}

}
