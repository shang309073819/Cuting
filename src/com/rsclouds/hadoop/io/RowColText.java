package com.rsclouds.hadoop.io;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class RowColText implements WritableComparable<RowColText> {

	private int col;
	private int row;

	public int getRow() {
		return row;
	}

	public void setRow(int row) {
		this.row = row;
	}

	public int getCol() {
		return col;
	}

	public void setCol(int col) {
		this.col = col;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		row = in.readInt();
		col = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(row);
		out.writeInt(col);
	}

	@Override
	public String toString() {
		return row + "_" + col;
	}

	public boolean equals(Object o) {
		if (!(o instanceof RowColText))
			return false;
		RowColText other = (RowColText) o;
		if (row == other.getRow()) {
			if (col == other.getCol()) {
				return true;
			}
		}
		return false;
	}

	public int hashCode() {
		return row * 163 + col;
	}

	@Override
	public int compareTo(RowColText rc) {
		if (this.row < rc.getRow()){
			return -1;
		}else if( this.row > rc.getRow() ){
			return 1;
		}else{
			if (this.col < rc.getCol()){
				return -1;
			}else if( this.col > rc.getCol() ){
				return 1;
			}
		}
		return 0;
	}

}
