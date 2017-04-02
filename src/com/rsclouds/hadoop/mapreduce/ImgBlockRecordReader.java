package com.rsclouds.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.rsclouds.hadoop.io.ImgInfo;

public class ImgBlockRecordReader extends RecordReader<ImgInfo, BytesWritable>{
	private ImgBlockSplit filesplit;
	private boolean processed = false;
	private Configuration conf;
	private BytesWritable value = new BytesWritable();
	private ImgInfo key = new ImgInfo();
	
	
	@Override
	public void close() throws IOException {}

	@Override
	public ImgInfo getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 0 : 1;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {	
		filesplit = (ImgBlockSplit)split;
		conf = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed){
			byte[] contents = new byte[(int)filesplit.getLength()];
			Path file = filesplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try{
				in = fs.open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
			}finally{
				IOUtils.closeStream(in);
			}
			key.setPath(new Text(file.toString()));
			key.setAdfGeoTransform(filesplit.getAdfGeoTransform());
			value.set(contents, 0, contents.length);
			processed = true;
			fs.close();
			in.close();
			return true;
		}
		return false;
	}
	
}
