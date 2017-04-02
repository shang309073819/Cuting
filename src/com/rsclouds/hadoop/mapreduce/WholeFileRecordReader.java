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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends RecordReader<Text, BytesWritable>{
	private FileSplit filesplit;
	private boolean processed = false;
	private Configuration conf;
	private BytesWritable value = new BytesWritable();
	private Text key = new Text();
	
	@Override
	public void close() throws IOException {}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 0 : 1;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {	
		filesplit = (FileSplit)split;
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
			key.set(file.toString());
			value.set(contents, 0, contents.length);
			processed = true;
			return true;
		}
		return false;
	}

}
