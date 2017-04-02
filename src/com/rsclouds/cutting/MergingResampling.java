package com.rsclouds.cutting;

import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.rsclouds.hadoop.io.CoordinateValue;
import com.rsclouds.hadoop.io.RowColText;
import com.rsclouds.hadoop.mapreduce.WholeFileInputFormat;

public class MergingResampling extends Configured implements Tool {

	static class Map extends Mapper<Text, BytesWritable, RowColText, CoordinateValue> {

		private RowColText rowcol = new RowColText();
		private CoordinateValue coorValue = new CoordinateValue();

		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			String path = key.toString();
			String[] splits = path.split("/");
			int length = splits.length;
			int rowInt = Integer.valueOf(splits[length - 2].substring(1), 16);
			String str = splits[length - 1];
			int indexof = str.lastIndexOf('.');
			int colInt = Integer.valueOf(str.substring(1, indexof), 16);

			rowcol.setRow(rowInt/2);
			rowcol.setCol(colInt/2);
			coorValue.setRow_coor(rowInt % 2);
			coorValue.setCol_coor(colInt % 2);
			coorValue.setValue(value);
			context.write(rowcol, coorValue);

		}

	}

	static class ReduceMerge extends
			Reducer<RowColText, CoordinateValue, Text, BytesWritable> {

		private int scale; // the scaling of picture
		private FileSystem fs;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			
			Configuration conf = context.getConfiguration();		
			fs = FileSystem.get(conf);
			scale = conf.getInt("scale", 2);
		}

		public void reduce(RowColText key, Iterable<CoordinateValue> values,
				Context context) throws IOException, InterruptedException {
			int width_all = 0;
			int height_all = 0;
			int width_0_0 = 0;
			int height_0_0 = 0;
			int width_0_1 = 0;
			int height_0_1 = 0;
			int width_1_0 = 0;
			int height_1_0 = 0;
			int width_1_1 = 0;
			int height_1_1 = 0;
			int length;
			int i = 0;
			List<CoordinateValue> coorList = new ArrayList<CoordinateValue>();
			List<ImageIcon> imageList = new ArrayList<ImageIcon>();
			
			for (CoordinateValue val : values) {
				CoordinateValue tmp = new CoordinateValue();
				tmp.setCol_coor(val.getCol_coor());
				tmp.setRow_coor(val.getRow_coor());
				tmp.setValue(val.getValue());
				coorList.add(tmp);
				imageList.add(new ImageIcon(val.getValue().getBytes()));
				if (val.getRow_coor() == 0) {
					if (val.getCol_coor() == 0) {
						width_0_0 = imageList.get(i).getIconWidth();
						height_0_0 = imageList.get(i).getIconHeight();

					} else {
						width_0_1 = imageList.get(i).getIconWidth();
						height_0_1 = imageList.get(i).getIconHeight();
					}
				} else if (val.getRow_coor() == 1) {
					if (val.getCol_coor() == 0) {
						width_1_0 = imageList.get(i).getIconWidth();
						height_1_0 = imageList.get(i).getIconHeight();

					} else {
						width_1_1 = imageList.get(i).getIconWidth();
						height_1_1 = imageList.get(i).getIconHeight();
					}
				}
				i ++;
			}

			width_all = (width_0_0 + width_0_1 + width_1_0 + width_1_1) / 2;
			height_all = (height_0_0 + height_0_1 + height_1_0 + height_1_1) / 2;
			length = coorList.size();


			// merge the picture
			BufferedImage img = new BufferedImage(width_all, height_all,
					BufferedImage.TYPE_INT_ARGB_PRE);
			Graphics2D gs = (Graphics2D) img.getGraphics();
			for (i = 0; i < length; i++) {
				CoordinateValue val = coorList.get(i);
				if (val.getRow_coor() == 0) {
					if (val.getCol_coor() == 0) {
						gs.drawImage(imageList.get(i).getImage(), 0, 0,
								imageList.get(i).getImageObserver());
					} else if (val.getCol_coor() == 1) {
						gs.drawImage(imageList.get(i).getImage(), width_0_0, 0,
								imageList.get(i).getImageObserver());
					}
				} else if (val.getRow_coor() == 1) {
					if (val.getCol_coor() == 0) {
						gs.drawImage(imageList.get(i).getImage(), 0, height_0_0,
								imageList.get(i).getImageObserver());
					} else if (val.getCol_coor() == 1) {
						gs.drawImage(imageList.get(i).getImage(), width_1_0, height_0_1,
								imageList.get(i).getImageObserver());
					}
				}
			}
			gs.dispose();

			// resample the picture
			width_all = width_all / scale;
			height_all = height_all / scale;
			Image reImage = img.getScaledInstance(width_all, height_all,
					Image.SCALE_AREA_AVERAGING);
			BufferedImage tag = new BufferedImage(width_all, height_all,
					BufferedImage.TYPE_INT_ARGB_PRE);
			Graphics g = tag.getGraphics();
			g.drawImage(reImage, 0, 0, null);
			g.dispose();
			
			//output the file 
			ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
	        ImageIO.write(img, "png", byteArrayOut);
	        byte[] data = byteArrayOut.toByteArray();
	        StringBuilder rowName = new StringBuilder(Integer.toHexString(key.getRow()));
	        int count = 8 - rowName.length();
	        for (i = 0; i < count; i ++){
	        	rowName.insert(0, "0");
	        }
	        rowName.insert(0, "R");
	        StringBuilder colName = new StringBuilder(Integer.toHexString(key.getCol()));
	        count = 8 - colName.length();
	        for (i = 0; i < count; i ++){
	        	colName.insert(0, "0");
	        }
	        colName.insert(0, "C");
			FSDataOutputStream out = fs.create(new Path(
					"hdfs://192.168.2.3:8020/nanlin_root/output/"+rowName.toString()+
							"/" + colName.toString() + ".png"));
			out.write(data);
			out.flush();
			out.close();
			byteArrayOut.close();
			coorList.clear();

			// value.set(data, 0, data.length);
			// context.write(arg0, value);

		}

		protected void clearnup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
			fs.close();
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf() == null ? new Configuration()
				: getConf();
		conf.setInt("scale", 2);

		Job job = Job.getInstance(conf, "merge and resampling");
		job.setJarByClass(MergingResampling.class);
		Path[] paths = new Path[2];
		paths[0] = new Path("hdfs://192.168.2.3:8020/nanlin_root/R000017cc");
		paths[1] = new Path("hdfs://192.168.2.3:8020/nanlin_root/R000017cd");
		FileInputFormat.setInputPaths(job, paths);
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://192.168.2.3:8020/nanlin_root/output_merge5"));

		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(RowColText.class);
		job.setMapOutputValueClass(CoordinateValue.class);
		job.setReducerClass(ReduceMerge.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new MergingResampling(), args);
		System.exit(status);
	}
}
