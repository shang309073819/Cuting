package com.rsclouds.cutting;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
//import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.rsclouds.ParameterDefine;
import com.rsclouds.hadoop.io.ImgInfo;
import com.rsclouds.hadoop.mapreduce.ImgBlockCuttingInputFormat;

public class ImageSegmentation extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(ImageSegmentation.class);

	// Mapper
	static class ImageSegMapper extends
			Mapper<ImgInfo, BytesWritable, Text, BytesWritable> {

		private FileSystem fs;
		private double[] adfGeoTransform;
		private Text path = new Text();
		private BytesWritable fileData = new BytesWritable();

		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			fs = FileSystem.get(conf);
			System.out.println();
		}

		public void map(ImgInfo key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			ImageIcon imageIcon = new ImageIcon(value.getBytes());
			int width = imageIcon.getIconWidth();
			int height = imageIcon.getIconHeight();
			BufferedImage img = new BufferedImage(width, height,
					BufferedImage.TYPE_INT_ARGB_PRE);
			Graphics2D gs = (Graphics2D) img.getGraphics();
			gs.drawImage(imageIcon.getImage(), 0, 0,
					imageIcon.getImageObserver());

			adfGeoTransform = key.getAdfGeoTransform().clone();
			int widthRead = 0;
			int heightRead = 0;
			int widthRang = 256;
			int heightRang = 256;
			double xCoordinate;
			;
			double yCoordinate;
			int tilerow;
			int tilecol;
			for (; widthRead < width;) {
				if (width - widthRead < widthRang) {
					widthRang = width - widthRead;
				}
				for (heightRead = 0; heightRead < height;) {
					if (height - heightRead < heightRang) {
						heightRang = height - heightRead;
					}
					xCoordinate = adfGeoTransform[0] + widthRead
							* adfGeoTransform[1] + heightRead
							* adfGeoTransform[2];
					yCoordinate = adfGeoTransform[3] + widthRead
							* adfGeoTransform[4] + heightRead
							* adfGeoTransform[5];
					tilerow = (int) Math.floor((90 - yCoordinate) / 256
							/ Math.abs(adfGeoTransform[5]));
					tilecol = (int) Math.floor((180 + xCoordinate) / 256
							/ adfGeoTransform[1]);
					StringBuilder rowName = new StringBuilder(
							Integer.toHexString(tilerow));
					int count = 8 - rowName.length();
					int i;
					for (i = 0; i < count; i++) {
						rowName.insert(0, "0");
					}
					rowName.insert(0, "R");
					StringBuilder colName = new StringBuilder(
							Integer.toHexString(tilecol));
					count = 8 - colName.length();
					for (i = 0; i < count; i++) {
						colName.insert(0, "0");
					}
					colName.insert(0, "C");
					int[] rgbArray = new int[widthRang * heightRang];
					rgbArray = img.getRGB(widthRead, heightRead, widthRang,
							heightRang, rgbArray, 0, widthRang);
					BufferedImage imgBlock = new BufferedImage(256, 256,
							BufferedImage.TYPE_INT_ARGB_PRE);
					imgBlock.setRGB(0, 0, widthRang, heightRang, rgbArray, 0,
							widthRang);
					ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
					ImageIO.write(imgBlock, "png", byteArrayOut);
					byte[] data = byteArrayOut.toByteArray();
					String hdfsFilename = "hdfs://node03.rsclouds.cn:8020/nanlin_root/imagSegmentation/"
							+ rowName.toString()
							+ "/"
							+ colName.toString()
							+ ".png";

					// String hdfsFilename = "D://nanlin//temp//" +
					// rowName.toString() + "//" + colName.toString() + ".png";
					if (!fs.createNewFile(new Path(hdfsFilename))) {
						System.out.println("create");
					}
					FSDataOutputStream out = fs.create(new Path(hdfsFilename));

					out.write(data, 0, data.length);
					out.flush();
					out.close();
					byteArrayOut.close();
					heightRead += heightRang;
					path.set(hdfsFilename);
					fileData.set(data, 0, data.length);
					context.write(path, fileData);
				}
				heightRang = 256;
				widthRead += widthRang;
			}

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
		//localTempfilePrefix
		conf.set(ParameterDefine.LOCALTEMPDIR, args[1]);
		//hdfstempdir
		conf.set(ParameterDefine.HDFSTEMPDIR, args[2]);
		//imgblock_width
		conf.set(ParameterDefine.IMGBLOCK_WIDTH, args[3]);

		Job job = Job.getInstance(conf);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2] + "output"));
		job.setMapperClass(ImageSegMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(ImgBlockCuttingInputFormat.class);
		job.setJarByClass(ImageSegmentation.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			LOG.info("usage: <inputpath> <local temp dir> <output path> <image block's mutiple  of the width(256)>");
		} else {

			// 从ToolRunner的静态方法run()可以看到，其通过GenericOptionsParser
			// 来读取传递给run的job的conf和命令行参数args，处理hadoop的通用命令行参数，然后将剩下的job自己定义的参数(toolArgs
			// = parser.getRemainingArgs();)交给tool来处理,再由tool来运行自己的run方法。

			int status = ToolRunner.run(new ImageSegmentation(), args);
			System.exit(status);
		}
	}
}
