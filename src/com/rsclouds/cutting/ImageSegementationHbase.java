package com.rsclouds.cutting;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.rsclouds.ParameterDefine;
import com.rsclouds.hadoop.io.FileInfo;
import com.rsclouds.hadoop.io.ImgInfo;
import com.rsclouds.hadoop.mapreduce.ImgBlockCuttingInputFormat;

public class ImageSegementationHbase extends Configured implements Tool {

	private static final Log LOG = LogFactory
			.getLog(ImageSegementationHbase.class);

	static class ImageSegMapper extends
			Mapper<ImgInfo, BytesWritable, Text, FileInfo> {

		private double[] adfGeoTransform;
		private Text path = new Text();
		private FileInfo fileInfo = new FileInfo();
		private HTable resourceTable;

		private StringBuilder rowkey;
		private StringBuilder rowName;
		private StringBuilder colName;
		private int rowkeyPrefixLen;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			
			Configuration conf = context.getConfiguration();
			resourceTable = new HTable(conf,
					ParameterDefine.RESOURCE_TABLENAME_DEFAULT);
			resourceTable.setAutoFlush(false, true);
			rowkey = new StringBuilder(conf.get(ParameterDefine.GTDATAPATH,
					ParameterDefine.GTDATAPATH_DEFAULT));
			rowkeyPrefixLen = rowkey.length();
			rowName = new StringBuilder();
			colName = new StringBuilder();

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
					rowName.replace(0, rowName.length(),
							Integer.toHexString(tilerow));
					int count = 8 - rowName.length();
					int i;
					for (i = 0; i < count; i++) {
						rowName.insert(0, "0");
					}
					rowName.insert(0, "R");

					colName.replace(0, rowName.length(),
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
					byteArrayOut.close();
					rowkey.replace(rowkeyPrefixLen, rowkey.length(), "/"
							+ rowName.toString());
					path.set(rowkey.toString());
					String filename = colName.toString() + ".png";
					fileInfo.setFilename(filename);
					fileInfo.setLength(data.length);
					rowkey.append("/" + filename);
					context.write(path, fileInfo);
					Put put = new Put(Bytes.toBytes(rowkey.toString()));
					put.add(ParameterDefine.RESOURCE_FAMILY,
							ParameterDefine.RESOURCE_LINKS, Bytes.toBytes("1"));
					put.add(ParameterDefine.RESOURCE_FAMILY,
							ParameterDefine.RESOURCE_DATA, data);
					resourceTable.put(put);
					heightRead += heightRang;

				}
				heightRang = 256;
				widthRead += widthRang;
				heightRead = 0;
			}

		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			super.cleanup(context);
			resourceTable.flushCommits();
			resourceTable.close();

			// HTable是HBase客户端与HBase服务端通讯的Java
			// API对象，客户端可以通过HTable对象与服务端进行CRUD操作（增删改查）
			Configuration conf = context.getConfiguration();
			HTable table = new HTable(conf,
					ParameterDefine.META_TABLENAME_DEFAULT);

			String url = conf.get(ParameterDefine.GTDATAPATH);
			
			Put put = new Put(Bytes.toBytes(url.replace("/_alllayers/L",
					"/_allayers//L")));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_DFS,
					Bytes.toBytes("0"));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_SIZE,
					Bytes.toBytes("-1"));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_URL,
					Bytes.toBytes(url));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_TIME,
					Bytes.toBytes("" + System.currentTimeMillis()));

			table.put(put);
			table.close();
		}
	}

	static class MetaTableInsertReducer extends
			TableReducer<Text, FileInfo, NullWritable> {
		private StringBuilder rowkey = new StringBuilder();
		private StringBuilder url = new StringBuilder();
		int indexof;
		int rowkeyPrefixLen;

		public void reduce(Text key, Iterable<FileInfo> values, Context context)
				throws IOException, InterruptedException {
			rowkey.replace(0, rowkey.length(), key.toString());
			rowkeyPrefixLen = rowkey.length();
			url.replace(0, url.length(), rowkey.toString());
			indexof = rowkey.lastIndexOf("/");
			rowkey.insert(indexof, "/");
			Put put = new Put(Bytes.toBytes(rowkey.toString()));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_DFS,
					Bytes.toBytes("0"));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_SIZE,
					Bytes.toBytes("-1"));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_URL,
					Bytes.toBytes(url.toString()));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_TIME,
					Bytes.toBytes("" + System.currentTimeMillis()));
			context.write(NullWritable.get(), put);
			rowkey.replace(0, rowkey.length(), key.toString());
			for (FileInfo val : values) {
				rowkey.replace(rowkeyPrefixLen, rowkey.length(), "//"
						+ val.getFilename().toString());
				url.replace(rowkeyPrefixLen, url.length(), "/"
						+ val.getFilename().toString());
				put = new Put(Bytes.toBytes(rowkey.toString()));
				put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_DFS,
						Bytes.toBytes("0"));
				put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_SIZE,
						Bytes.toBytes(String.valueOf(val.getLength())));
				put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_URL,
						Bytes.toBytes(url.toString()));
				put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_TIME,
						Bytes.toBytes("" + System.currentTimeMillis()));
				context.write(NullWritable.get(), put);
			}
		}
	}

	public int run(String[] args) throws Exception {

		int layersCount = Integer.parseInt(args[4]);
		StringBuilder layerName = new StringBuilder(
				Integer.toHexString(layersCount));
		// StringBuilder rowkey = new StringBuilder();
		if (layerName.length() == 1) {
			layerName.insert(0, "L0");
		} else {
			layerName.insert(0, "L");
		}
		// rowkey.append(args[5] + args[2] + "//" + layerName.toString());
		layerName.insert(0, args[5] + args[2] + "/");

		// hbase操作必备
		Configuration conf = HBaseConfiguration.create();

		conf.setStrings("mapred.child.java.opts", "-Xmx2048m");
		conf.setInt("mapreduce.map.memory.mb", 3072);

		conf.set(ParameterDefine.LOCALTEMPDIR, args[1]);
		conf.set(ParameterDefine.GTDATAPATH, layerName.toString());
		conf.set(ParameterDefine.IMGBLOCK_WIDTH, args[3]);
		conf.set(TableOutputFormat.OUTPUT_TABLE,
				ParameterDefine.META_TABLENAME_DEFAULT);

		Job job = Job.getInstance(conf);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setMapperClass(ImageSegMapper.class);
		job.setReducerClass(MetaTableInsertReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FileInfo.class);
		job.setInputFormatClass(ImgBlockCuttingInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setJarByClass(ImageSegementationHbase.class);

		int status = job.waitForCompletion(true) ? 0 : 1;

		while (status == 0 && layersCount > 0) {

			LOG.info("imageSegementation of the " + layersCount
					+ " is sucessed");
			args[0] = layerName.toString();
			LOG.info("the layer for read: " + args[0]);
			String temp = null;
			layersCount--;
			if (layersCount < 16) {
				temp = "L0" + Integer.toHexString(layersCount);
			} else {
				temp = "L" + Integer.toHexString(layersCount);
			}
			LOG.info("imageSegementation of the " + layersCount
					+ " processing........");
			status = new MergingResamplingHbase().run(args);
			layerName.replace(layerName.length() - 3, layerName.length(), temp);
		}
		if (0 == status) {
			LOG.info("imageSegementation of the " + layersCount
					+ " is sucessed");
		}
		return status;
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 6) {
			LOG.info("usage: <inputpath> <local temp dir> <gt-data ouput path(../../_alllayers)> <image block's mutiple  of the width(256)> <the total number of layers> <user>");
		} else {

			// 从ToolRunner的静态方法run()可以看到，其通过GenericOptionsParser
			// 来读取传递给run的job的conf和命令行参数args，处理hadoop的通用命令行参数，然后将剩下的job自己定义的参数(toolArgs
			// = parser.getRemainingArgs();)交给tool来处理,再由tool来运行自己的run方法。

			int status = ToolRunner.run(new ImageSegementationHbase(), args);
			System.exit(status);
		}
	}
}
