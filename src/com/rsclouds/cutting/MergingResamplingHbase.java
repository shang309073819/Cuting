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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;

import com.rsclouds.ParameterDefine;
import com.rsclouds.hadoop.io.CoordinateValue;
import com.rsclouds.hadoop.io.RowColText;

public class MergingResamplingHbase extends Configured implements Tool {
	private static final Log LOG = LogFactory
			.getLog(MergingResamplingHbase.class);
	static class ReadHbaseMapper extends
			TableMapper<RowColText, CoordinateValue> {

		private RowColText rowcol = new RowColText();
		private CoordinateValue coorValue = new CoordinateValue();

		public void map(ImmutableBytesWritable key, Result values,
				Context context) throws IOException, InterruptedException {
			String rowkey = new String(key.get());
			String[] splits = rowkey.split("/");
			int length = splits.length;
			int rowInt = Integer.valueOf(splits[length - 2].substring(1), 16);
			String str = splits[length - 1];
			int indexof = str.lastIndexOf('.');
			int colInt = Integer.valueOf(str.substring(1, indexof), 16);

			rowcol.setRow(rowInt / 2);
			rowcol.setCol(colInt / 2);
			coorValue.setRow_coor(rowInt % 2);
			coorValue.setCol_coor(colInt % 2);
			coorValue.setValue(values.getValue(ParameterDefine.RESOURCE_FAMILY,
					ParameterDefine.RESOURCE_DATA));
			context.write(rowcol, coorValue);
		}
	}

	static class ResampleReducer extends
			TableReducer<RowColText, CoordinateValue, NullWritable> {
		private int scale; // the scaling of picture
		private HTable resourceTable;
		private StringBuilder rowkey;
		private StringBuilder url;
		private StringBuilder rowName;
		private StringBuilder colName;
		private int rowkeyPrefixLen;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			scale = conf.getInt("scale", 2);
			resourceTable = new HTable(conf,
					ParameterDefine.RESOURCE_TABLENAME_DEFAULT);
			resourceTable.setAutoFlush(false, true);
			rowkey = new StringBuilder(conf.get(ParameterDefine.GTDATAPATH,
					ParameterDefine.GTDATAPATH_DEFAULT));
			rowkeyPrefixLen = rowkey.length();
			rowName = new StringBuilder();
			colName = new StringBuilder();
			url = new StringBuilder(rowkey.toString());
		}

		public void reduce(RowColText key, Iterable<CoordinateValue> values,
				Context context) throws IOException, InterruptedException {
			int width_all = 0;
			int height_all = 0;
//			int width_0_0 = 0;
//			int height_0_0 = 0;
//			int width_0_1 = 0;
//			int height_0_1 = 0;
//			int width_1_0 = 0;
//			int height_1_0 = 0;
//			int width_1_1 = 0;
//			int height_1_1 = 0;
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
//				if (val.getRow_coor() == 0) {
//					if (val.getCol_coor() == 0) {
//						width_0_0 = imageList.get(i).getIconWidth();
//						height_0_0 = imageList.get(i).getIconHeight();
//					} else {
//						width_0_1 = imageList.get(i).getIconWidth();
//						height_0_1 = imageList.get(i).getIconHeight();
//					}
//				} else if (val.getRow_coor() == 1) {
//					if (val.getCol_coor() == 0) {
//						width_1_0 = imageList.get(i).getIconWidth();
//						height_1_0 = imageList.get(i).getIconHeight();
//					} else {
//						width_1_1 = imageList.get(i).getIconWidth();
//						height_1_1 = imageList.get(i).getIconHeight();
//					}
//				}
				i++;
			}
			length = coorList.size();
//			if ( length != 1){
//				width_all = (width_0_0 + width_0_1 + width_1_0 + width_1_1) / 2;
//				height_all = (height_0_0 + height_0_1 + height_1_0 + height_1_1) / 2;
//			}else{
//				width_all = width_0_0 + width_0_1 + width_1_0 + width_1_1;
//				height_all = height_0_0 + height_0_1 + height_1_0 + height_1_1;
//			}
			width_all = ParameterDefine.WIDTH_DEFAULT * 2;
			height_all = ParameterDefine.HEIGHT_DEFAULT * 2;

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
						gs.drawImage(imageList.get(i).getImage(), ParameterDefine.WIDTH_DEFAULT, 0,
								imageList.get(i).getImageObserver());
					}
				} else if (val.getRow_coor() == 1) {
					if (val.getCol_coor() == 0) {
						gs.drawImage(imageList.get(i).getImage(), 0,
								ParameterDefine.HEIGHT_DEFAULT, imageList.get(i).getImageObserver());
					} else if (val.getCol_coor() == 1) {
						gs.drawImage(imageList.get(i).getImage(), ParameterDefine.WIDTH_DEFAULT,
								ParameterDefine.HEIGHT_DEFAULT, imageList.get(i).getImageObserver());
					}
				}
			}
			gs.dispose();

			// resample the picture
			width_all = width_all / scale;
			height_all = height_all / scale;
			Image reImage = img.getScaledInstance(width_all, height_all,
					Image.SCALE_AREA_AVERAGING);
			BufferedImage tag = new BufferedImage(ParameterDefine.WIDTH_DEFAULT, ParameterDefine.HEIGHT_DEFAULT,
					BufferedImage.TYPE_INT_ARGB_PRE);
			Graphics g = tag.getGraphics();
			g.drawImage(reImage, 0, 0, null);
			g.dispose();

			// output the file
			ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
			ImageIO.write(tag, "png", byteArrayOut);
			byte[] data = byteArrayOut.toByteArray();
			byteArrayOut.close();

			rowName.replace(0, rowName.length(),
					Integer.toHexString(key.getRow()));
			int count = 8 - rowName.length();
			for (i = 0; i < count; i++) {
				rowName.insert(0, "0");
			}
			rowName.insert(0, "R");
			colName.replace(0, rowName.length(),
					Integer.toHexString(key.getCol()));
			count = 8 - colName.length();
			for (i = 0; i < count; i++) {
				colName.insert(0, "0");
			}
			colName.insert(0, "C");
			rowkey.replace(rowkeyPrefixLen, rowkey.length(),
					"//" + rowName.toString());
			url.replace(rowkeyPrefixLen, url.length(), "/" + rowName.toString());
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

			put = new Put(Bytes.toBytes(url.toString() + "//"
					+ colName.toString() + ".png"));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_DFS,
					Bytes.toBytes("0"));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_SIZE,
					Bytes.toBytes(String.valueOf(data.length)));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_URL,
					Bytes.toBytes(url.toString() + "/" + colName.toString() + ".png"));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_TIME,
					Bytes.toBytes("" + System.currentTimeMillis()));
			context.write(NullWritable.get(), put);

			put = new Put(Bytes.toBytes(url.toString() + "/" + colName.toString() + ".png"));
			put.add(ParameterDefine.RESOURCE_FAMILY,
					ParameterDefine.RESOURCE_LINKS, Bytes.toBytes("1"));
			put.add(ParameterDefine.RESOURCE_FAMILY,
					ParameterDefine.RESOURCE_DATA, data);
			resourceTable.put(put);

			coorList.clear();
		}
		
		public void cleanup(Context context)throws IOException, InterruptedException{
			super.cleanup(context);
			resourceTable.flushCommits();
			resourceTable.close();
			Configuration conf = context.getConfiguration();
			HTable table = new HTable(conf,
					ParameterDefine.META_TABLENAME_DEFAULT);
			String url = conf.get(ParameterDefine.GTDATAPATH);
			Put put = new Put(Bytes.toBytes(url.replace("/_alllayers/L", "/_allayers//L")));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_DFS,
					Bytes.toBytes("0"));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_SIZE,
					Bytes.toBytes("-1"));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_URL,
					Bytes.toBytes(url));
			put.add(ParameterDefine.META_FAMILY, ParameterDefine.META_TIME,
					Bytes.toBytes("" + System.currentTimeMillis()));
			table.put(put);
		}
	}
	
	public int run(String[] args)throws Exception{
		
		int layerNumber = Integer.valueOf(args[0].substring(args[0].length() -2), 16);
		layerNumber --;
		String layerName = Integer.toHexString(layerNumber);
		if ( layerNumber < 16){
			layerName = args[0].substring(0, args[0].length() -2 ) + "0" + layerName;
		}else{
			layerName = args[0].substring(0, args[0].length() -2 ) + layerName;
		}
		LOG.info("MergingResamplingHbase output layers is: " + layerName);
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.nameserver.address", "7.2.168.192.in-addr.arpa");
		conf.set(ParameterDefine.GTDATAPATH, layerName);
//		conf.set(TableInputFormat.INPUT_TABLE, ParameterDefine.RESOURCE_TABLENAME_DEFAULT);
//		conf.set(TableInputFormat.SCAN_ROW_START, args[0] + "/R0000000/");
//		conf.set(TableInputFormat.SCAN_ROW_STOP, args[0] + "/Rffffffff/");
//		conf.set(TableOutputFormat.OUTPUT_TABLE, ParameterDefine.META_TABLENAME_DEFAULT);
		Job job = Job.getInstance(conf, "MergingResamplingHbase");
		job.setJarByClass(MergingResamplingHbase.class);
		job.setNumReduceTasks(9);
				
		Scan scan = new Scan();
		scan.addFamily(ParameterDefine.RESOURCE_FAMILY);
		Filter filter = new RowFilter(CompareOp.EQUAL,
				new BinaryPrefixComparator(args[0].getBytes()));
		scan.setFilter(filter);
		scan.setBatch(400);
		scan.setCacheBlocks(false);
		scan.setStartRow(Bytes.toBytes(args[0] + "/R0000000/"));
		scan.setStopRow(Bytes.toBytes(args[0] + "/Rfffffff/"));
		
//		job.setOutputFormatClass(TableOutputFormat.class);
//		job.setInputFormatClass(TableInputFormat.class);
//		job.setMapperClass(ReadHbaseMapper.class);
//		job.setReducerClass(ResampleReducer.class);
//		job.setMapOutputKeyClass(RowColText.class);
//		job.setMapOutputValueClass(CoordinateValue.class);
		TableMapReduceUtil.initTableMapperJob(
				ParameterDefine.RESOURCE_TABLENAME_DEFAULT, scan,
				ReadHbaseMapper.class, RowColText.class,
				CoordinateValue.class, job, true);
		TableMapReduceUtil.initTableReducerJob(
				ParameterDefine.META_TABLENAME_DEFAULT,
				ResampleReducer.class, job);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
//	public static void main(String[] args)throws Exception{
//		args = new String[1];
//		args[0] = "res_test/nanlin/ImgSegementation/word_test/Layers/_alllayers/L07";
//		int status = ToolRunner.run(new MergingResamplingHbase(), args);
//		System.exit(status);
//	}
}
