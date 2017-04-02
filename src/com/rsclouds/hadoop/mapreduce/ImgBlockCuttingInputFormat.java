package com.rsclouds.hadoop.mapreduce;

import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconst;
import org.gdal.gdalconst.gdalconstConstants;

import com.rsclouds.ParameterDefine;
import com.rsclouds.hadoop.io.ImgInfo;

public class ImgBlockCuttingInputFormat extends
		FileInputFormat<ImgInfo, BytesWritable> {

	private static final Log LOG = LogFactory
			.getLog(ImgBlockCuttingInputFormat.class);
	private int count = 0;

	/**
	 * Generate the list of files and make them into FileSplits.
	 */
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();
		List<FileStatus> files = listStatus(job);
		for (FileStatus file : files) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(job.getConfiguration());
			splits.addAll(pyramidCutting(file.getPath(),
					job.getConfiguration(), fs));
			fs.close();
		}
		// Save the number of input files in the job-conf
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

		LOG.debug("Total # of splits: " + splits.size());
		return splits;

	}

	public List<ImgBlockSplit> pyramidCutting(Path filePath,
			Configuration conf, FileSystem fs) {
		List<ImgBlockSplit> pathList = new ArrayList<ImgBlockSplit>();
		try {
			// copy the file from hdfs to local
			DataInputStream in = fs.open(filePath);
			String localTempfilePrefix = conf.get(ParameterDefine.LOCALTEMPDIR,
					ParameterDefine.LOCALTEMPDIR_DEFAULT);
			if (localTempfilePrefix.lastIndexOf('/') != localTempfilePrefix
					.length() - 1) {
				localTempfilePrefix += "/";
			}
			int localTempfilePrefixLen = localTempfilePrefix.length();
			StringBuilder localfile = new StringBuilder(localTempfilePrefix
					+ filePath.getName());
			FileOutputStream out = new FileOutputStream(localfile.toString());
			byte[] byteRead = new byte[1024];
			while (in.read(byteRead, 0, 1024) != -1) {
				out.write(byteRead, 0, 1024);
			}
			in.close();
			out.close();

			// pyramidCutting
			gdal.AllRegister();
			gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "YES");
			gdal.SetConfigOption("SHAPE_ENCODING", "");
			Dataset hDataset = gdal.Open(localfile.toString(),
					gdalconstConstants.GA_ReadOnly);
			if (hDataset == null) {
				LOG.info("GDALOpen failed - " + gdal.GetLastErrorNo());
				LOG.info(gdal.GetLastErrorMsg());
				System.exit(1);
			}

			int rasterCount = hDataset.GetRasterCount(); // ������
			int width = hDataset.GetRasterXSize(); // ���
			int height = hDataset.GetRasterYSize(); // �߶�
			double[] adfGeoTransform = hDataset.GetGeoTransform();
			double xCoordinate = adfGeoTransform[0];
			double yCoordinate = adfGeoTransform[3];

			Driver poDriver = gdal.GetDriverByName("PNG");
			Driver drimen = gdal.GetDriverByName("MEM");
			String tempdirHDFS = conf.get(ParameterDefine.HDFSTEMPDIR,
					ParameterDefine.HDFSTEMPDIR_DEFAULT);
			Path hdfsdstPath = new Path(tempdirHDFS);
			Dataset menDataset = null;

			int widthRead = 0;
			int heightRead = 0;
			int multiple = conf.getInt(ParameterDefine.IMGBLOCK_WIDTH,
					ParameterDefine.IMGBLOCK_WIDTH_DEFAULT);
			int maxMutiple = multiple + (multiple / 2 < 50 ? multiple / 2 : 50);
			int widthrang = 256 * multiple;
			int heightrang = 256 * multiple;
			if (width / 256 < maxMutiple)
				widthrang = width;
			if (height / 256 < maxMutiple)
				heightrang = height;
			int heightrangInit = heightrang;
			int num = widthrang * heightrang;
			byte[] b = new byte[num];
			boolean processed = false;
			if (1 == rasterCount) {
				Band band = hDataset.GetRasterBand(1);
				int colorInterp = band.GetColorInterpretation();
				if (2 == colorInterp) {// colorInterp = Palette
					processed = true;
					byte[] bandbyte = new byte[num];
					for (; widthRead < width;) {
						if (width - widthRead < widthrang) {
							widthrang = width - widthRead;
						}
						adfGeoTransform[0] = xCoordinate + widthRead
								* adfGeoTransform[1] + heightRead
								* adfGeoTransform[2];
						for (heightRead = 0; heightRead < height;) {
							if (height - heightRead < heightrang) {
								heightrang = height - heightRead;
							}
							adfGeoTransform[3] = yCoordinate + widthRead
									* adfGeoTransform[4] + heightRead
									* adfGeoTransform[5];
							band.ReadRaster(widthRead, heightRead, widthrang,
									heightrang, gdalconst.GDT_Byte, b);
							localfile.replace(localTempfilePrefixLen,
									localfile.length(), count + ".png");
							menDataset = drimen.Create(localfile.toString(),
									widthrang, heightrang, 3,
									gdalconst.GDT_Byte);
							int arraySize = widthrang * heightrang;
							for (int j = 0; j < arraySize; j++) {
								bandbyte[j] = (byte) band.GetColorTable()
										.GetColorEntry((b[j] & 0xff)).getRed();
							}
							menDataset.GetRasterBand(1).WriteRaster(0, 0,
									widthrang, heightrang, gdalconst.GDT_Byte,
									bandbyte);

							for (int j = 0; j < arraySize; j++) {
								bandbyte[j] = (byte) band.GetColorTable()
										.GetColorEntry((b[j] & 0xff))
										.getGreen();
							}
							menDataset.GetRasterBand(2).WriteRaster(0, 0,
									widthrang, heightrang, gdalconst.GDT_Byte,
									bandbyte);

							for (int j = 0; j < arraySize; j++) {
								bandbyte[j] = (byte) band.GetColorTable()
										.GetColorEntry((b[j] & 0xff)).getBlue();
							}
							menDataset.GetRasterBand(3).WriteRaster(0, 0,
									widthrang, heightrang, gdalconst.GDT_Byte,
									bandbyte);

							Dataset poDataset = poDriver.CreateCopy(
									localfile.toString(), menDataset);
							poDataset.FlushCache();
							poDataset.delete();
							menDataset.delete();
							heightRead += heightrang;

							// make split
							fs.copyFromLocalFile(false,
									new Path(localfile.toString()), hdfsdstPath);
							Path hdfscopyFile = new Path(hdfsdstPath.toString()
									+ "/" + count + ".png");
							FileStatus fstatus = fs.getFileStatus(hdfscopyFile);
							long hdfsfilelength = fstatus.getLen();
							BlockLocation[] blkLocations = fs
									.getFileBlockLocations(hdfscopyFile, 0,
											hdfsfilelength);

							count++;
							pathList.add(new ImgBlockSplit(hdfscopyFile, 0,
									hdfsfilelength, blkLocations[0].getHosts(),
									adfGeoTransform));
						}
						heightrang = heightrangInit;
						widthRead += widthrang;
					}
				}
			}
			if (!processed) {
				for (widthRead = 0; widthRead < width;) {
					if (width - widthRead < widthrang) {
						widthrang = width - widthRead;
					}
					adfGeoTransform[0] = xCoordinate + widthRead
							* adfGeoTransform[1] + heightRead
							* adfGeoTransform[2];
					for (heightRead = 0; heightRead < height;) {
						if (height - heightRead < heightrang) {
							heightrang = height - heightRead;
						}
						adfGeoTransform[3] = yCoordinate + widthRead
								* adfGeoTransform[4] + heightRead
								* adfGeoTransform[5];
						for (int i = 1; i <= rasterCount; i++) {
							localfile.replace(localTempfilePrefixLen,
									localfile.length(), count + ".png");
							menDataset = drimen.Create(localfile.toString(),
									widthrang, heightrang, rasterCount,
									gdalconst.GDT_Byte);
							if (menDataset == null) {
								System.out.println("null");
							}
							Band band_i = hDataset.GetRasterBand(i);
							band_i.ReadRaster(widthRead, heightRead, widthrang,
									heightrang, gdalconst.GDT_Byte, b);
							menDataset.GetRasterBand(i).WriteRaster(0, 0,
									widthrang, heightrang, gdalconst.GDT_Byte,
									b);
						}
						Dataset poDataset = poDriver.CreateCopy(
								localfile.toString(), menDataset);
						poDataset.FlushCache();
						poDataset.delete();
						menDataset.delete();
						heightRead += heightrang;

						// make split
						fs.copyFromLocalFile(false,
								new Path(localfile.toString()), hdfsdstPath);
						Path hdfscopyFile = new Path(hdfsdstPath.toString()
								+ "/" + count + ".png");
						FileStatus fstatus = fs.getFileStatus(hdfscopyFile);
						long hdfsfilelength = fstatus.getLen();
						BlockLocation[] blkLocations = fs
								.getFileBlockLocations(hdfscopyFile, 0,
										hdfsfilelength);
						pathList.add(new ImgBlockSplit(hdfscopyFile, 0,
								hdfsfilelength, blkLocations[0].getHosts(),
								adfGeoTransform));
						count++;
					}
					heightrang = heightrangInit;
					widthRead += widthrang;
				}
			}
			hDataset.delete();
			gdal.GDALDestroyDriverManager();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return pathList;
	}

	@Override
	public RecordReader<ImgInfo, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new ImgBlockRecordReader();
	}

}
