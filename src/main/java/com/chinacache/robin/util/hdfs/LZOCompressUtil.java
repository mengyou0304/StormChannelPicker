package com.chinacache.robin.util.hdfs;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class LZOCompressUtil {
	/**
	 * 
	 * @param from: hdfsURL
	 * @param to :hdfsURL
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public static void lzoCompress(String from,String to,Configuration conf) throws ClassNotFoundException, IOException{
		String codecClassname = "com.hadoop.compression.lzo.LzopCodec";
		Class<?> codeCLass = Class.forName(codecClassname);
		CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codeCLass, conf);
		Path inputpath =new Path(from);
		Path outputpath =new Path( to + ".lzo");
		FileSystem fs =inputpath.getFileSystem(conf);
		FileSystem fs2 =
				outputpath.getFileSystem(conf);
//		System.out.println(fs+"\n"+fs2);
		FSDataInputStream is = fs.open(inputpath);
		FSDataOutputStream os = fs2.create(outputpath);
		CompressionOutputStream out = codec.createOutputStream(os);
		IOUtils.copyBytes(is, out, 4096, true);
		
//		Path inputPath = new Path(inputpath);
//		FileStatus[] status = fs.listStatus(inputPath);
//		for(int i=0; i<status.length; i++){
//			Path outputPath = new Path(status[i].getPath() + ".lzo");
//			BufferedInputStream is = new BufferedInputStream(fs.open(status[i].getPath()));
//			FSDataOutputStream os = fs.create(outputPath);
//			CompressionOutputStream out = codec.createOutputStream(os);
//			IOUtils.copyBytes(is, out, 4096, true);
//		}
//		
	}
	public static void main(String[] args) {
		LZOCompressUtil util=new LZOCompressUtil();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Configuration conf=new Configuration();
		try {
			String hdfs="hdfs://223.202.46.136:8020/";
			String inputURL=hdfs+"tmp123/part-r-00000";
			String outputURL=hdfs+"tmp123/part-r-00000";
			
			util.lzoCompress(inputURL,outputURL,conf);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
}
