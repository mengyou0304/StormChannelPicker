package com.chinacache.robin.util.hdfs;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressUploader {
	private static final Logger logger = LoggerFactory
			.getLogger(CompressUploader.class);

	static Configuration conf = new Configuration();
	
	static {
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("hadoop.user", "hadoop");
		conf.set("dfs.replication", "3");
	}

	public static void putLocalToHDFS(boolean delSrc, Path src, Path dest) {		
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		try {
			String srcGzipFile = compress(src, true);
			Path srcGzipPath = new Path(srcGzipFile);
			FileSystem destFs = dest.getFileSystem(conf);
			destFs.mkdirs(dest);
			destFs.copyFromLocalFile(true, srcGzipPath, dest);
			logger.info("Transfer " + dest + " to " + src);
			if (delSrc) {
				FileSystem srcFs = FileSystem.getLocal(conf);
				srcFs.delete(src, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String compress(Path srcPath, boolean overwrite)
			throws IOException {
		InputStream in = null;
		GZIPOutputStream out = null;
		try {
			String gzFilePath = srcPath.toString() + ".gz";

			in = new FileInputStream(srcPath.toString());			
			out = new GZIPOutputStream(
					new BufferedOutputStream(new FileOutputStream(gzFilePath)));

			IOUtils.copyBytes(in, out, conf, true);

//			out.finish();
			out.flush();
			out.close();
			in.close();			
			logger.info("Compress file {} to {}", srcPath, gzFilePath);
			return gzFilePath;
		} catch (IOException e) {			
			IOUtils.closeStream(out);
			IOUtils.closeStream(in);
			throw e;
		}
	}

	public static void main(String[] args) {

		if(args.length < 2){
			System.out.println("Usage : localFile hdfsFilePath");
			return;
		}
		
//		String src = "./lib";
//		String dest = "./compress/";
//		putLocalToHDFS(false, new Path(src), new Path(dest));
		
		putLocalToHDFS(false, new Path(args[0]), new Path(args[1]));
	}
}
