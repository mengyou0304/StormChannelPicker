package com.chinacache.robin.util.hdfs;

import backtype.storm.Config;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

public class HDFSUtility implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory
		.getLogger(HDFSUtility.class);

	static Configuration conf = new Configuration();

	static {
		conf.set("fs.hdfs.impl",
			org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
			org.apache.hadoop.fs.LocalFileSystem.class.getName());
	}

	public static Config getConfig(HashMap<String, String> map) {
		Config config = new Config();
		Set<String> set = map.keySet();
		for (String key : set) {
			String value = map.get(key);
			if (StringUtils.isNumeric(value)) {
				try {
					config.put(key, Integer.valueOf(value));
				} catch (NumberFormatException e) {
					e.printStackTrace();
					config.put(key, value);
				}
			} else
				config.put(key, value);
		}
		return config;
	}

	public static void downloadFromHDFS(String hdfsUrl, String localUrl)
		throws IOException {
		Path path = new Path(hdfsUrl);
		FileSystem fs = path.getFileSystem(conf);
		Path p1 = new Path(hdfsUrl);
		Path p2 = new Path(localUrl);
		logger.info("copy from " + p1 + " to " + p2);
		fs.copyToLocalFile(false, p1, p2, true);
		fs.close();// 释放资源
	}

	public static void main(String[] args) {
	}

	public static void putLocalToHDFS(Path src, Path dest) {
		//CompressUploader.putLocalToHDFS(true, src, dest);
		oldputLocalToHDFS(src, dest);
	}

	private static void oldputLocalToHDFS(Path src, Path dest) {
		conf.set("fs.hdfs.impl",
			org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
			org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("hadoop.user", "hadoop");
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		try {
			FileSystem fs = dest.getFileSystem(conf);
			fs.mkdirs(dest);
			fs.copyFromLocalFile(true, src, dest);
			logger.info("Transfer " + dest + " from " + src);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
