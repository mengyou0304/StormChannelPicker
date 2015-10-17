package com.chinacache.robin.bolt;

import backtype.storm.Config;
import com.chinacache.robin.bolt.managebolt.ExceptionCollectionBolt;
import com.chinacache.robin.bolt.managebolt.LogCollectionBolt;
import com.chinacache.robin.bolt.managebolt.SatasticCollectionBolt;
import com.chinacache.robin.format.action.LocalToHDFSMoveFileAction;
import com.chinacache.robin.format.action.SimpleMoveFileAction;
import com.chinacache.robin.format.fileunit.LocalFileUnit;
import com.chinacache.robin.format.messageformat.ChinaCacheRecordFormat;
import com.chinacache.robin.format.messageformat.LogExceptionRecordFormat;
import com.chinacache.robin.format.nameformat.CCFileNameFormat;
import com.chinacache.robin.format.nameformat.LogFileNameFormat;
import com.chinacache.robin.format.nameformat.SimpleFileNameFormat;
import com.chinacache.robin.format.rotation.FileSizeAndTimeRotationPolicy;
import com.chinacache.robin.format.rotation.SimpleFileSizeRotationPolicy;
import com.chinacache.robin.format.sync.CCSyncPolicy;
import com.chinacache.robin.format.sync.SimpleCountSyncPolicy;
import com.chinacache.robin.util.FileUtility;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;

public class CCBoltGenerator implements Serializable {
	private static final long serialVersionUID = 1L;

	public static MultiFileHDFSBolt getMultifileBolt(Config config)
			throws IOException {
		CCSyncPolicy syncPolicy = new SimpleCountSyncPolicy(
				Integer.parseInt(String.valueOf(config.get("syncnum"))));

		FileSizeAndTimeRotationPolicy fSizeTimePolicy = new FileSizeAndTimeRotationPolicy(
				Integer.parseInt(String.valueOf(config.get("rotation.size"))),
				Integer.parseInt(String.valueOf(config.get("rotation.time"))),
				FileSizeRotationPolicy.Units.MB,
				TimedRotationPolicy.TimeUnit.SECONDS);

		CCFileNameFormat fileNameFormat = new LogFileNameFormat()
				.withExtension(".txt")
				.withTestTaskId(1);

		ChinaCacheRecordFormat format = new ChinaCacheRecordFormat();
		Yaml yaml = new Yaml();
		FileUtility.makeEmptyFile("/tmp/test.yaml");
		InputStream in = new FileInputStream("/tmp/test.yaml");
		Map<String, Object> yamlConf = (Map<String, Object>) yaml.load(in);
		in.close();
		config.put("hdfs.config", yamlConf);
		// init bolt
		MultiFileHDFSBolt hfdsbolt = new MultiFileHDFSBolt()
				.withConfigKey("hdfs.config")
				.withFileNameFormat(fileNameFormat).withRecordFormat(format)
				.withRotationPolicy(fSizeTimePolicy).withSyncPolicy(syncPolicy)
				.withFileUnit(LocalFileUnit.class)
				.addRotationAction(new LocalToHDFSMoveFileAction());
		return hfdsbolt;
	}

	public static LogCollectionBolt getLogCollectionBolt(Config config)
			throws IOException {
		CCSyncPolicy syncPolicy = new SimpleCountSyncPolicy(
				Integer.parseInt(String.valueOf(10)));
		SimpleFileSizeRotationPolicy fSizeTimePolicy = new SimpleFileSizeRotationPolicy(
				Integer.parseInt(String.valueOf(config.get("rotation.size"))),
				SimpleFileSizeRotationPolicy.Units.MB);

		SimpleFileNameFormat fileNameFormat = new SimpleFileNameFormat()
				.withPath("/storm/log/").withExtension("");

		LogExceptionRecordFormat format = new LogExceptionRecordFormat();
		Yaml yaml = new Yaml();
		FileUtility.makeEmptyFile("/tmp/test.yaml");
		InputStream in = new FileInputStream("/tmp/test.yaml");
		Map<String, Object> yamlConf = (Map<String, Object>) yaml.load(in);
		in.close();
		config.put("hdfs.config", yamlConf);
		// init bolt
		LogCollectionBolt lbolt = new LogCollectionBolt();
		lbolt.withConfigKey("hdfs.config");
		lbolt.withFileNameFormat(fileNameFormat);
		lbolt.withRecordFormat(format);
		lbolt.withRotationPolicy(fSizeTimePolicy);
		lbolt.withSyncPolicy(syncPolicy);
		lbolt.addRotationAction(new SimpleMoveFileAction());
		return lbolt;
	}

	public static ExceptionCollectionBolt getExceptionCollectionBolt(
			Config config) throws IOException {
		CCSyncPolicy syncPolicy = new SimpleCountSyncPolicy(
				Integer.parseInt(String.valueOf(3)));
		SimpleFileSizeRotationPolicy fSizeTimePolicy = new SimpleFileSizeRotationPolicy(
				Integer.parseInt(String.valueOf(config.get("rotation.size"))),
				SimpleFileSizeRotationPolicy.Units.MB);

		SimpleFileNameFormat fileNameFormat = new SimpleFileNameFormat()
				.withPath("/storm/ex/").withExtension("");

		LogExceptionRecordFormat format = new LogExceptionRecordFormat();
		Yaml yaml = new Yaml();
		FileUtility.makeEmptyFile("/tmp/test.yaml");
		InputStream in = new FileInputStream("/tmp/test.yaml");
		Map<String, Object> yamlConf = (Map<String, Object>) yaml.load(in);
		in.close();
		config.put("hdfs.config", yamlConf);
		// init bolt
		ExceptionCollectionBolt lbolt = new ExceptionCollectionBolt();
		lbolt.withConfigKey("hdfs.config");
		lbolt.withFileNameFormat(fileNameFormat);
		lbolt.withRecordFormat(format);
		lbolt.withRotationPolicy(fSizeTimePolicy);
		lbolt.withSyncPolicy(syncPolicy);
		lbolt.addRotationAction(new SimpleMoveFileAction());
		return lbolt;
	}

	public static SatasticCollectionBolt getStatisticCollectionBolt(
			Config config) throws IOException {
		CCSyncPolicy syncPolicy = new SimpleCountSyncPolicy(
				Integer.parseInt(String.valueOf(1)));
		SimpleFileSizeRotationPolicy fSizeTimePolicy = new SimpleFileSizeRotationPolicy(
				Integer.parseInt(String.valueOf(config.get("rotation.size"))),
				SimpleFileSizeRotationPolicy.Units.MB);

		SimpleFileNameFormat fileNameFormat = new SimpleFileNameFormat()
				.withPath("/storm/sts/").withExtension("");

		LogExceptionRecordFormat format = new LogExceptionRecordFormat();
		Yaml yaml = new Yaml();
		FileUtility.makeEmptyFile("/tmp/test.yaml");
		InputStream in = new FileInputStream("/tmp/test.yaml");
		Map<String, Object> yamlConf = (Map<String, Object>) yaml.load(in);
		in.close();
		config.put("hdfs.config", yamlConf);
		// init bolt
		SatasticCollectionBolt lbolt = new SatasticCollectionBolt();
		lbolt.withConfigKey("hdfs.config");
		lbolt.withFileNameFormat(fileNameFormat);
		lbolt.withRecordFormat(format);
		lbolt.withRotationPolicy(fSizeTimePolicy);
		lbolt.withSyncPolicy(syncPolicy);
		lbolt.addRotationAction(new SimpleMoveFileAction());
		return lbolt;
	}
	// public static StatusContorlBolt getStatusCollectionBolt(Config config)
	// throws IOException {
	// SyncPolicy syncPolicy = new CountSyncPolicy(Integer.parseInt(String
	// .valueOf(config.get("syncnum"))));
	// FileSizeRotationPolicy fSizeTimePolicy = new FileSizeRotationPolicy(
	// Integer.parseInt(String.valueOf(config.get("rotation.size"))),
	// FileSizeRotationPolicy.Units.MB);
	//
	// FileNameFormat fileNameFormat = new LogFileNameFormat()
	// .withPath("/storm/logs/").withExtension(".txt")
	// .withTestComponentID("logbolt").withTestTaskId(1);
	//
	// LogExceptionRecordFormat format = new LogExceptionRecordFormat();
	// Yaml yaml = new Yaml();
	// FileUtility.makeEmptyFile("/tmp/test.yaml");
	// InputStream in = new FileInputStream("/tmp/test.yaml");
	// Map<String, Object> yamlConf = (Map<String, Object>) yaml.load(in);
	// in.close();
	// config.put("hdfs.config", yamlConf);
	// // init bolt
	// StatusContorlBolt lbolt = new StatusContorlBolt();
	// lbolt.withConfigKey("hdfs.config");
	// lbolt.withFsUrl(HDFSConfUtility.HDFS_URL);
	// lbolt.withFileNameFormat(fileNameFormat);
	// lbolt.withRecordFormat(format);
	// lbolt.withRotationPolicy(fSizeTimePolicy);
	// lbolt.withSyncPolicy(syncPolicy);
	// lbolt.addRotationAction(new ChinaCacheMoveFileAction());
	// return lbolt;
	// }

}
