package com.chinacache.robin.format.nameformat;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;

public interface CCFileNameFormat extends FileNameFormat {

	public CCFileNameFormat copy();

	public CCFileNameFormat withSpecialKey(String specialKey);

	public CCFileNameFormat withBoltID(String boltID);

	public CCFileNameFormat withLogType(String logtypo);
}
