package com.chinacache.robin.format.fileunit;

import backtype.storm.tuple.Tuple;
import com.chinacache.robin.bolt.MultiFileHDFSBolt;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;

/**
 * This class responsible for each channel data storage. We could save data on
 * local file, do some processing before upload to HDFS ; or send to hdfs
 * directly
 * 
 * @author you.meng, zexing.hu
 *
 */
public interface FileUnit extends Serializable {
	public static final String diskerro = "DISK_ERRER_WHEN_CREATINGFILE";

	public void prepare();

	public void init(MultiFileHDFSBolt multiFileHDFSBolt, String key,
					 String k1, String k2, String k3);

	public void rotateOutputFile() throws IOException;

	public void reOpenOutput() throws IOException;

	public void onTupleArrive(Tuple tuple) throws IOException;

	public void addStatus(String status) throws IOException;

	public Path getNewName(String parent, String oldname, FileSystem fs)
			throws IOException;

	public void onClose();

	public void initSimple(MultiFileHDFSBolt multiFileHDFSBolt, String key)
			throws Exception;
}
