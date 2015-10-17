package com.chinacache.robin.service;

import org.apache.hadoop.fs.Path;

enum FileEventType {
	COMPRESS, UPLOAD,TO_INNER_KAFKA
}

/**
 * Event when file move from local to HDFS, currently support COMPRESS and
 * UPLOAD
 * 
 * @author zexing.hu
 *
 */
public class FileEvent extends AbstractEvent<FileEventType> {

	private Path localFile;

	private Path hdfsFile;
	private String keyID;

	private boolean delSrc;

	public FileEvent(FileEventType type, Path localFile, Path hdfsDestFile,
	                 boolean del) {
		super(type);
		this.localFile = localFile;
		this.hdfsFile = hdfsDestFile;
		this.delSrc = del;
	}

	public FileEvent(FileEventType type,String keyID, Path localFile, Path hdfsDestFile,
			boolean del) {
		this(type,localFile,hdfsDestFile,del);
		this.keyID=keyID;
	}

	public Path getLocalFile() {
		return localFile;
	}

	public Path getHdfsFile() {
		return hdfsFile;
	}


	public String getKeyID() {
		return keyID;
	}

	public boolean getDeleteOption() {
		return delSrc;
	}

	@Override
	public String toString() {
		return String.format("{type=%s, localFile=%s, hdfsFIle=%s, del=%s}",
				getType(), localFile, hdfsFile, delSrc);
	}

	public static void main(String[] args) {
		FileEvent event = new FileEvent(FileEventType.UPLOAD,"channel1", new Path("src"),
				new Path("dest"), true);
		System.out.println(event);
	}
}
