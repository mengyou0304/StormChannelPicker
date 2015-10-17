package com.chinacache.robin.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.concurrent.TimeUnit;

import com.chinacache.robin.util.hdfs.HDFSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chinacache.log.common.service.LogLineService;

public class ReinitLogPicker implements Callback {
	private static final Logger LOG = LoggerFactory
			.getLogger(ReinitLogPicker.class);

	private String localFilePath;
	private String hdfsFilePath;

	public ReinitLogPicker(String localFile, String hdfsFile) {
		localFilePath = localFile;
		hdfsFilePath = hdfsFile;
	}

	@Override
	public void execute() {
		FileOutputStream out;
		FileLock lock = null;
		try {
			out = new FileOutputStream(localFilePath);
			lock = out.getChannel().tryLock();
			if (lock != null) {
				File localFile = new File(localFilePath);
				long modifyTime = localFile.lastModified();
				if (localFile.exists()
						&& (System.currentTimeMillis() - modifyTime < TimeUnit.MILLISECONDS
								.convert(5, TimeUnit.MINUTES))) {
					LOG.info("Download hdfs file :" + hdfsFilePath);
					HDFSUtility.downloadFromHDFS(hdfsFilePath, localFilePath);
				}

				LOG.info("Action : LogLineService.reload();");
				LogLineService.reload();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (lock != null) {
				try {
					lock.release();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
