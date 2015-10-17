package com.chinacache.robin.service;

import com.chinacache.robin.service.AsyncDispatcher.EventHandler;
import com.chinacache.robin.util.hdfs.CompressUploader;
import com.chinacache.robin.util.hdfs.HDFSUtility;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Use Event-Driven model to process file move event. When a file need
 * processed, only a event generated and put to FileUploadService. Details are
 * hidden in FileUploadService.
 *
 * @author zexing.hu
 *
 */
public class FileUploadService {
//	FilePersisitProducer kafkaConnection;
	HashMap<String,String> topicMap;

	private static class InstanceHolder {
		private static FileUploadService instance = new FileUploadService();
	}

	public static FileUploadService getInstance() {
		return InstanceHolder.instance;
	}

	private AsyncDispatcher dispatcher;

	private ThreadPoolExecutor pool;

	private FileUploadService() {
//		kafkaConnection= FilePersisitProducer.getInstance(AllConfiguration.KAFKA_INNER_BROKER);
//		kafkaConnection.buildConnection();
		pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);
		dispatcher = new AsyncDispatcher();
		dispatcher.register(FileEventType.class, new CompressEventHandler());
		dispatcher.start();
	}

	public void upload(final Path src, Path dest) {
		dispatcher.putEvent(new FileEvent(FileEventType.UPLOAD, src, dest, false));
	}
	public void uploadAndToKafka(final Path src, Path dest) {
		dispatcher.putEvent(new FileEvent(FileEventType.TO_INNER_KAFKA, src, dest, true));
	}

	public void compressAndUpload(final Path src, Path dest) {
		dispatcher.putEvent(new FileEvent(FileEventType.COMPRESS, src, dest, true));
	}

	public class CompressEventHandler implements EventHandler<FileEvent> {
		@Override
		public void handle(final FileEvent event) {
			FileEventType type = event.getType();
			if(type==FileEventType.TO_INNER_KAFKA){
				pool.submit(new Runnable() {
					@Override
					public void run() {
						String keyID=event.getKeyID();
						String topicid=topicMap.get(keyID);
//						kafkaConnection.sendFile(event.getLocalFile(),topicid,keyID);
						HDFSUtility.putLocalToHDFS(event.getLocalFile(),
							event.getHdfsFile());
					}
				});
			}
			if (type == FileEventType.UPLOAD) {
				pool.submit(new Runnable() {
					@Override
					public void run() {
						HDFSUtility.putLocalToHDFS(event.getLocalFile(),
							event.getHdfsFile());
					}
				});
			}
			if (type == FileEventType.COMPRESS) {
				pool.submit(new Runnable() {
					@Override
					public void run() {
						try {
							String gzFile = CompressUploader.compress(
									event.getLocalFile(),
									event.getDeleteOption());

							dispatcher.putEvent(new FileEvent(
									FileEventType.UPLOAD, new Path(gzFile),
									event.getHdfsFile(), event.getDeleteOption()));
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});
			}
		}

		@Override
		public String toString() {
			return getClass().getName();
		}
	}
}
