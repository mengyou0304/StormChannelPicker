package com.chinacache.robin.util;

import com.chinacache.robin.util.config.AllConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to monitor HDFS file or directory changes. It's only duty
 * is report the change and call Callback::execute method
 *
 * Anyone who interested in file change should be register the file or
 * directory, and implement the Callback interface
 *
 * @author zexing.hu 2015/05/21
 */
public class ResourceManager {
	private static final Logger LOG = LoggerFactory
			.getLogger(ResourceManager.class);

	private Map<String, Long> fileUpdateTimes;
	private Map<String, Callback> fileUpdateAction;
	FileSystem fileSystem;

	ScheduledExecutorService executorService;

	private ResourceManager() {
		fileUpdateTimes = new ConcurrentHashMap<String, Long>();
		fileUpdateAction = new ConcurrentHashMap<String, Callback>();

		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("hadoop.user", "hadoop");
		conf.set("fs.hdfs.impl.disable.cache", "true");

		try {
			Path path = new Path(AllConfiguration.HDFS_CONFIG_URL);
			fileSystem = path.getFileSystem(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		executorService = Executors.newScheduledThreadPool(1);
		executorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				update();
			}
		}, 1, 3, TimeUnit.MINUTES);
	}

	private static class InstanceHolder {
		private static ResourceManager instance = new ResourceManager();
	}

	public static ResourceManager getInstance() {
		return InstanceHolder.instance;
	}

	public synchronized void register(String filePath, Callback callback) {
		fileUpdateAction.put(filePath, callback);
		fileUpdateTimes.put(filePath, 0L);

		LOG.info("register hdfs file :" + filePath);
		FileStatus status;
		try {
			status = fileSystem.getFileStatus(new Path(filePath));
			fileUpdateTimes.put(filePath, status.getModificationTime());
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized void remove(String filePath) {
		if (fileUpdateTimes.containsKey(filePath)) {
			fileUpdateTimes.remove(filePath);
			fileUpdateAction.remove(filePath);
		}
	}

	public synchronized void update() {
		Set<String> files = fileUpdateTimes.keySet();
		for (String filePath : files) {
			LOG.info("Watcher file :" + filePath);
			try {
				FileStatus status = fileSystem
						.getFileStatus(new Path(filePath));
				long lastTime = status.getModificationTime();
				long previoustime = fileUpdateTimes.get(filePath);
				if (lastTime > previoustime) {
					fileUpdateTimes.put(filePath, lastTime);
					fileUpdateAction.get(filePath).execute();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		ResourceManager manager = ResourceManager.getInstance();
		String localFile = args[0];
		String hdfsFile = args[1];
		ReinitLogPicker callback = new ReinitLogPicker(localFile, hdfsFile);
		manager.register(hdfsFile, callback);
	}

}
