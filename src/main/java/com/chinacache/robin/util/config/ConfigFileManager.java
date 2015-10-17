package com.chinacache.robin.util.config;

import com.chinacache.robin.util.hdfs.HDFSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.channels.FileLock;
import java.util.concurrent.TimeUnit;

/**
 * Download the configurations the first time the bolt started
 *
 * @author robinmac
 */
public class ConfigFileManager implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory
		.getLogger(ConfigFileManager.class);

//	public static void deleteAndDownload() {
//		ManageBoltUtil.initConfig();
//		String baseUrl = Locator.getInstance().getNLABaseLocation();
//		// File stormconf = new File(baseUrl + "storm.conf");
//		// if (stormconf.exists())
//		// stormconf.delete();
//		File groupconf = new File(baseUrl + "ccgroup.conf");
//		if (groupconf.exists())
//			groupconf.delete();
//		// File sysp = new File(baseUrl + "logpick-system.properties");
//		// if (sysp.exists())
//		// sysp.delete();
//		// File admam = new File(baseUrl + "adam_channels.conf");
//		// if (admam.exists())
//		// admam.delete();
//		// directly remove the storm.conf
//		for (String conf : conflist) {
//			try {
//				File tmpfile = new File(baseUrl + conf);
//				if (!tmpfile.exists())
//					HDFSConfUtility.downloadFromHDFS(AllConfiguration.HDFS_CONFIG_URL + conf,
//							baseUrl + conf);
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//	}

	public static synchronized void downloadConf() throws IOException {
		ManageBoltUtil.initConfig();
		final String baseUrl = Locator.getInstance().getNLABaseLocation();

		operationWthFileLock(baseUrl + "lock.lock", new Runnable() {
			@Override
			public void run() {
				checkAndDelete(baseUrl, AllConfiguration.STORM_CONF_FILE_NAME);
				checkAndDelete(baseUrl, "ccgroup.conf");
				checkAndDelete(baseUrl, AllConfiguration.ADAM_CONFIG_FILE);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				// directly remove the storm.conf
				for (String fileName : AllConfiguration.conflist) {
					File file = new File(baseUrl + fileName);
					file.deleteOnExit();
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					try {
						HDFSUtility.downloadFromHDFS(AllConfiguration.HDFS_CONFIG_URL
							+ fileName, baseUrl + fileName);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		});
	}

	public static void operationWthFileLock(String lockFileName, Runnable runnable) {
		FileLock lock = null;
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(lockFileName);
			lock = out.getChannel().lock();

			Thread t = new Thread(runnable);
			t.start();
			t.join();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (lock != null)
				try {
					lock.release();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (out != null)
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}

	public static void checkAndDelete(String baseDir, String fileName) {

		String localFileName = baseDir + fileName;
		boolean needDel = false;
		try {
			File localFile = new File(localFileName);

			if (!localFile.exists()) {
				needDel = true;
				LOG.info("conf file {} not exist", fileName);
			} else {
				long modifyTime = localFile.lastModified();
				long timeInterval = TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES);

				if ((System.currentTimeMillis() - modifyTime > timeInterval)) {
					needDel = true;
					LOG.info("conf file {} too old", fileName);
				}
			}
			int pid = getPid();

			if (needDel) {
				localFile.delete();
				LOG.info("Download conf file {} by process={}", fileName, pid);
			} else {
				LOG.info("NoNeed to download conf file {} by process={} ", fileName, pid);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void downloadTestMessageFile() {
		ManageBoltUtil.initConfig();
		String baseUrl = Locator.getInstance().getNLABaseLocation();
		String filename = "mengyou";
		try {
			HDFSUtility.downloadFromHDFS(AllConfiguration.HDFS_CONFIG_URL + filename, baseUrl
				+ filename);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static int getPid() {
		RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
		String name = runtime.getName(); // format: "pid@hostname"
		System.out.println(name);
		try {
			return Integer.parseInt(name.substring(0, name.indexOf('@')));
		} catch (Exception e) {
			return -1;
		}
	}

	public static void writeToFile(String fileName) {
		try {
			File file = new File(fileName);
			if (file.exists()) {
				LOG.info("No need write");
				return;
			}
			LOG.info("write to " + fileName);
			BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
			writer.write(String.valueOf(getPid()) + "\n");
			writer.write(String.valueOf(System.currentTimeMillis()));
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		final String dir = "H:\\Project\\StormOnline\\";
		operationWthFileLock(dir + "lock.lock", new Runnable() {
			@Override
			public void run() {
				checkAndDelete(dir, "tmp.txt");
				try {
					TimeUnit.SECONDS.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				writeToFile(dir + "tmp.txt");
			}
		});
	}

}
