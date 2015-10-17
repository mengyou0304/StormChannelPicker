package com.chinacache.robin.util.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

/**
 * @author Robin
 *
 */
public class Locator implements Serializable {



	private static final Logger logger = LoggerFactory.getLogger(Locator.class);

	private static Locator instance = new Locator();
	

	private Locator() {

	}

	public static synchronized Locator getInstance() {
		return instance;
	}

	/**
	 */
	private String baseLocation = System.getProperty("user.dir")
			+ File.separator;

	public String getNLABaseLocation() {
		baseLocation = AllConfiguration.LOCAL_BASE_LOCATION;
		return this.baseLocation;
	}

	public static void main(String[] args) {
		Locator.getInstance().detectFileDirs();
		for (int i = 0; i < 20; i++)
			try {
				System.out.println(Locator.getInstance()
						.getLocalDataBaseLocation());
			} catch (Exception e) {
				e.printStackTrace();
			}
	}

	boolean detected = false;
	ArrayList<String> availableLocalURLs;

	public void detectFileDirs() {
		availableLocalURLs = new ArrayList<String>();
		for (int i = 0; i < 10; i++) {
			String currentURL = "/data/cache" + i + "/stormdata/";
			File file = new File(currentURL);
			if (file.exists() && file.canWrite())
				availableLocalURLs.add(currentURL);
		}
		logger.info("available file directory size:" + availableLocalURLs.size());
		detected = true;
	}

	public ArrayList<String> getAvailableLocalURLs() {
		return availableLocalURLs;
	}

	public String getLocalDataBaseLocation() throws Exception {
		if (!detected)
			throw new Exception(
					"locator haven't detect the capable files, please call method=detectFileDirs first");
		if (availableLocalURLs.size() == 0)
			throw new Exception(
					"No avialable LocalURL is found, please check the local_url like /data/cachen/stormdata/");
		Random r = new Random();
		int k = r.nextInt(availableLocalURLs.size());
		return availableLocalURLs.get(k);
	}
}
