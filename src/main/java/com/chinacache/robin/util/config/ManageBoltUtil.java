package com.chinacache.robin.util.config;

import java.io.File;
import java.io.Serializable;

public class ManageBoltUtil implements Serializable {
	private static final long serialVersionUID = 1L;

	public static String LOG_BOLT_STREAM_NAME = "log.bolt.stream";
	public static String EXCEPTION_BOLT_STREAM_NAME = "exception.bolt.stream";
	public static String STATISTIC_BOLT_STREAM_NAME = "statistic.bolt.stream";
	public static String STATUS_BOLT_STREAM_NAME = "status.bolt.stream";

	public static String LOG_BOLT_KEY_NAME = "log";
	public static String EXCEPTION_BOLT_KEY_NAME = "ex";
	public static String STATISTIC_BOLT_KEY_NAME = "sts";
	public static boolean useManagementBolt = true;

	public static final Object STATISTIC_BOLT_ACTION_ADD = "statistic.add";
	public static final Object STATISTIC_BOLT_ACTION_UPDATE = "statistic.update";

	/**
	 * Generate local config file Location
	 */
	public static void initConfig() {
		String url = Locator.getInstance().getNLABaseLocation();
		File file = new File(url);
		File path = file.getAbsoluteFile();
		path.mkdirs();
	}
}
