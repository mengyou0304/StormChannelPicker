/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.logic;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class ConfigurableContants {

	private static final String DEFAULT_VALUE = "";

	private static final Logger logger = LoggerFactory.getLogger(ConfigurableContants.class);

	private static final Properties p = new Properties();

	static {
		init("/stormstat-system.properties");
	}

	private static void init(String propertyFileName) {

		InputStream inputStream = null;

		try {

			inputStream = ConfigurableContants.class.getResourceAsStream(propertyFileName);
			p.load(inputStream);

		} catch (IOException e) {
			logger.error("load " + propertyFileName + " into Contants error");
		} finally {
			IOUtils.closeQuietly(inputStream);
		}
	}

	protected static String getProperty(String key, String defaultValue) {
		String str = p.getProperty(key, defaultValue);
		return str.trim();
	}

	protected static String getProperty(String key) {
		return getProperty(key, DEFAULT_VALUE);
	}

	/**
	 * 取以","分割的集合属性
	 */
	protected static Set<String> getSetProperty(String key, String defaultValue) {

		String[] strings = p.getProperty(key, defaultValue).split(",");
		HashSet<String> hashSet = new HashSet<String>(strings.length);
		for (String string : strings) {
			hashSet.add(string.trim());
		}
		return hashSet;
	}

	protected static Set<String> getSetProperty(String key) {
		return getSetProperty(key, DEFAULT_VALUE);
	}

	/**
	 * 取以":"分隔，再以","分隔的映射
	 */
	protected static Map<String, Integer> getMapProperty(String key) {

		String[] mappings = p.getProperty(key).split(":");
		Map<String, Integer> map = new HashMap<String, Integer>();
		for (String entry : mappings) {
			String[] splits = entry.split(",");
			map.put(splits[0].trim(), Integer.parseInt(splits[1].trim()));
		}
		return map;
	}

	/**
	 * 取以";"分隔记录，再以":"分隔的前半部分为整个map的key, 以":"分隔的后部分字符串str, 再将str以"@"分隔，得到另一个形如："FCACCESS-Channel5Minute,ChannelHourProvince",
	 * 再将"FCACCESS-Channel5Minute,ChannelHourProvince"用"-"和","分隔,最后组成Map<String,List<String>>,作为整个map的value
	 * 备注：channelId:LogType1-StatType1,StatType2@LogType2-StatType3,StatType4;
	 * eg:   13576:FCACCESS-Channel5Minute@FMSACCESS-ChannelHourProvince;10001:FMSACCESS-ChannelHourISP,ChannelHourCountry,ChannelHourProvince
	 */
	protected static Map<String, Map<String, List<String>>> getMapPropertyForMap(String key) {
		Map<String, Map<String, List<String>>> map = new HashMap<String, Map<String, List<String>>>();
		String tmpStr = p.getProperty(key);
		if (tmpStr == null || "".equals(tmpStr.trim())) {
			return map;
		} else {
			String[] mappings = tmpStr.split(";");
			for (String entry : mappings) {
				String[] splits = entry.split(":");
				if (splits.length < 2) {
					continue;
				}
				Map<String, List<String>> logTypeMap = new HashMap<String, List<String>>();
				String channelId = splits[0].trim();
				String[] logTypeMapArr = splits[1].trim().split("@");
				for (String str : logTypeMapArr) {
					String[] arr = str.trim().split("-");
					if (arr.length < 2) {
						continue;
					}
					String logType = arr[0].trim();
					String[] statTypeArr = arr[1].trim().split(",");
					List<String> statTypeList = new ArrayList<String>();
					for (int j = 0; j < statTypeArr.length; j++) {
						statTypeList.add(statTypeArr[j].trim());
					}
					logTypeMap.put(logType, statTypeList);
				}
				map.put(channelId, logTypeMap);
			}
		}
		return map;
	}
}