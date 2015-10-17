package com.chinacache.robin.util;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class RandomKeyGenerator implements Serializable {
	ArrayList<String> keylist;
	private static HashMap<String, RandomKeyGenerator> instanceMap;

	public static synchronized RandomKeyGenerator getInstance(String URL) {
		if (instanceMap == null)
			instanceMap = new HashMap<String, RandomKeyGenerator>();
		RandomKeyGenerator instance = instanceMap.get(URL);
		if (instance == null) {
			instance = new RandomKeyGenerator(URL);
			instanceMap.put(URL, instance);
		}
		return instance;
	}

	public RandomKeyGenerator(String configFileURL) {
		keylist = new ArrayList<String>();
		String conf = FileUtility.readFromFile(configFileURL);
		String[] lines = conf.split("\n");
		for (String line : lines) {
			String[] ss = line.split("=");
			if (ss.length != 2)
				continue;
			String key = ss[0];
			Long longv=Long.parseLong(ss[1]);
			Integer weight =Integer.MAX_VALUE;
			if(longv<Integer.MAX_VALUE)
				weight = longv.intValue();
			for (int i = 0; i < weight; i++)
				keylist.add(key);
		}
	}

	public  String getKey() {
		Random r = new Random();
		return keylist.get(r.nextInt(keylist.size()));
	}

}
