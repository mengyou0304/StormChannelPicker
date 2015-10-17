package com.chinacache.robin.grouping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




import com.chinacache.robin.util.FileUtility;
import com.chinacache.robin.util.basic.Entry;
import com.chinacache.robin.util.basic.Sorter;


public class KeyManager {

	private static final Logger logger = LoggerFactory
			.getLogger(KeyManager.class);

	private KeyManager(List<Integer> targetTasks) {
		this.boltTaskIDList = targetTasks;
	}

	private static KeyManager instance;

	public synchronized static KeyManager getInstance(List<Integer> targetTasks) {
		if (instance == null)
			instance = new KeyManager(targetTasks);
		return instance;
	}

	private List<Integer> boltTaskIDList;

	public HashMap<String, ArrayList<Integer>> getBalanceMap(String url) {
		HashMap<String, Integer> keyWeightMap = new HashMap<String, Integer>();
		String conf = FileUtility.readFromFile(url);
		String[] lines = conf.split("\n");
		for (String line : lines) {
			String[] ss = line.split("=");
			if (ss.length != 2)
				continue;
			String key = ss[0];
			int intweight = 1;
			Long weight = Long.parseLong(ss[1]);
			intweight = weight.intValue();
			if (intweight < 0)
				intweight = -intweight;
			keyWeightMap.put(key, intweight);
		}
		return manageBoldAndKeys(keyWeightMap);
	}

	public HashMap<String, ArrayList<Integer>> readfromHDFS(String url) {
		HashMap<String, Integer> keyWeightMap = new HashMap<String, Integer>();
		return manageBoldAndKeys(keyWeightMap);
	}

	private HashMap<String, ArrayList<Integer>> manageBoldAndKeys(
			HashMap<String, Integer> keyWeightMap) {
		Set<String> entrySet = keyWeightMap.keySet();
		Long totalWeight = 0L;
		ArrayList<Entry> enlist = new ArrayList<Entry>();
		for (String key : entrySet) {
			Integer weight = keyWeightMap.get(key);
			Entry en = new Entry(weight, key);
			enlist.add(en);
			totalWeight += weight;
		}

		// sort keys according to their weights
		Sorter.qsort(enlist, 0, enlist.size() - 1);

		// use basic strategy for load balance
		int boltNum = boltTaskIDList.size();
		Double slotWeight = (1.0D * totalWeight) / (boltNum * 15);
		logger.info("total={}, tasknum={}, slot={}", totalWeight,
				boltTaskIDList.size(), slotWeight);

		// boltmap save info : task1---> <key1,key2>
		HashMap<Integer, ArrayList<String>> boltMap = new HashMap<Integer, ArrayList<String>>();
		for (Integer taskID : boltTaskIDList) {
			boltMap.put(taskID, new ArrayList<String>());
		}
		// System.out.println("task ids:"+boldTaskIDList);

		int taskIndexer = 0;
		for (int i = 0; i < enlist.size(); i++) {
			Entry entry = enlist.get(i);
			if (entry.getKey().equals("000"))
				continue;
			double cweight = entry.getWeight();
			// some bolt may occupy several bolt
			while (cweight >= -0.01d) {
				// TODO index wrong!!
				ArrayList<String> keylist = boltMap.get(boltTaskIDList
						.get(taskIndexer));
				keylist.add(entry.getKey());
				taskIndexer = (taskIndexer + 1) % boltTaskIDList.size();
				cweight = cweight - slotWeight;
			}
		}
		// System.out.println("=");
		// FileUtility.debugMap(boltmap);
		// System.out.println("=");
		HashMap<String, ArrayList<Integer>> keymap = new HashMap<String, ArrayList<Integer>>();
		Set<Integer> boltKeySet = boltMap.keySet();
		for (Integer boltid : boltKeySet) {
			ArrayList<String> keys = boltMap.get(boltid);
			for (String key : keys) {
				ArrayList<Integer> list = keymap.get(key);
				if (list == null) {
					list = new ArrayList<Integer>();
					keymap.put(key, list);
				}
				list.add(boltid);
			}
		}
		// FileUtility.debugMap(keymap);
		return keymap;
	}
}
