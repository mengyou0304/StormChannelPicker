package com.chinacache.robin.grouping.dp;

import com.chinacache.robin.util.FileUtility;
import com.chinacache.robin.util.basic.LineProcess;
import com.chinacache.robin.util.config.Locator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

public class GenerateCCGroupingConf implements Serializable {

	private static final long serialVersionUID = 1L;

	private Logger logger = LoggerFactory
			.getLogger(GenerateCCGroupingConf.class);

	private static final HashMap<String, Integer> weightConstMap = new HashMap<String, Integer>();

	private final HashMap<String, Long> weightMap = new HashMap<String, Long>();
	private final HashMap<String, Integer> priorityMap = new HashMap<String, Integer>();

	public void init() {
		weightConstMap.put("P1", 3);
		weightConstMap.put("P2", 3);
		weightConstMap.put("P3", 3);
		weightConstMap.put("P4", 2);
		weightConstMap.put("P5", 2);
		weightConstMap.put("P6", 2);
		weightConstMap.put("P7", 1);
		weightConstMap.put("P8", 1);
		weightConstMap.put("P9", 1);
	}

	public void test() {
		String url = Locator.getInstance().getNLABaseLocation();
		System.out.println(url);

		FileUtility.processByFile(url + "channel_size.txt", new LineProcess() {
			@Override
			public void onEachFileLine(String line,Integer lineNum,Long offset) {
				String[] ss = line.split("\t");
				String s1 = ss[0];
				Long weight = 0l;
				if (ss.length > 1)
					weight = Long.parseLong(ss[1]);
				else
					logger.warn("wrong line in channel_size.txt" + line);
				weightMap.put(s1, weight);
				// System.out.println("put "+ s1+" \t "+weight);
			}
		});
		FileUtility.processByFile(url + "channels_Priority.conf",
				new LineProcess() {
					@Override
					public void onEachFileLine(String line,Integer lineNum,Long offset) {
						String[] ss = line.split(" ");
						String key = ss[0];
						Integer weight = 1;
						if (ss.length > 1) {
							weight = weightConstMap.get(ss[1]);
							if (weight == null) {
								logger.warn("no weight recognition of key:"
										+ key + " with value " + ss[1]);
								weight = 1;
							}
						} else
							logger.warn("wrong line in channels_Priority: "
									+ line);
						priorityMap.put(key, weight);
					}
				});
		Set<String> keyset = weightMap.keySet();
		int minv=1;
		StringBuffer line = new StringBuffer();
		for (String key : keyset) {
			Integer weight = priorityMap.get(key);
			if (weight == null)
				weight = weightConstMap.get("P9");
			Long oldNum = weightMap.get(key);
			Long newNum = oldNum * weight;
			weightMap.put(key, newNum);
		}
		int sum=0;
		for(String key :keyset){
			Long divnum=(weightMap.get(key)/1000000l);
			if(divnum<0)
				System.out.println(divnum.intValue());
			line.append(key + "=" + divnum + "\n");
			System.out.println((sum+=divnum));
		}
		FileUtility.writeToFile(url + "ccgroup2.conf", line.toString());
	}

	public static void main(String[] args) {
		GenerateCCGroupingConf gc = new GenerateCCGroupingConf();
		gc.init();
		gc.test();
	}

}
