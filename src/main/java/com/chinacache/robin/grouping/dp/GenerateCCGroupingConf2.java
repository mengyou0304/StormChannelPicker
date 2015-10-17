package com.chinacache.robin.grouping.dp;

import com.chinacache.robin.util.FileUtility;
import com.chinacache.robin.util.basic.LineProcess;
import com.chinacache.robin.util.config.Locator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;

public class GenerateCCGroupingConf2 implements Serializable {

	private static final long serialVersionUID = 1L;
	private Logger logger = LoggerFactory
			.getLogger(GenerateCCGroupingConf2.class);

	public void init() {

	}

	public void test() {
		String url = Locator.getInstance().getNLABaseLocation();
		System.out.println(url);
		final StringBuffer sb = new StringBuffer();
		final HashMap<String, Long> map = new HashMap<String, Long>();
		FileUtility.processByFile(url + "size2.out", new LineProcess() {
			@Override
			public void onEachFileLine(String line,Integer linenum,Long offset) {
				String[] ss = line.split(" ");
				String s1 = ss[0];
				ss[1] = ss[ss.length - 1];
				Long weight = 0l;
				weight = Long.parseLong(ss[0].trim());
				weight = weight / 10000000l;
				String key = "";
				char[] cc = ss[1].toCharArray();
				for (int i = cc.length - 1; i >= 0; i--) {
					if (cc[i] == '/') {
						key = ss[1].substring(i + 1, ss[1].length());
						break;
					}
				}
				map.put(key, weight);
				// System.out.println(key+"="+weight);
			}
		});
		FileUtility.processByFile(url + "size.out", new LineProcess() {
			@Override
			public void onEachFileLine(String line,Integer lineNum,Long offset) {
				String[] ss = line.split(" ");
				String s1 = ss[0];
				ss[1] = ss[ss.length - 1];
				Long weight = 0l;
				weight = Long.parseLong(ss[0].trim());
				weight = weight / 10000000l;
				String key = "";
				char[] cc = ss[1].toCharArray();
				for (int i = cc.length - 1; i >= 0; i--) {
					if (cc[i] == '/') {
						key = ss[1].substring(i + 1, ss[1].length());
						break;
					}
				}
				if (map.containsKey(key))
					map.put(key, (map.get(key) + weight) / 2);
				else
					map.put(key, weight);
			}
		});
		for (String key : map.keySet()) {
			sb.append(key + "=" + map.get(key) + "\n");
		}
		FileUtility.writeToFile(url + "" +
				".conf", sb.toString());
	}

	public static void main(String[] args) {
		GenerateCCGroupingConf2 gc = new GenerateCCGroupingConf2();
		gc.init();
		gc.test();
	}

}
