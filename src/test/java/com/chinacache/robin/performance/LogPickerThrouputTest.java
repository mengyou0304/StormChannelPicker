/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.performance;

import com.chinacache.robin.bolt.KeyGenerationBolt;
import com.chinacache.robin.logic.NLAKeyGenerator;
import com.chinacache.robin.logic.NLAResult;
import com.chinacache.robin.util.FileUtility;
import com.chinacache.robin.util.basic.LineProcess;
import com.chinacache.robin.util.config.Locator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by robinmac on 15-8-28.
 */
public class LogPickerThrouputTest {
	private static final Logger logger = LoggerFactory
		.getLogger(KeyGenerationBolt.class);

	NLAKeyGenerator keyGenrator;
	String[] lines = new String[100000];
	int i = 0;
	public void init() {
		String url = Locator.getInstance().getNLABaseLocation();
		keyGenrator = new NLAKeyGenerator();
//		keyGenrator=new NLAKeyGenerator2();
		keyGenrator.init();

		FileUtility.processByFile(url + "mengyou", new LineProcess() {
			@Override
			public void onEachFileLine(String line, Integer lineNum, Long offset) {
				if (i < lines.length)
					lines[i++] = line;
			}
		});
		for(int k=0;i<lines.length&&k<lines.length;k++){
			lines[i++]=lines[k];
		}
		logger.info("Init finish");

	}

	public void start(int k) {
		logger.info("Start Test.");
		int nullnum = 0;
		int fuckeddata = 0;
		int linnum = 0;
		Long starttime = System.currentTimeMillis();
		for (int i = 0; i < k; i++) {
			for (String line : lines) {
				//System.out.println(line);
				NLAResult key = keyGenrator.execute(line);
				if (key == null) {
					nullnum = 0;
					continue;
				}
				if (key.getType() == null || key.getType().length() == 0)
					fuckeddata++;
				if (key.getChannelID() == null || key.getChannelID().length() == 0)
					fuckeddata++;
				if (key.getUserID() == null || key.getUserID().length() == 0)
					fuckeddata++;
//				System.out.println(key.getChannelID());
//				if (key.getUserID().equals("2195")) {
//					System.out.println(line);
//					System.out.println(key.getChannelID());
//					System.out.println(key.getType());
//					System.out.println(key.getUserID());
//					System.out.println(key.getMessage());
//					System.out.println("*********");
//				}
				linnum++;
			}
		}
		Long endtime = System.currentTimeMillis();
		String res = "";
		res += "time: " + (endtime - starttime) + " data: " + (k * 10) + "w\n";
		res += "nullnum: " + nullnum + " fuckeddata:" + fuckeddata;
		logger.info(res);
//		System.out.println(keyGenrator.getTimes());

	}

	public static void main(String[] args) {
		LogPickerThrouputTest tl = new LogPickerThrouputTest();
		tl.init();
		tl.start(20);
	}

}
