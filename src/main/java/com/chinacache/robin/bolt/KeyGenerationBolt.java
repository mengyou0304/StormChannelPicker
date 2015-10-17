/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.chinacache.robin.bolt.basic.CCBolt;
import com.chinacache.robin.logic.NLAKeyGenerator;
import com.chinacache.robin.logic.NLAResult;
import com.chinacache.robin.util.RandomKeyGenerator;
import com.chinacache.robin.util.ReinitLogPicker;
import com.chinacache.robin.util.ResourceManager;
import com.chinacache.robin.util.config.AllConfiguration;
import com.chinacache.robin.util.config.ConfigFileManager;
import com.chinacache.robin.util.config.Locator;
import com.chinacache.robin.util.config.ManageBoltUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;

/**
 * accept sentence and get the keys
 * 
 * @author robinmac
 */
public class KeyGenerationBolt extends CCBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory
			.getLogger(KeyGenerationBolt.class);

	Long num = 1l;
	static Integer ex_num = 0;
	static long lastTime = 0;
	static long startTime = 0;
	TopologyContext tc;
	OutputCollector tcollecer;

	NLAKeyGenerator keyGenrator;
	RandomKeyGenerator rkg;
	Timer timer;

	Boolean useLogic = true;
	Map stormconfs;
	String fakeURL = "";

	public KeyGenerationBolt(boolean uselogic, String URL) {
		fakeURL = URL;
		if (uselogic)
			useLogic = true;
		else {
			useLogic = false;
		}
	}

	@Override
	public void declareCCOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("channelid", "userid", "type", "logline"));
	}

	@Override
	public void onClose() {

	}

	@Override
	public void onLogicDataArrive(Tuple input) {
		String line = input.getString(0);
		if (line == null || line.trim().length() == 0)
			return;
		num++;
		if (num % 10000 == 0) {
			long curTime = System.currentTimeMillis();
			String info = ("[in KeyBolt] message:" + (num / 10000)
					+ "W, usingtime for 1W: " + (curTime - lastTime)
					+ " avr for 1W:" + (curTime - startTime) / (num / 10000)
					+ " exception num:" + ex_num + " useLogic:" + useLogic);
			logger.info(info);

			sendToStatsiticBolt(new Values(
					ManageBoltUtil.STATISTIC_BOLT_ACTION_ADD, "KeyGenerator.W",
					1L), tcollecer);
			sendToStatsiticBolt(new Values(
					ManageBoltUtil.STATISTIC_BOLT_ACTION_ADD, "KeyGenerator.T",
					1L * (curTime - lastTime)), tcollecer);
			lastTime = curTime;
		}
		NLAResult key = null;

		try {
			if (keyGenrator == null)
				return;
			key = keyGenrator.execute(line);
			if (key == null) {
				List<Object> list = new ArrayList<Object>();
				list.add("010");
				list.add("000");
				list.add("UNKNOWN");
				list.add(line);
				tcollecer.emit(input, list);
				return;
			}
			
			// These logs are useless and occupy too much space, so ignored
			String channel = key.getChannelID();
			if ("AtType".equals(channel) || "LogSkiped".equals(channel))
				return;
			
			List<Object> list = new ArrayList<Object>();
			list.add(key.getChannelID());
			list.add(key.getUserID());
			list.add(key.getType());
			list.add(key.getMessage());
			// list.add(key);
			tcollecer.emit(input, list);
		} catch (Exception e) {
			ex_num++;
			List<Object> list = new ArrayList<Object>();
			list.add("011");
			list.add("000");
			list.add("PARSEEXCEPTION");
			list.add(line);
			tcollecer.emit(input, list);
		}
	}

	@Override
	public Boolean onControlDataArrive(Tuple input) {
		return Boolean.FALSE;
	}

	@Override
	public void onPrepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.tcollecer = collector;
		tc = context;
		if (!useLogic)
			rkg = RandomKeyGenerator.getInstance(fakeURL);
		this.stormconfs = stormConf;

		try {
			ConfigFileManager.downloadConf();
			Thread.sleep(100);
			keyGenrator = new NLAKeyGenerator();
			keyGenrator.init();
		} catch (Exception e) {
			e.printStackTrace();
		}
		lastTime = System.currentTimeMillis();
		startTime = System.currentTimeMillis();

		ManageBoltUtil.initConfig();
		String baseUrl = Locator.getInstance().getNLABaseLocation();
		String localFile = baseUrl + AllConfiguration.ADAM_CONFIG_FILE;
		String hdfsFile = AllConfiguration.HDFS_CONFIG_URL + AllConfiguration.ADAM_CONFIG_FILE;
		ReinitLogPicker callback = new ReinitLogPicker(localFile, hdfsFile);

		ResourceManager manager = ResourceManager.getInstance();
		manager.register(hdfsFile, callback);

		// timer = new Timer();
		// timer.schedule(new AdmConfUpdater(), 1000L * 3600, 3L * 3600 * 1000);
	}

}
