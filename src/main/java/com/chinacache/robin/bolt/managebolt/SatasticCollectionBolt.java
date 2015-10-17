package com.chinacache.robin.bolt.managebolt;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.chinacache.robin.bolt.MultiFileHDFSBolt;
import com.chinacache.robin.util.basic.ExceptionUtil;
import com.chinacache.robin.util.config.ManageBoltUtil;

/**
 * 
 * 
 * @author robinmac
 */
public class SatasticCollectionBolt extends MultiFileHDFSBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory
			.getLogger(SatasticCollectionBolt.class);

	static String[] stbolt = new String[] { "MHDBolt", "KeyGenerator" };

	HashMap<String, Long> addMap = new HashMap<String, Long>();
	HashMap<String, String> statusMap = new HashMap<String, String>();

	int writetimes = 200;
	int ctimes = 0;

	@Override
	public void onPrepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		addMap = new HashMap<String, Long>();
		super.onPrepare(stormConf, context, collector);
	}

	public void onLogicDataArrive(Tuple tuple) {
		// if(tuple.getSourceStreamId().equals(FunctionBoltUtil.LOG_BOLT_STREAM_NAME)){
		if (tuple.getString(0).equals(ManageBoltUtil.STATISTIC_BOLT_ACTION_ADD)) {
			String key = tuple.getString(1);
			Long value = tuple.getLong(2);
			if (!addMap.containsKey(key)) {
				addMap.put(key, 0l);
			}
			Long origin=addMap.get(key) ;
			Long all=origin+ value;
			addMap.put(key, all);
			logger.debug("Add_action :" + key + ", " + origin +" -> "+all);
		}
		if (tuple.getString(0).equals(
				ManageBoltUtil.STATISTIC_BOLT_ACTION_UPDATE)) {
			String key = tuple.getString(1);
			String value = tuple.getString(2);
			statusMap.put(key, value);
			logger.debug("Update_action :" + key + "," + value);
		}
		ctimes++;
		if (ctimes >= writetimes) {
			super.addStatus(ManageBoltUtil.STATISTIC_BOLT_KEY_NAME,
					getStatusLine() + "\n");
			ctimes=0;
		}
	};

	public String getStatusLine() {
		StringBuffer sb = new StringBuffer();
		sb.append("********************\n");
		sb.append("**"+ExceptionUtil.sdf.format(new Date())+"**\n");
		sb.append("********************\n");
		
		Set<String> set = statusMap.keySet();
		for (String key : set) {
			String value = statusMap.get(key);
			sb.append(key + " : " + value + "\n");
		}
		Set<String> set2 = addMap.keySet();
		for (String key : set2) {
			Long value = addMap.get(key);
			sb.append(key + " : " + value + "\n");
		}
		sb.append("====================\n");
		for (String boltkey : stbolt) {
			Long num1=1l;
			Long time1=1l;
			num1 = addMap.get(boltkey + ".W");
			time1 = addMap.get(boltkey + ".T");
			String line = "Message Num in " + boltkey + " :" + num1
					+ " avg time for 1W:" + time1 / (num1);
			sb.append(line + "\n");
		}
		sb.append("********************\n");
		return sb.toString();
	}
}
