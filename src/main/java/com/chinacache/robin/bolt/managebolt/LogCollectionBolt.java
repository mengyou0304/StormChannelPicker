package com.chinacache.robin.bolt.managebolt;

import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.chinacache.robin.bolt.MultiFileHDFSBolt;
import com.chinacache.robin.util.basic.ExceptionUtil;
import com.chinacache.robin.util.config.ManageBoltUtil;

public class LogCollectionBolt extends MultiFileHDFSBolt {

	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory
			.getLogger(LogCollectionBolt.class);

	@Override
	public void onPrepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.onPrepare(stormConf, context, collector);
		String restartedInfo = "********************************\n";
		restartedInfo += "*******System Restartd again****\n";
		restartedInfo += "********************************\n";
		// super.addStatus(ManageBoltUtil.LOG_BOLT_KEY_NAME, restartedInfo);
	}

	public void onLogicDataArrive(Tuple tuple) {
		String time = ExceptionUtil.sdf.format(new Date());
		// if(tuple.getSourceStreamId().equals(FunctionBoltUtil.LOG_BOLT_STREAM_NAME)){
		logger.info(tuple.getString(2));
		super.addStatus(ManageBoltUtil.LOG_BOLT_KEY_NAME,
				time + " : " + tuple.getString(2) + "\n");
	};
}
