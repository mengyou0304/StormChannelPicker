package com.chinacache.robin.bolt.managebolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.chinacache.robin.bolt.MultiFileHDFSBolt;
import com.chinacache.robin.util.basic.ExceptionUtil;
import com.chinacache.robin.util.config.ManageBoltUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ExceptionCollectionBolt extends MultiFileHDFSBolt {
	private static final Logger logger = LoggerFactory
			.getLogger(ExceptionCollectionBolt.class);
	OutputCollector collector;

	@Override
	public void onPrepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		super.onPrepare(stormConf, context, collector);
		String restartedInfo = "********************************\n";
		restartedInfo += "*******System Restartd again****\n";
		restartedInfo += "********************************\n";
		// super.addStatus(ManageBoltUtil.EXCEPTION_BOLT_KEY_NAME,
		// restartedInfo);
	}

	public void onLogicDataArrive(Tuple tuple) {
		List<Object> list = tuple.getValues();
		Exception e = null;
		String key = "";
		if (list.size() == 3) {
			key = String.valueOf(list.get(1));
			try {
				e = (Exception) list.get(2);
			} catch (ClassCastException cce) {
				return;
			}
		}

		String infoline = ExceptionUtil.getExcetpionInfo(e, key);

		System.out.println(infoline);
		logger.info(infoline);
		infoline += "*********************\n\n\n";
		super.addStatus(ManageBoltUtil.EXCEPTION_BOLT_KEY_NAME, infoline);
	};
}