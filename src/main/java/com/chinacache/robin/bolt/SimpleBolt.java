package com.chinacache.robin.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SimpleBolt extends BaseRichBolt{
	private static final Logger logger = LoggerFactory.getLogger(SimpleBolt.class);
	OutputCollector tc;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		tc=collector;
	}

	@Override
	public void execute(Tuple input) {
		tc.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
