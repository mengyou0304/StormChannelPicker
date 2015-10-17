package com.chinacache.robin.bolt.basic;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.chinacache.robin.util.config.ManageBoltUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * Bolt that extends BaseBasicBolt, whose ack automatically done by storm. For
 * the sake of throughput, currently no ack is used, so this class is
 * deprecated.
 * 
 * @author zexing.hu
 *
 */
public abstract class CBBolt extends BaseBasicBolt implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(CBBolt.class);

	/**
	 * Don't overide this method again. You can overide onPrepare to achieve the
	 * same efficiency.
	 * 
	 * Override this method will lead to control message lose efficiency.
	 * Override this method will lead to exception control lose efficiency.
	 * Override this method will lead to satastical bolt lose efficiency.
	 * Override this method will lead to status managment lose efficiency.
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		ManageBoltUtil.initConfig();
		onPrepare(stormConf, context);
	}

	/**
	 * Don't overide this method again. You can overide onLogicDataArrive to
	 * achieve the same efficiency.
	 * 
	 * Override this method will lead to control message lose efficiency.
	 * Override this method will lead to exception control lose efficiency.
	 * Override this method will lead to satastical bolt lose efficiency.
	 * Override this method will lead to status managment lose efficiency.
	 */
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Boolean isControl = true;
		try {
			isControl = onControlDataArrive(tuple, collector);
		} catch (Exception e) {
			e.printStackTrace();
			// sendToExceptionBolt(new
			// Values(FunctionBoltUtil.EXCEPTION_BOLT_STREAM_NAME,"CCBolt", e),
			// collector);
		}
		if (isControl == null || isControl)
			return;
		try {
			onLogicDataArrive(tuple, collector);
		} catch (Exception e) {
			e.printStackTrace();
			sendToExceptionBolt(new Values(this.getClass().getName(), "CCBolt",
					e), collector);
		}
	};

	// public boolean sendToLogBolt(Values values, BasicOutputCollector
	// collector) {
	// if(!ManageBoltUtil.useManagementBolt)
	// return false;
	// if(values.size()<2){
	// logger.error("sended message to logcolletor Num Error");
	// return false;
	// }
	// Values vs=new
	// Values(ManageBoltUtil.LOG_BOLT_STREAM_NAME,values.get(0),values.get(1));
	// collector.emit(ManageBoltUtil.LOG_BOLT_STREAM_NAME, vs);
	// return true;
	// }

	public boolean sendToExceptionBolt(Values values,
			BasicOutputCollector collector) {
		if (!ManageBoltUtil.useManagementBolt)
			return false;
		if (values.size() < 2) {
			logger.error("sended message to excollector Num Error");
			return true;
		}
		Values vs = new Values(ManageBoltUtil.EXCEPTION_BOLT_STREAM_NAME,
				values.get(0), values.get(1));
		collector.emit(ManageBoltUtil.EXCEPTION_BOLT_STREAM_NAME, vs);
		return true;
	}

	public boolean sendToStatsiticBolt(Values values,
			BasicOutputCollector collector) {
		if (!ManageBoltUtil.useManagementBolt)
			return false;
		if (values.size() < 3) {
			logger.error("sended message to stcollector Num Error");
			return false;
		}
		collector.emit(ManageBoltUtil.STATISTIC_BOLT_STREAM_NAME, values);
		return true;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(ManageBoltUtil.LOG_BOLT_STREAM_NAME, new Fields(
				"type", "key", "info"));
		declarer.declareStream(ManageBoltUtil.EXCEPTION_BOLT_STREAM_NAME,
				new Fields("type", "key", "info"));
		declarer.declareStream(ManageBoltUtil.STATISTIC_BOLT_STREAM_NAME,
				new Fields("type", "key", "info"));
		declarer.declareStream(ManageBoltUtil.STATUS_BOLT_STREAM_NAME,
				new Fields("type", "key", "info"));
		declareCCOutputFields(declarer);
	}

	public abstract void declareCCOutputFields(OutputFieldsDeclarer declarer);

	public abstract void onLogicDataArrive(Tuple input,
			BasicOutputCollector collector);

	public abstract Boolean onControlDataArrive(Tuple input,
			BasicOutputCollector collector);

	public abstract void onPrepare(Map stormConf, TopologyContext context);

}
