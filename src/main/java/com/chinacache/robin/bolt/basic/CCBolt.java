package com.chinacache.robin.bolt.basic;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.chinacache.robin.util.config.ManageBoltUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * Base class for all bolt
 *
 */
public abstract class CCBolt extends BaseRichBolt implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(CCBolt.class);
	
	private Integer exceptionNum = 1;
	
	protected OutputCollector collector;

	/**
	 * Don't override this method again. You can override onPrepare to achieve the
	 * same efficiency.
	 * 
	 * Override this method will lead to control message lose efficiency.
	 * Override this method will lead to exception control lose efficiency.
	 * Override this method will lead to statistical bolt lose efficiency.
	 * Override this method will lead to status management lose efficiency.
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		onPrepare(stormConf, context, collector);
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
	@Override
	public void execute(Tuple tuple) {
		Boolean isControl = true;
		try {
			isControl = onControlDataArrive(tuple);
		} catch (Exception e) {
			exceptionNum++;
			e.printStackTrace();
		}
		if (isControl == null || isControl)
			return;
		try {
			onLogicDataArrive(tuple);
			collector.ack(tuple);
		} catch (Exception e) {
			e.printStackTrace();
			exceptionNum++;
			sendToExceptionBolt(new Values(this.getClass().getName(), e),
					collector);
			collector.fail(tuple);
		}
		//collector.ack(tuple);
		if (exceptionNum % 1000 == 0)
			logger.info("expeption num :" + exceptionNum);
	}

	@Override
	public void cleanup() {
		super.cleanup();
		onClose();
	}

	public boolean sendToLogBolt(Values values, OutputCollector collector) {
		if (!ManageBoltUtil.useManagementBolt)
			return false;
		if (values.size() < 2) {
			logger.error("sended message to logcolletor Num Error");
			return false;
		}
		Values vs = new Values(ManageBoltUtil.LOG_BOLT_STREAM_NAME,
				values.get(0), values.get(1));
		if (collector != null)
			collector.emit(ManageBoltUtil.LOG_BOLT_STREAM_NAME, vs);
		return true;
	}

	public boolean sendToExceptionBolt(Values values, OutputCollector collector) {
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

	public boolean sendToStatsiticBolt(Values values, OutputCollector collector) {
		if (!ManageBoltUtil.useManagementBolt)
			return false;
		if (values.size() < 3) {
			logger.error("sended message to stcollector Num Error");
			return false;
		}
		if (collector != null)
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

	public abstract void onClose();

	public abstract void declareCCOutputFields(OutputFieldsDeclarer declarer);

	public abstract void onLogicDataArrive(Tuple input);

	public abstract Boolean onControlDataArrive(Tuple input);

	public abstract void onPrepare(Map stormConf, TopologyContext context,
			OutputCollector collector);

}
