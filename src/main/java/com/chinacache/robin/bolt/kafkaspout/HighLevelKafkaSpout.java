package com.chinacache.robin.bolt.kafkaspout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import kafka.consumer.ConsumerIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chinacache.robin.util.config.ManageBoltUtil;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class HighLevelKafkaSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory
			.getLogger(HighLevelKafkaSpout.class);

	private SpoutOutputCollector collector;

	int tupleNumPerTime = 1000;

	private long totalMessageNum = 0L;
	private long countInTenThousand = 0L; // how many 10000 tuples has have been
											// sent
	private long elapseTime = 0L;
	private long lastTime = 0L;
	private long curTime = 0L;

	private String zkstring = null;
	private String topic = null;
	private String group = null;
	private Integer threadNum = 1;

	private ConsumerIterator<byte[], byte[]> currentConsumer = null;

	public void setConfigures(String zk, String topic, String gourpid) {
		this.zkstring = zk;
		this.topic = topic;
		this.group = gourpid;
	}

	public HighLevelKafkaSpout() {
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		HighlevelConsumer highLevelConsumer = new HighlevelConsumer(zkstring,
				group, topic, threadNum);
		currentConsumer = highLevelConsumer.getItlist().get(0);
		lastTime = System.currentTimeMillis();
	}

	@Override
	public void nextTuple() {
		try {
			while (currentConsumer.hasNext()) {
				totalMessageNum++;
				if (totalMessageNum % 10000 == 0) {
					curTime = System.currentTimeMillis();
					long delta = curTime - lastTime;
					lastTime = curTime;
					elapseTime += delta;
					countInTenThousand++;
					logger.info("1W message for ms: " + delta
							+ "\t avg for 1W:"
							+ (elapseTime / countInTenThousand) + ""
									+ " ws:"
							+ countInTenThousand + " alltime:" + elapseTime);
					Thread.sleep(20);
					break;
				}

				byte[] message = currentConsumer.next().message();
				String str = new String(message);
				List<Object> list = new ArrayList<Object>();
				list.add(str);
				collector.emit(list, str);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		super.close();
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
		collector.emit(new Values(id), id);
	}

	public void setTupleNumPerTime(Integer tupleNumPerTime) {
		this.tupleNumPerTime = tupleNumPerTime;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
		declarer.declareStream(ManageBoltUtil.LOG_BOLT_STREAM_NAME, new Fields(
				"loginfo", "from"));
	}

}