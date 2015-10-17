package com.chinacache.robin.main;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.chinacache.robin.bolt.CCBoltGenerator;
import com.chinacache.robin.bolt.KeyGenerationBolt;
import com.chinacache.robin.bolt.MultiFileHDFSBolt;
import com.chinacache.robin.bolt.kafkaspout.CCKafkaSpoutUtil;
import com.chinacache.robin.bolt.kafkaspout.HighLevelKafkaSpout;
import com.chinacache.robin.bolt.managebolt.ExceptionCollectionBolt;
import com.chinacache.robin.bolt.managebolt.LogCollectionBolt;
import com.chinacache.robin.bolt.managebolt.SatasticCollectionBolt;
import com.chinacache.robin.grouping.CCGrouping;
import com.chinacache.robin.util.FileUtility;
import com.chinacache.robin.util.config.AllConfiguration;
import com.chinacache.robin.util.config.ConfigFileManager;
import com.chinacache.robin.util.config.Locator;
import com.chinacache.robin.util.config.ManageBoltUtil;
import com.chinacache.robin.util.hdfs.HDFSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;

public class StormComponentGenerator implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory.getLogger(StormComponentGenerator.class);
	
	private Config config;
	
	private HashMap<String, String> confMap;

	public StormComponentGenerator() {

	}

	/**
	 * Init configurations
	 */
	public void init(boolean uselocal) {
		ManageBoltUtil.initConfig();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		String localConfFile = Locator.getInstance().getNLABaseLocation()
				+ AllConfiguration.STORM_CONF_FILE_NAME;
		try {
			ConfigFileManager.downloadConf();
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		HashMap<String, String> map = FileUtility
				.getConfigurations(localConfFile);
		confMap = map;

		// Configuration related
		config = HDFSUtility.getConfig(map);
		config.put(Config.TOPOLOGY_DEBUG, false);
		config.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
		config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 8192);
		config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 8192);
		config.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
		config.put("topology.max.spout.pending", 204800);
		// config.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 64 * 1024);
		// config.put("nimbus.task.timeout.secs", 120);
		config.put("topology.sleep.spout.wait.strategy.time.ms", 5000);
		config.put("topology.worker.receiver.thread.count", 2);
		config.put("topology.worker.shared.thread.pool.size", 6);
	}

	/**
	 * Init spout Related
	 * 
	 * @param builder
	 */
	public void initSpout(TopologyBuilder builder) {
		CCKafkaSpoutUtil.init(confMap);

		int cncSpoutNum = Integer.parseInt(String.valueOf(config
				.get("spout.cnc.num")));
		int chnSpoutNum = Integer.parseInt(String.valueOf(config
				.get("spout.chn.num")));
		int tupleNumPerTime = Integer.parseInt(String.valueOf(config
				.get("spout.tuple.per.time")));
		logger.info("Kafka Spout Number:" + (cncSpoutNum + chnSpoutNum));

		HighLevelKafkaSpout chnSpout = CCKafkaSpoutUtil
				.getHighLevelKafkaSpoutCHN();
		HighLevelKafkaSpout cncSpout = CCKafkaSpoutUtil
				.getHighLevelKafkaSpoutCNC();
		cncSpout.setTupleNumPerTime(tupleNumPerTime);
		chnSpout.setTupleNumPerTime(tupleNumPerTime);

		builder.setSpout("kspout-cnc", cncSpout, cncSpoutNum);
		builder.setSpout("kspout-chn", chnSpout, chnSpoutNum);
	}

	/**
	 * Init logic bolt related
	 * 
	 * @param builder
	 */
	public void initLogicBolt(TopologyBuilder builder) {
		Integer boltNum = Integer.parseInt(String.valueOf(config
				.get("logic_bolt.num")));
		logger.info("Use  Logic MODEL......... boltNum:" + boltNum);

		KeyGenerationBolt gkb = new KeyGenerationBolt(true, AllConfiguration.GROUPING_LOCAL_URL);
		builder.setBolt("split", gkb, boltNum).shuffleGrouping("kspout-cnc")
				.shuffleGrouping("kspout-chn");
	}

	/**
	 * Init hdfsBolt Related
	 * 
	 * @param builder
	 * @throws IOException
	 */
	public Integer initHFDSBolt(TopologyBuilder builder) throws IOException {
		Integer hdfsboltNum = Integer.parseInt(String.valueOf(config
				.get("hdfs_bolt.num")));
		logger.info("Use  HDFS Bolt ......... boltNum:" + hdfsboltNum);
		MultiFileHDFSBolt mfbolt = CCBoltGenerator.getMultifileBolt(config);
		mfbolt.setWriteData(true);
		if ("true".equals(String.valueOf(config.get("grouping.custom")))) {
			System.out.println("Using CCGrouping......");
			builder.setBolt("hdfsbolt", mfbolt, hdfsboltNum).customGrouping(
					"split", new CCGrouping());
		} else {
			builder.setBolt("hdfsbolt", mfbolt, hdfsboltNum).shuffleGrouping(
					"split");
			System.out.println("Using Shuffle Grouping......");
		}
		return hdfsboltNum;
	}

	public void startTopology(TopologyBuilder builder)
			throws AlreadyAliveException, InvalidTopologyException,
			InterruptedException {
		logger.info("Using Distributed Running model");

		config.setNumWorkers(Integer.parseInt(String.valueOf(config
				.get("alltaskNum"))));

		int ackerNum = Integer.parseInt(String.valueOf(config
				.get("acker.num")));
		config.setNumAckers(ackerNum);

		StormSubmitter.submitTopology("Storm-Intest"+new Date().getTime(), config,
				builder.createTopology());
	}

	/**
	 * Add additional bolt
	 * 
	 * @param builder
	 * @throws IOException
	 */
	public void addAdditionalBolt(TopologyBuilder builder) throws IOException {
		LogCollectionBolt lcbolt = CCBoltGenerator.getLogCollectionBolt(config);
		builder.setBolt("logcollector", lcbolt, 1)
				.globalGrouping("split", ManageBoltUtil.LOG_BOLT_STREAM_NAME)
				.globalGrouping("hdfsbolt", ManageBoltUtil.LOG_BOLT_STREAM_NAME);

		ExceptionCollectionBolt exbolt = CCBoltGenerator
				.getExceptionCollectionBolt(config);
		builder.setBolt("excollector", exbolt, 1)
				.globalGrouping("split",
						ManageBoltUtil.EXCEPTION_BOLT_STREAM_NAME)
				.globalGrouping("hdfsbolt",
						ManageBoltUtil.EXCEPTION_BOLT_STREAM_NAME);

		SatasticCollectionBolt staticbolt = CCBoltGenerator
				.getStatisticCollectionBolt(config);
		builder.setBolt("stcollector", staticbolt, 1)
				.globalGrouping("split",
						ManageBoltUtil.STATISTIC_BOLT_STREAM_NAME)
				.globalGrouping("hdfsbolt",
						ManageBoltUtil.STATISTIC_BOLT_STREAM_NAME);
	}
}
