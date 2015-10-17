package com.chinacache.robin.bolt.kafkaspout;

/**
 * system.properties配置文件
 */
public final class KafkaSpoutConfConstants {

	/**
	 * zookeeper： just ip:port
	 */
	public final static String BROKER_ZK_STR = "broker.zk.str";

	/**
	 * 电信、联通kafka集群存储所有topic、partition信息的路径。Default is /brokers.
	 */
	public final static String BROKER_ZK_PATH_CNC = "broker.zk.path.cnc";

	public final static String BROKER_ZK_PATH_CHN = "broker.zk.path.chn";

	/**
	 * store your consumer's offset 电信、联通kafka集群存储路径不同
	 */
	public final static String CONSUMER_OFFSET_PATH_CNC = "consumer.offset.path.cnc";

	public final static String CONSUMER_OFFSET_PATH_CHN = "consumer.offset.path.chn";

	/**
	 * consumer topic's name
	 */
	public final static String TOPIC_NAME = "topic.name";

	// public final static String TOPOLOGY_NAME = "topology.name";

	public final static String CHANNEL_DISTRIBUTE_CONF = "channel_distribute_conf";

	public final static String LOG_CHANNEL_CONF = "log_channel_conf";
	/********* Level2 *************************/

	public final static String GROUP_ID = "group.id";
	public final static String CNC_CONNECTION_STRING = "connection.string.cnc";
	public final static String CHN_CONNECTION_STRING = "connection.string.chn";
	public final static String ZK_CONNECTION_STRING = "connection.string.zk";

	/** HDFS 根路径 */
	public final static String HDFS_PATH = "hdfs.path";

}