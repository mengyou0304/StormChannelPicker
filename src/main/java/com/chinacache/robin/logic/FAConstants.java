/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.logic;


/**
 * system.properties配置文件
 */
public final class FAConstants extends ConfigurableContants {

	/**
	 * zookeeper： just ip:port
	 */
	public final static String BROKER_ZK_STR = getProperty("broker.zk.str");

	/**
	 * 电信、联通kafka集群存储所有topic、partition信息的路径。Default is /brokers.
	 */
	public final static String BROKER_ZK_PATH_CNC = getProperty("broker.zk.path.cnc");

	public final static String BROKER_ZK_PATH_CHN = getProperty("broker.zk.path.chn");

	/**
	 * store your consumer's offset 电信、联通kafka集群存储路径不同
	 */
	public final static String CONSUMER_OFFSET_PATH_CNC = getProperty("consumer.offset.path.cnc");

	public final static String CONSUMER_OFFSET_PATH_CHN = getProperty("consumer.offset.path.chn");

	/**
	 * consumer topic's name
	 */
	public final static String TOPIC_NAME = getProperty("topic.name");


	/**
	 * kafka group id
	 */
	public static String KAFKA_GROUP_ID = getProperty("kafka.group.id");

	/**
	 * 日志统计最大个数
	 */
	public final static long LOG_STAT_MAX = Long.parseLong(getProperty("log.stat.max"));

	/**
	 * Redis批量入库条数
	 */
	public final static int REDIS_BARCH_SIZE = 100;//Integer.parseInt(getProperty("redis.barch.size","10000"));

	/**
	 * Hbase批量入库条数
	 */
	public final static int HBASE_BARCH_SIZE = 5000;//Integer.parseInt(getProperty("redis.barch.size","10000"));

	/**
	 * 入库周期，默认5秒入库一次
	 */
	public final static int STORAGE_INTERVAL = 5 * 1000;//Integer.parseInt(getProperty("redis.barch.size","10000"));

	/**
	 * redis key的分隔符
	 */
	public final static String REDIS_KEY_SEPARTOR = ":";

	/**
	 * hbase rowkey的分隔符
	 */
	//public final static String HBASE_KEY_SEPARTOR = "-";

	/**
	 * HTable pool的最大连接数
	 */
	//public static int HTABLE_MAXCONNECT = Integer.parseInt(getProperty("htable_maxconnect"));

	public static String MASTER = "";

	public static String ZOOKEEPER = "";

	public static String HDFS = "";

	/**
	 * 默认列族名称，目前只有一个列族
	 */
	public static byte[] HBASE_COLUMNFAMILY = toBytes(getProperty("hbase.default.columnfamily"));

	/**
	 * Hbase表名之间的分隔符
	 */
	public static final String HBASETABLE_SEPARTOR = "_";

	/**
	 * 入库失败数据回流进kafka的topic
	 */
	public static final String STORAGE_FAIL_TOPIC = getProperty("storage.fail.topic.name", "logstat-storage-fail");


	/**
	 * 配置文件更新周期 小时
	 * 注意：不得大于24
	 */
	public static final int CONFIG_UPDATE_PERIOD = Integer.parseInt(getProperty("config.update.period", "3"));

	/**
	 * logpick
	 */
	public static final int LOGPICK_NUM_MAX = Integer.parseInt(getProperty("logpick.num.max", "100000"));

	/**
	 * loganalyse
	 */
	public static final int LOGANALYSE_NUM_MAX = Integer.parseInt(getProperty("loganalyse.num.max", "50000"));

	/**
	 * 日志格式正则过滤
	 */
	public final static String LOG_TYPE_REGEX = getProperty("log.type.regex", "[A-Z]");
	public static byte[] toBytes(String str) {
		return null != str?str.getBytes():"".getBytes();
	}



}