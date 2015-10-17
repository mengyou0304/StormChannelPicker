package com.chinacache.robin.bolt.kafkaspout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

//import com.chinacache.stormkafka.BrokerHosts;
//import com.chinacache.stormkafka.KafkaSpout;
//import com.chinacache.stormkafka.SpoutConfig;
//import com.chinacache.stormkafka.StringScheme;
//import com.chinacache.stormkafka.ZkHosts;

public class CCKafkaSpoutUtil {
    private static HashMap<String, String> map;

    private static boolean inited = false;

    private static final Logger logger = LoggerFactory
            .getLogger(CCKafkaSpoutUtil.class);

    public static void init(HashMap<String, String> confmap) {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        map = confmap;
        inited = true;
    }

//    public static KafkaSpout getKafkaSpoutCNC() throws Exception {
//        if (!inited)
//            throw new RuntimeException("Config of Kafka spout is not inited");
//        BrokerHosts zkHostsCnc = new ZkHosts(
//                map.get(KafkaSpoutConfConstants.BROKER_ZK_STR),
//                map.get(KafkaSpoutConfConstants.BROKER_ZK_PATH_CNC));
//
//        SpoutConfig spoutConfigCnc = new SpoutConfig(zkHostsCnc,
//                map.get(KafkaSpoutConfConstants.TOPIC_NAME), "",
//                map.get(KafkaSpoutConfConstants.CONSUMER_OFFSET_PATH_CNC));
//        spoutConfigCnc.stateUpdateIntervalMs = 4000000L;
//        spoutConfigCnc.scheme = new SchemeAsMultiScheme(new StringScheme());
//        return new KafkaSpout(spoutConfigCnc);
//
//    }
//
//    public static KafkaSpout getKafkaSpoutCHN() {
//        if (!inited)
//            throw new RuntimeException("Config of Kafka spout is not inited");
//        BrokerHosts zkHostsChn = new ZkHosts(
//                map.get(KafkaSpoutConfConstants.BROKER_ZK_STR),
//                map.get(KafkaSpoutConfConstants.BROKER_ZK_PATH_CHN));
//
//        SpoutConfig spoutConfigChn = new SpoutConfig(zkHostsChn,
//                map.get(KafkaSpoutConfConstants.TOPIC_NAME), "",
//                map.get(KafkaSpoutConfConstants.CONSUMER_OFFSET_PATH_CHN));
//
//        spoutConfigChn.stateUpdateIntervalMs = 40000000L;
//        spoutConfigChn.scheme = new SchemeAsMultiScheme(new StringScheme());
//        return new KafkaSpout(spoutConfigChn);
//    }

    public static HighLevelKafkaSpout getHighLevelKafkaSpoutCHN() {
        if (!inited)
            throw new RuntimeException("Config of Kafka spout is not inited");

        HighLevelKafkaSpout spout = new HighLevelKafkaSpout();
        String zkstring = map.get(KafkaSpoutConfConstants.ZK_CONNECTION_STRING)
                + map.get(KafkaSpoutConfConstants.CHN_CONNECTION_STRING);
        String topic = map.get(KafkaSpoutConfConstants.TOPIC_NAME);
        String group = map.get(KafkaSpoutConfConstants.GROUP_ID);

        logger.info("CHN Configure : zkstring={}, topic={}, group= {}",
                zkstring, topic, group);

        spout.setConfigures(zkstring, topic, group);
        return spout;
    }

    public static HighLevelKafkaSpout getHighLevelKafkaSpoutCNC() {
        if (!inited)
            throw new RuntimeException("Config of Kafka spout is not inited");

        HighLevelKafkaSpout spout = new HighLevelKafkaSpout();
        String zkstring = map.get(KafkaSpoutConfConstants.ZK_CONNECTION_STRING)
                + map.get(KafkaSpoutConfConstants.CNC_CONNECTION_STRING);
        String topic = map.get(KafkaSpoutConfConstants.TOPIC_NAME);
        String group = map.get(KafkaSpoutConfConstants.GROUP_ID);

        logger.info("CNC Configure : zkstring={}, topic={}, group= {}",
                zkstring, topic, group);

        spout.setConfigures(zkstring, topic, group);
        return spout;
    }

    public static void main(String[] args) {
        // CCKafkaSpoutUtil.init(new HashMap<String, String>());
        // KafkaSpout sp1=CCKafkaSpoutUtil.getKafkaSpoutCHN();
        // KafkaSpout sp2=CCKafkaSpoutUtil.getKafkaSpoutCNC();
        // builder.setSpout("kafka-cnc", sp2, Integer.valueOf(spoutCount));
        // builder.setSpout("kafka-chn", sp1, Integer.valueOf(spoutCount));
    }

}
