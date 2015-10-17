package com.chinacache.robin.util.config;

/**
 * Created by robinmac on 15-7-27.
 */
public class AllConfiguration {

    public static final String GROUPING_LOCAL_URL= Locator
        .getInstance().getNLABaseLocation() +"ccgroup.conf";

    // public static final String HDFS_URL = "hdfs://10.20.72.217/";
    // public static final String STORM_CONF_FILE_NAME = "storm.conf";

    public static final String HDFS_URL = "hdfs://hadoop-test1/";
    //public static final String HDFS_URL = "hdfs://10.20.73.210/";
    public static final String STORM_CONF_FILE_NAME = "storm_in_test.conf";
    public static final String STORM_CONF_SATASTIC_FILE_NAME = "satastic.channels";


    public static final String ADAM_CONFIG_FILE = "adam_channels.conf";

    public static final String HDFS_CONFIG_URL = HDFS_URL + "conf/";
    public static final String LOCAL_BASE_LOCATION = "/Application/nla/log_pick/conf/";

    public static final String KAFKA_INNER_BROKER ="10.20.73.187:9092,10.20.73.188:9092,10.20.73.189:9092";

    //10 minutes according to experiences
    public static final Long KAFKA_PRODUCER_MESSAGE_TIMEOUT_SECONEDS =1000l*60*10;


    public static final String[] conflist = new String[] {AllConfiguration.ADAM_CONFIG_FILE,
        "devmap.xml", "mobileDevice.txt", "system.properties",
        "ccgroup2.conf", "ccgroup.conf", "loganalyse-system.properties","channels.conf","upper_nodes_of_channel.txt",
        AllConfiguration.STORM_CONF_FILE_NAME,AllConfiguration.STORM_CONF_SATASTIC_FILE_NAME};

    public static String getHdfsDestPath(String day, String userId,
                                         String channel) {
        return String.format("%sBILog/src/%s/%s/%s/", AllConfiguration.HDFS_URL, day, userId,
            channel);
    }
}
