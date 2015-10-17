///*
// * Copyright (c) 2015.
// * @author You.Meng
// */
//
//package com.chinacache.robin.logic;
//
////import com.chinacache.log.common.config.FAConstants;
////import com.chinacache.log.common.config.LogType;
////import com.chinacache.log.common.service.LogLineService;
////import com.chinacache.log.common.service.NlaPickerService;
////import com.chinacache.log.common.service.builder.LogEntityBuilderFactory;
//
//
//import com.chinacache.loganalyse.model.LogType;
//import com.chinacache.loganalyse.pick.service.LogPickerBuilderFactory;
//import com.chinacache.loganalyse.pick.service.builder.LogEntityBuilder;
//import com.chinacache.loganalyse.service.LineServicePick;
//import org.apache.commons.lang3.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.Serializable;
//import java.util.Map;
//
//public class NLAKeyGenerator2 extends NLAKeyGenerator implements Serializable {
//
//	private static final long serialVersionUID = 1L;
//	private static final Logger logger = LoggerFactory
//		.getLogger(NLAKeyGenerator2.class);
//
//
//	private LogEntityBuilder logEntityBuilder;
//
//	private long errorCount = 0;
//
//
//	private long logNums = 0;
//
//
//	public void init() {
//		logEntityBuilder = LogPickerBuilderFactory.logEntityBuilder;
//	}
//
//	public NLAResult execute(String messageLine) {
//		logNums++;
//		String kafkaMessage = null;
//		try {
//			kafkaMessage =messageLine;
//
//			String[] message = StringUtils.split(kafkaMessage, "\t", 2);
//			if (message.length < 2)
//				return null;
//
//			if (!message[0].matches(FAConstants.LOG_TYPE_REGEX))
//				return null;
//
//			String deviceId = null;
//
//
//			// Step2: 获取日志格式
//			LogType type = logEntityBuilder.getLogType(message[0], deviceId);
//
//			try {
//				Map<String, String> map = LineServicePick.process(logEntityBuilder, StringUtils.substringAfter(message[1], " "),
//					type);
//				if (null != map) {
////					collector.emit(new Values(type.toString(), map.get("userId"), map.get("channelId"), kafkaMessage));
//					return new NLAResult(type.toString(), map.get("userId"), map.get("channelId"), kafkaMessage);
//				}
//
//			} catch (Exception e) {
//				errorCount++;
//				if (errorCount % 1000 == 0) {
//					logger.error("log message parse error, count: " + errorCount);
//					errorCount = 0;
//				}
//			}
//		} catch (Exception e) {
//			logger.error("log message parse error:" + kafkaMessage);
//		}
//		return null;
//	}
//
//	public int getPartitionFromSN(String sn) {
//		if (sn == null)
//			return 0;
//		else if (sn.trim().length() == 0)
//			return 1;
//		else {
//			return 2 + (sn.trim().hashCode() & 0xF);
//		}
//	}
//
//	public static void main(String[] args) {
//		NLAKeyGenerator2 gen = new NLAKeyGenerator2();
//		String[] sns = {"010021H3dT", "01001743S3"};
//		for (String sn : sns) {
//			System.out.println(sn + ":" + gen.getPartitionFromSN(sn));
//		}
//	}
//}
