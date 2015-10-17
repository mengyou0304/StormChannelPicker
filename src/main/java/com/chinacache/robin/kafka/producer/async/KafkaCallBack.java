///*
// * Copyright (c) 2015.
// * @author You.Meng
// */
//
//package com.chinacache.robin.kafka.producer.async;
//
//import com.chinacache.robin.util.LocalFileReader;
//import com.chinacache.robin.util.config.AllConfiguration;
//import org.apache.kafka.clients.producer.Callback;
//import org.apache.kafka.clients.producer.RecordMetadata;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * In KafkaCallBack, it stores "fileKey" and "offset" to determine the exactly line position.
// * It also stores the "fileKey" and "lineNums" which can determine whether all lines of a file is transffered.
// *
// * Created by robinmac on 15-8-20.
// *
// */
//class KafkaCallBack implements Callback {
//	private static final Logger logger = LoggerFactory
//		.getLogger(Callback.class);
//	private static final Integer ackerlength = 1 << 25;
//
//	Integer lineNum;
//	Long offset;
//	String fileKey;
//	String key;
//
//	public KafkaCallBack() {
//	}
//
//	public KafkaCallBack(Integer lineNum,Long offset, String fileKey) {
//		if (!BitAcker.ackMap.containsKey(fileKey))
//			BitAcker.ackMap.put(fileKey, new BitAcker(fileKey, ackerlength));
//		this.lineNum = lineNum;
//		this.fileKey = fileKey;
//	}
//
//	public KafkaCallBack( String key, Integer lineNum, Long offset, String filename) {
//		this(lineNum,offset,filename);
//		this.key=key;
//	}
//
//	/**
//	 * A callback method the user can implement to provide asynchronous handling of request completion. This method will
//	 * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
//	 * non-null.
//	 *
//	 * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
//	 *                  occurred.
//	 * @param exception The exception thrown during processing of this record. Null if no error occurred.
//	 */
//	public void onCompletion(RecordMetadata metadata, Exception exception) {
//		if(metadata!=null)
//			BitAcker.ackMap.get(fileKey).ack(lineNum);
//		else {
//			logger.info("Fail at Line " + lineNum);
//			resend(metadata.topic(),lineNum,offset,fileKey);
//		}
//
//	}
//
//	private void resend(String topic,Integer lineNum, Long offset, String fileKey) {
//		String line= LocalFileReader.readLineByOffSet(fileKey,offset);
//		FilePersisitProducer.getInstance(AllConfiguration.KAFKA_INNER_BROKER).basicSend(topic,key,line,lineNum,offset,fileKey);
//	}
//}
