//package com.chinacache.robin.kafka.producer.async;
//
//
//import com.chinacache.robin.util.LocalFileReader;
//import com.chinacache.robin.util.basic.LineProcess;
//import com.chinacache.robin.util.config.AllConfiguration;
//import org.apache.hadoop.fs.Path;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.KafkaException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.FileNotFoundException;
//import java.util.Properties;
//import java.util.Timer;
//import java.util.TimerTask;
//
///**
// *
// *
// * The basic idea of CCKProducer is batch send and batch resend.
// *
// * In order to avoid random seek offset, we use BitAcker to record the failed message and timely resend
// * the message in batch.
// *
// * Each message will be tried for three times before being abandoned.
// *
// * Created by robinmac on 15-8-19.
// *
// */
//
//public class FilePersisitProducer {
//	private static final Logger logger = LoggerFactory
//		.getLogger(FilePersisitProducer.class);
//	private static Long nums = 0l;
//	private KafkaProducer<String, String> producer;
//	private Properties configurations;
//
//	private static FilePersisitProducer instance;
//
//	public static FilePersisitProducer getInstance(String innerKafkaBroker){
//		if(instance==null)
//			instance=new FilePersisitProducer(innerKafkaBroker);
//		return instance;
//	}
//
//	private FilePersisitProducer(String innerKafkaBroker) {
//		this.configurations = initParams(innerKafkaBroker);
//	}
//
//
//	private Properties initParams(String kafkaBroker) {
//		Properties props = new Properties();
//		props.put("bootstrap.servers", kafkaBroker);
//		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		//		props.put("partitioner.class", "com.robin.kafka.ConfigBasedPartitioner")
//		props.put("batch.size", "16384");
//		int k = 1024 * 1024 * 8;
//		props.put("send.buffer.bytes", String.valueOf(k));
//		//async only
//		return props;
//	}
//
//	public void buildConnection() {
//		producer = new KafkaProducer<String, String>(configurations);
//	}
//
//	public void sendFile(final Path localFile, final String topicID, final String key) {
//		String localURL = localFile.toString();
//		logger.info("Starting Sending file of " + localURL);
//		LocalFileReader filereader = new LocalFileReader();
//		try {
//			filereader.readM1(localURL, new LineProcess() {
//				@Override
//				public void onEachFileLine(String line, Integer linenum, Long offset) {
//					basicSend(topicID, key, line, linenum, offset, localFile.getName());
//					if(linenum%10000==0)
//						logger.debug("sending infos as num: "+linenum/1000+"W");
//				}
//			});
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//	}
//
//	/**
//	 *
//	 *
//	 * @param topicID the topic should send
//	 * @param key the kafka key in topic
//	 * @param msg the kafka message
//	 * @param lineNum   the lineNum of the the file. It is used for ack to check whether all lines of a file has been acked
//	 * @param offset   the offset of a line in the file. It is used for resending the line.
//	 * @param filename  the currently sending file
//	 */
//	public void basicSend(String topicID, String key, String msg, Integer lineNum, Long offset, String filename) {
//		producer.send(new ProducerRecord<String, String>(topicID, key, msg),
//			new KafkaCallBack(key,lineNum, offset, filename));
//		if(lineNum%10000==0) {
//			BitAcker acker = BitAcker.ackMap.get(filename);
//			if(acker==null){
//				acker=new BitAcker(filename,lineNum + 10000);
//				BitAcker.ackMap.put(filename,acker);
//			}
//			acker.setMaxLineNum(lineNum + 10000);
//		}
//	}
//
//	public void close() {
//		try {
//			producer.close();
//		} catch (KafkaException e) {
//			e.printStackTrace();
//		}
//	}
//
//
//	public static void main(String[] args) {
//		FilePersisitProducer cp = new FilePersisitProducer(AllConfiguration.KAFKA_INNER_BROKER);
//		cp.buildConnection();
//		String url="/Application/nla/log_pick/conf/test/readedfile";
//		String url2="/Application/nla/log_pick/conf/test/size.out";
//		final String url3="/Application/nla/log_pick/conf/test/testfile";
//		final String usingURL=url2;
//		cp.sendFile(new Path(usingURL), "testtopic300", "fileKey");
//		Timer t=new Timer();
//		TimerTask task=new TimerTask() {
//			@Override
//			public void run() {
//				BitAcker acker=BitAcker.ackMap.get(new Path(usingURL).getName());
//				if(acker!=null)
//					acker.debugFailedLines();
//				else
//					logger.debug("Error in finding the file of "+usingURL);
//			}
//		};
//		t.schedule(task,0,1000);
//	}
//}
//
//
