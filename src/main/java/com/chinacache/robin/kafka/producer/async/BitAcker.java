/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.kafka.producer.async;

import com.chinacache.robin.kafka.AckCannotClearException;
import com.chinacache.robin.util.config.AllConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * It is used for check if a files all lines has been acked, each line is present as one bit.
 * There won't be any racing condition so we don't use any lock on it.
 *
 * Created by robinmac on 15-8-19.
 */
public class BitAcker {
	private static final Logger logger = LoggerFactory
		.getLogger(BitAcker.class);
	/**
	 * Properties of each bitAcker
	 */
	int[] ackerInfos;
	private String key;
	private Integer maxLineNum=Integer.MAX_VALUE;
	Long initTime;
	Integer lastNotAckedNum =-1;

	/**
	 * Properties of the Only BitAcker
	 */
	public static HashMap<String, BitAcker> ackMap = new HashMap<String, BitAcker>();
	private static Timer timer;

	/**
	 * Selfcheck is used for checking the status of all the bitack in ackMap.
	 * A timeout seconds is used for checking the message sent to kafka.
	 *
	 */
	public void startSelfCheckerThread(){
		timer=new Timer();
		TimerTask timertask=new TimerTask() {
			@Override
			public void run() {
				Set<String> keyset=ackMap.keySet();
				Long time=System.currentTimeMillis();
				for(String key:keyset){
					BitAcker acker=ackMap.get(key);
					//low level lock which only lock by object
					synchronized (acker) {
						if (acker == null) {
							ackMap.remove(key);
							continue;
						}
						Integer notAckedNum=getFailedLines().size();
						//if the time exceed the max timeout and no message back again,
						// we trigger the resend process
						if((notAckedNum== lastNotAckedNum)&&(time-initTime)> AllConfiguration.KAFKA_PRODUCER_MESSAGE_TIMEOUT_SECONEDS)
							acker.trigerResend();
						lastNotAckedNum =notAckedNum;
						if(lastNotAckedNum ==0)
							acker.clearSelf();
					}
				}
			}
		};
		timer.schedule(timertask,0,60*1000l);
	}

	private void trigerResend() {
		ArrayList<Integer> failedLines=getFailedLines();

	}


	public void clearSelf() throws AckCannotClearException {
		if(debugFailedLines()!=0)
			throw new AckCannotClearException("The Ack can't be cleared due to: It's ack has not been all cleared");
		ackerInfos=null;
		key=null;
		maxLineNum=null;
		synchronized(this) {
			//Killing myself e....e....e......
			ackMap.remove(key);
		}
	}

	public BitAcker(String key,int lineNum) {
		this(lineNum);
		this.key=key;
		initTime=System.currentTimeMillis();
	}

	public BitAcker(int lineNum) {
		int arrayLength = lineNum / 32 + 1;
		ackerInfos = new int[arrayLength];
		for (int i = 0; i < arrayLength; i++)
			ackerInfos[i] = 0xFFFFFFFF;
	}

	public void ack(int lineNum) {
		int position = lineNum / 32;
		int offset = lineNum % 32;
		int mask=((1 << offset) ^ 0xFFFFFFFF);
//		logger.debug("find position="+position+"  OFFSET="+offset+" mask="+toBit(mask));
//		logger.debug("before operation: "+toBit(ackerInfos[position]));
		ackerInfos[position] = ackerInfos[position] & mask;
//		logger.debug("after operation:  "+toBit(ackerInfos[position]));
	}

	public ArrayList<Integer> getFailedLines() {
		ArrayList<Integer> lines = new ArrayList<Integer>();
		for (int i = 0; i < ackerInfos.length; i++) {
			if (ackerInfos[i] == 0)
				continue;
			if(i*32>maxLineNum)
				break;
			for (int k = 0; k < 32; k++)
				if (((1 << k) & ackerInfos[i]) != 0)
					lines.add(i * 32 + k);
		}
		return lines;
	}
	public int debugFailedLines(){
		ArrayList<Integer> list=getFailedLines();
		logger.debug("failed linenum"+list.size());
		return list.size();
	}

	public static String toBit(Integer v) {
		StringBuffer sb = new StringBuffer();
		for (int i = 31; i >= 0; i--) {
			if ((v & (1 << i)) != 0)
				sb.append(1);
			else sb.append(0);
		}
		return sb.toString();
	}

	public void setMaxLineNum(Integer maxLineNum) {
		this.maxLineNum = maxLineNum;
	}

	private void showAllStorage(){
		logger.debug("================================");
		logger.debug("=======Storage of BigAcker======");
		logger.debug("================================");
		for(int i= 0; i < ackerInfos.length;i++)
			logger.debug(toBit(ackerInfos[i]));
		logger.debug("================================");
	}

	public static void main(String[] args) {
		int length = 129;
		BitAcker ba = new BitAcker(length);
		ba.setMaxLineNum(length+1);
		for(int i=0;i<=length;i++)
			ba.ack(i);
		ba.showAllStorage();
		ArrayList<Integer> failedList = ba.getFailedLines();
		for (int i : failedList)
			System.out.println("fail at line: " + i);
	}
}


