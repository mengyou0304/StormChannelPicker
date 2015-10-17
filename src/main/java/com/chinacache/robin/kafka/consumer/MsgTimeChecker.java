/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by robinmac on 15-9-23.
 */
public class MsgTimeChecker {
	private static final Logger logger = LoggerFactory
		.getLogger(MsgTimeChecker.class);
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	static HashSet<MsgTimeChecker> checkers=new HashSet<MsgTimeChecker>();
	public static final int MAX_DELAY_MINNUTES = 10;
	////*******System Vars********
	String topicName;
	Integer partition;
	String part;

	////*******Statistic Vars********
	Long deadline = null;
	Long deadlineInSec = null;

	String lastStart = null;
	Long allMessageNum = 0l;
	Long lagMessageNum=0l;
	Long lagMsgNumPlus3=0l;
	Long lagMsgNumPlus6=0l;
	Integer p1=5;
	Integer p2=10;

	private long deadlineInSecPlus3Min=0l;
	private long deadlineInSecMinus6Min=0l;
	HashMap<String,Integer> lateMap=new HashMap<String,Integer>();

	public MsgTimeChecker(String a_topic, int a_partition, String part) {
		this.topicName=a_topic;
		this.partition=a_partition;
		this.part=part;
		checkers.add(this);
	}

	public void checkTime(String log) {
		if (allMessageNum % 50000 == 0) {
//			logger.info("Message num as: " + allMessageNum / 10000 + "W");
			logger.info(getStatusStirng());
			if (allMessageNum % 500000 == 0)
				checkDeadLine();
		}
		allMessageNum++;
		if (lastStart != null && log.contains(lastStart))
			return;
		if (log.length() < 27)
			return;
		lastStart = log.substring(0, 27);
		String timestring = log.substring(13, 27);
		String hostid = log.substring(2, 12);
		Long timeInSec = 0l;
		Long time = 0l;
		try {
			timeInSec = Long.parseLong(timestring.substring(0, 10));
			time = timeInSec * 1000l;
		} catch (Exception e) {
			return;
		}
		if (timeInSec < deadlineInSec) {
//			System.out.println("old message: " + sdf.format(new Date(time)) + " ~" + hostid);
			lagMessageNum++;
		}
		if (timeInSec < deadlineInSecPlus3Min) {
			lagMsgNumPlus3++;
		}
		if (timeInSec < deadlineInSecMinus6Min) {
			if(!lateMap.containsKey(hostid)) {
				logger.warn("Quit Old Message: " + sdf.format(new Date(time)) + " ~" + hostid);
			}
			Integer num=lateMap.get(hostid);
			if(num==null)
				num=0;
			lateMap.put(hostid,num+1);
			lagMsgNumPlus6++;
		}
	}

	private void checkDeadLine() {
		deadline = new Date().getTime();
		deadline = deadline - 1000 * 60 * MAX_DELAY_MINNUTES;
//		logger.debug("Set Dead line as: " + sdf.format(new Date(deadline)));
		deadlineInSec = deadline / 1000;
		deadlineInSecPlus3Min=deadlineInSec-p1*60;
		deadlineInSecMinus6Min=deadlineInSec-p2*60;
	}

	public String getFront(){
		String s="";
		s+=part+"/"+topicName+"/"+partition;
		return s;
	}
	public String getStatusStirng(){
		String s="";
		Double v0=100d*lagMessageNum/(allMessageNum+1);
		Double v3=100d*lagMsgNumPlus3/(allMessageNum+1);
		Double v6=100d*lagMsgNumPlus6/(allMessageNum+1);

		String res0=String.format("%.2f",v0);
		String res3=String.format("%.2f",v3);
		String res6=String.format("%.2f",v6);
		s+="["+getFront()+"][Allmsg]: "+this.allMessageNum+"\t"+"[LagMsg]: "+lagMessageNum+"\t"+"[LagPercent "+MAX_DELAY_MINNUTES+":"+(MAX_DELAY_MINNUTES+p1)+":"+(MAX_DELAY_MINNUTES+p2)+"]:"+res0+"%,"+res3+"%,"+res6+"%(host:"+lateMap+")\n";
		return s;
	}

}
