/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.grouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import com.chinacache.robin.grouping.statistic.SatasticGroupingManager;
import com.chinacache.robin.util.basic.ExceptionUtil;
import com.chinacache.robin.util.config.AllConfiguration;
import com.chinacache.robin.util.config.ConfigFileManager;
import com.chinacache.robin.util.config.Locator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * 1. STARTUP: In fact, the System will generate CustomGrouping as the same
 * number as the former bold/spout and call the prepare for each
 * CustomStreamGrouping start up
 *
 * 2. RUNNING: During the running, the output of the former bold/spout will
 * input into the CustomStreamGrouping and call the function chooseTasks.
 *
 * Problems:
 *  1. As grouping should be dispatch to any machine, how to read the
 * grouping configuration？ from HDFS?
 *
 *  2. CCGourping algorithm will dispatch keys
 * according to the booted bolt(In fact as we tested, we can't directly control
 * the bolt num, so this algorithm will automaticly control the bolt num of each
 * key according to their num/AllNum )
 *
 *
 * 
 * @author robin
 * 
 */
public class CCGrouping_st implements CustomStreamGrouping {

	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory
			.getLogger(CCGrouping_st.class);
	
	private WorkerTopologyContext context;
	private GlobalStreamId stream;

	SatasticGroupingManager stManager;
	Random r=new Random();


	long exceptionNum = 1;
	Integer defaultTaskID = -1;
	int fault1 = 1;
	int fault2 = 1;
	int fault3 = 1;
	int fault4 = 1;
	int messageNum = 1;

	/**
	 * Are called when first build the grouping
	 */
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
			List<Integer> allTaskLists) {
		logger.info("Start init CCGrouping...  all task:" + allTaskLists.size());
		logger.info("AllTasks :" + allTaskLists);
		this.context=context;
		this.stream=stream;
		try {
			ConfigFileManager.downloadConf();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String url= Locator.getInstance().getNLABaseLocation()+ AllConfiguration.STORM_CONF_SATASTIC_FILE_NAME;
		stManager=new SatasticGroupingManager(url,allTaskLists);
		logger.info(stManager.channelMap.toString());
	}


	/**
	 * Tuple values are stored in values, and also get taskIDs
	 */
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		messageNum++;
		//counting
		if (messageNum % 10000 == 0) {
			String line = "In CCGrouping.....";
			line += "faults: [" + exceptionNum + "]," + fault1 + "," + fault2
					+ "," + fault3 + "," + fault4 + ", message(W):"
					+ (messageNum / 10000);
			logger.info(line);
		}
		try {
			//Fault Type1: tuple(null)
			if (values == null || values.size() == 0) {
				fault1++;
				return new ArrayList<Integer>();
			}
			//Fault Type2: title==null
			Object v = values.get(0);
			if (v == null) {
				fault2++;
				 return new ArrayList<Integer>();
			}
			//Fault Type3: no taskid
			String channelid= String.valueOf(values.get(0));
			String userid= String.valueOf(values.get(1));
			String key=userid+"_"+channelid;
			fault4++;
			List<Integer> boltlist=stManager.getBolts(key);
			if(boltlist==null||boltlist.size()==0)
				return new ArrayList<Integer>();
			Integer boltid=boltlist.get(r.nextInt(boltlist.size()));
			ArrayList<Integer> reslist=new ArrayList<Integer>();
			reslist.add(boltid);
			fault3++;
			return reslist;
		} catch (Exception e) {
			exceptionNum++;
			e.printStackTrace();
			String exline = ExceptionUtil.getExcetpionInfo(e, "ccgrouping");
			System.out.println(exline);
			if (exceptionNum % 1000 == 0)
				logger.warn("get expcetion ..... " + exceptionNum);
			return Arrays.asList(defaultTaskID);
		}

	}

	public static void main(String[] args) {
		ArrayList<Integer> allTaskLists=new ArrayList<Integer>();
		for(int i=0;i<10;i++)
			allTaskLists.add(i);
		CCGrouping_st grouping =new CCGrouping_st();
		grouping.prepare(null, null, allTaskLists);

		for(int i=0;i<10;i++) {
			ArrayList<Object> list = new ArrayList<Object>();
			list.add("6939");
			list.add("75415");
			List<Integer> res=grouping.chooseTasks(-1, list);
			System.out.println("res is "+res);
		}
		for(int i=0;i<10;i++) {
			ArrayList<Object> list = new ArrayList<Object>();
			list.add("6939");
			list.add("75427");
			List<Integer> res=grouping.chooseTasks(-1, list);
			System.out.println("res is " + res);
		}

		for(int i=0;i<10;i++) {
			ArrayList<Object> list = new ArrayList<Object>();
			list.add("11");
			list.add("22");
			List<Integer> res=grouping.chooseTasks(-1, list);
			System.out.println("res is " + res);
		}

	}

}
