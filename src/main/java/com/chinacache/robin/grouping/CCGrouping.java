package com.chinacache.robin.grouping;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import com.chinacache.robin.util.basic.ExceptionUtil;
import com.chinacache.robin.util.config.Locator;
import com.chinacache.robin.util.config.ManageBoltUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
public class CCGrouping implements CustomStreamGrouping {

	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory
			.getLogger(CCGrouping.class);
	
	private WorkerTopologyContext context;
	private GlobalStreamId stream;

	private List<Integer> allTaskList;
	private List<Integer> orginTaskList;
	private ArrayList<Integer> zeroTaskList;
	private HashSet<Integer> tmpset;

	HashMap<String, ArrayList<Integer>> taskMap;
	private KeyManager km;
	Random rand = new Random();
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
		
		ManageBoltUtil.initConfig();
		this.context = context;
		this.stream = stream;
		allTaskList = allTaskLists;
		int zeroTaskNum = 1 + allTaskList.size() / 10;
		orginTaskList = new ArrayList<Integer>();
		zeroTaskList = new ArrayList<Integer>();
		tmpset = new HashSet<Integer>();
		for (int i = 0; i < allTaskList.size(); i++) {
			tmpset.add(allTaskList.get(i));
			if (i < (allTaskList.size() - zeroTaskNum))
				orginTaskList.add(allTaskList.get(i));
			else
				zeroTaskList.add(allTaskList.get(i));
		}
		km = KeyManager.getInstance(orginTaskList);
		try {
			// Step1. download config file from hdfs
			// ConfigFileManager.deleteAndDownload();
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			String groupconfigURL = Locator.getInstance().getNLABaseLocation()
					+ "ccgroup.conf";
			// Step2. read from config file get a balance map
			taskMap = km.getBalanceMap(groupconfigURL);
			logger.info("task map : " + taskMap);
			
			int[] bg = new int[100];
			for (String key : taskMap.keySet()) {
				int size = taskMap.get(key).size();
				bg[size]++;
			}
			for (int i = 0; i < bg.length; i++)
				System.out.println("get size:" + i + "\t" + bg[i]);
			
			//tastMap.put("000", zeroTaskList);
			initForSpecialChannels(zeroTaskList);
			
			// key->[1,2,3]
			defaultTaskID = orginTaskList.get(0);
			logger.info("Default TaskID = " + defaultTaskID);
			logger.info("Finish init CCGrouping....");
			System.out.println("Finish init CCGrouping....");
			
			logger.info("Record which channel to which task");
			for (Map.Entry<String, ArrayList<Integer>> entry : taskMap.entrySet()) {
				logger.info(entry.getKey() + " : " + entry.getValue());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void initForSpecialChannels(ArrayList<Integer> zeroTaskList) {
		taskMap.put("TabIsNotTwo", zeroTaskList);
		taskMap.put("TypeError", zeroTaskList);
		for (int index = 2; index <= 17; index++) {
			taskMap.put("ChannelIsNull" + index, zeroTaskList);
			taskMap.put("FieldException" + index, zeroTaskList);
			taskMap.put("ParseException" + index, zeroTaskList);
		}
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
				return Arrays.asList(orginTaskList.get(rand
						.nextInt(orginTaskList.size())));
			}
			//Fault Type2: title==null
			Object v = values.get(0);
			if (v == null) {
				fault2++;
				return Arrays.asList(orginTaskList.get(rand
						.nextInt(orginTaskList.size())));
			}
			//Fault Type3: no taskid
			String key = String.valueOf(v);
			ArrayList<Integer> taskID = taskMap.get(key);
			if (taskID == null || taskID.size() == 0) {
				fault3++;
				Integer targettask = orginTaskList.get(key.hashCode()
						% orginTaskList.size());
				ArrayList<Integer> tasklist = new ArrayList<Integer>();
				tasklist.add(targettask);
				taskMap.put(key, tasklist);
				return tasklist;
			}
			int k = rand.nextInt(taskID.size());
			int restaskid = taskID.get(k);
			if (!tmpset.contains(restaskid)) {
				// restaskid=defaultTaskID;
				fault1++;
				return new ArrayList<Integer>();
			}
			fault4++;
			return Arrays.asList(restaskid);
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

}
