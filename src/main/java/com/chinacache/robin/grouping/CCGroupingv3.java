//package com.chinacache.robin.grouping;
//
//
//import backtype.storm.generated.GlobalStreamId;
//import backtype.storm.grouping.CustomStreamGrouping;
//import backtype.storm.task.WorkerTopologyContext;
//import com.chinacache.robin.util.basic.ExceptionUtil;
//import com.chinacache.robin.grouping.computation.SizeComputation;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import scala.Tuple2;
//
//import java.util.*;
//
///**
// * Newly written grouping method that:
// * 1. Download sizefile from HDFSServer
// * 2. Run Scala-SizeComputation to generate the boltGrouping file and topic Grouping file
// *
// * @author robin
// */
//public class CCGroupingv3 implements CustomStreamGrouping {
//
//	private static final long serialVersionUID = 1L;
//
//	private static final Logger logger = LoggerFactory
//		.getLogger(CCGroupingv3.class);
//
//	HashMap<String, ArrayList<Integer>> taskMap = new HashMap<String, ArrayList<Integer>>();
//	List<Integer> allTaskLists;
//	HashSet<Integer> taskSet = new HashSet<Integer>();
//
//	long exceptionNum = 1;
//	Integer defaultTaskID = -1;
//	int fault1 = 1;
//	int fault2 = 1;
//	int fault3 = 1;
//	int fault4 = 1;
//	int messageNum = 1;
//
//
//
//	/**
//	 * Are called when first build the grouping
//	 */
//	@Override
//	public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
//	                    List<Integer> allTaskLists) {
//		logger.info("Start init CCGrouping...  all task:" + allTaskLists.size());
//		logger.info("AllTasks :" + allTaskLists);
//		this.allTaskLists = allTaskLists;
//
//		//as taskid may not start form 0  and may not continued as 4,5,6. It may like 0,3,6,7,9
//		//so we have to generate a Maping to our id to real task id
//		HashMap<Integer,Integer> reflectMap=new HashMap<>();
//		Collections.sort(allTaskLists);
//		int p=0;
//		for(Integer i=0;i<allTaskLists.size();i++)
//			reflectMap.put(i,allTaskLists.get(p++));
//		for (Integer i : allTaskLists)
//			taskSet.add(i);
//		String workingLocalURL = "/Application/nla/log_pick/conf/size.out";
//		//TODO Current skip the downloading file process
////		try {
////			HDFSUtility.downloadFromHDFS(AllConfiguration.HDFS_URL+"/conf/size.out", workingLocalURL);
////		} catch (IOException e) {
////			e.printStackTrace();
////		}
//		SizeComputation sc = new SizeComputation(workingLocalURL, 300, allTaskLists.size());
//		Tuple2<String, Object>[] os = sc.startAll();
//		for (Tuple2 tp : os) {
//			ArrayList<Integer> list = taskMap.get(String.valueOf(tp._1()));
//			if (list == null) {
//				list = new ArrayList<Integer>();
//				taskMap.put(String.valueOf(tp._1()), list);
//			}
//			Integer originValue=Integer.parseInt(String.valueOf(tp._2()));
//			Integer relectedId=reflectMap.get(originValue);
//			list.add(relectedId);
//		}
//		//now finish key:v1,v2,v3 maping
//		Iterator<String> it = taskMap.keySet().iterator();
//		while (it.hasNext()) {
//			String key = it.next();
//			logger.debug("[" + key + "]:  " + taskMap.get(key));
//		}
//
//	}
//
//	/**
//	 * Tuple values are stored in values, and also get taskIDs
//	 */
//	@Override
//	public List<Integer> chooseTasks(int taskId, List<Object> values) {
//		Random rand = new Random();
//		messageNum++;
//		//counting
//		if (messageNum % 10000 == 0) {
//			String line = "In CCGrouping.....";
//			line += "faults: [" + exceptionNum + "]," + fault1 + "," + fault2
//				+ "," + fault3 + "," + fault4 + ", message(W):"
//				+ (messageNum / 10000);
//			logger.info(line);
//		}
//		try {
//			//Fault Type1: tuple(null)
//			if (values == null || values.size() == 0) {
//				fault1++;
//				return Arrays.asList(allTaskLists.get(allTaskLists.size() - 1));
//			}
//			//Fault Type2: title==null
//			Object v = values.get(0);
//			if (v == null) {
//				fault2++;
//				return Arrays.asList(allTaskLists.get(allTaskLists.size() - 1));
//			}
//			String key = String.valueOf(v);
//			ArrayList<Integer> taskID = taskMap.get(key);
//			if (taskID == null || taskID.size() == 0) {
//				//Fault Type3: no taskId
//				fault3++;
//				//so we give him a random taskId and save it
//				Integer targetTask = allTaskLists.get(key.hashCode()
//					% allTaskLists.size());
//				ArrayList<Integer> taskList = new ArrayList<>();
//				taskList.add(targetTask);
//				taskMap.put(key, taskList);
//				return taskList;
//			}
//			int k = rand.nextInt(taskID.size());
//			int restaskid = taskID.get(k);
//			if (!taskSet.contains(restaskid)) {
//				// we generate a task id that don't exists.
//				fault1++;
//				return new ArrayList<Integer>();
//			}
//			//the things that we do it right
//			fault4++;
//			return Arrays.asList(restaskid);
//		} catch (Exception e) {
//			exceptionNum++;
//			e.printStackTrace();
//			String exline = ExceptionUtil.getExcetpionInfo(e, "ccgrouping");
//			System.out.println(exline);
//			if (exceptionNum % 1000 == 0)
//				logger.warn("get expcetion ..... " + exceptionNum);
//			return Arrays.asList(defaultTaskID);
//		}
//
//	}
//	public static void main(String[] args) {
////		String workingLocalURL="/Application/nla/log_pick/conf/size.out";
////		try {
////			HDFSUtility.downloadFromHDFS("/conf/size.out",workingLocalURL);
////		} catch (IOException e) {
////			e.printStackTrace();
////		}
//		CCGroupingv3 c3 = new CCGroupingv3();
//		ArrayList<Integer> list = new ArrayList<Integer>();
//		int starter=200;
//		for (int i = starter; i < starter+150; i++)
//			list.add(i);
//		c3.prepare(null, null, list);
//	}
//
//}
