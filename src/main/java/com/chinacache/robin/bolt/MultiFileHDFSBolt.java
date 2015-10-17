package com.chinacache.robin.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.chinacache.robin.bolt.basic.CCBolt;
import com.chinacache.robin.format.action.CCRotationAction;
import com.chinacache.robin.format.fileunit.FileUnit;
import com.chinacache.robin.format.fileunit.RemoteFileUnit;
import com.chinacache.robin.format.nameformat.CCFileNameFormat;
import com.chinacache.robin.format.rotation.CCRotationPolicy;
import com.chinacache.robin.format.sync.CCSyncPolicy;
import com.chinacache.robin.util.config.Locator;
import com.chinacache.robin.util.timely.LocalFileDetector;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

public class MultiFileHDFSBolt extends CCBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory
			.getLogger(MultiFileHDFSBolt.class);

	public ArrayList<CCRotationAction> rotationActions = new ArrayList<>();
	public CCSyncPolicy syncPolicy;
	public CCRotationPolicy rotationPolicy;
	public CCFileNameFormat fileNameFormat;
	public RecordFormat recordFormat;

	public String configKey;
	public transient Timer rotationTimer; // only used for
	protected HashMap<String, FileUnit> fileMap;
	public Class<?> FileUnitclass = RemoteFileUnit.class;
	public Map stormConf;
	public TopologyContext context;
	public OutputCollector collector;

	private Long tupleNum = 1L;
	private Long curTime = 0l;
	private Long startTime = 0l;
	int taskId = -1;
	private boolean writeData = true;

	public ThreadPoolExecutor threadPool;
	private Timer detectorTimer;

	public void localfileInit() {
		Random r = new Random();
		Locator.getInstance().detectFileDirs();
		LocalFileDetector lfd = new LocalFileDetector();
		detectorTimer = new Timer();
		detectorTimer.schedule(lfd, r.nextInt(100) * 1000L, 4000L * 1000);
		// threadPool = new ThreadPoolExecutor(3, 20, 10, TimeUnit.SECONDS,
		// new ArrayBlockingQueue<Runnable>(20),
		// new ThreadPoolExecutor.DiscardOldestPolicy());
	}

	/**
	 * Unlike Abstract hdfsbolt it holds many file describer, so they would
	 * dynamically started during the running process instead of doing it in the
	 * prepare.
	 */
	@Override
	public void onPrepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		if (context != null) {
			taskId = context.getThisTaskId();
			fileNameFormat.withBoltID(context.getThisComponentId());
		}
		localfileInit();

		this.stormConf = stormConf;
		this.context = context;
		this.collector = collector;
		fileMap = new HashMap<String, FileUnit>();
		startTime = System.currentTimeMillis();
		curTime = System.currentTimeMillis();
	}

	@Override
	public void onLogicDataArrive(Tuple tuple) {
		if (tuple.size() == 0)
			return;
		
		tupleNum++;
		if (tupleNum % 10000 == 0) {
			Long ntime = System.currentTimeMillis();
			String info = "[in HDFS] message: " + tupleNum / 10000
					+ "w, time cost  1w:" + (ntime - curTime)
					+ "  avg 1w message: " + (ntime - startTime)
					/ (tupleNum / 10000) + " filesize" + fileMap.size();
			logger.info(info);
			curTime = ntime;
		}
		
		if (!writeData)
			return;

		String key = "default_key";
		String channel = "none";
		String user = "none";
		String loyType = "UNKNOWN";
		try {
			channel = tuple.getString(0);
			user = tuple.getString(1);
			loyType = tuple.getString(2);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
		StringBuffer sb = new StringBuffer();
		sb.append(channel).append("_").append(user).append("_").append(loyType);
		String nkey = sb.toString();
		if (nkey != null && nkey.trim().length() > 0)
			key = nkey;
		
		FileUnit fu = fileMap.get(key);
		if (fu == null) {
			try {
				fu = (FileUnit) FileUnitclass.newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			fu.init(this, key, channel, user, loyType);
			fileMap.put(key, fu);
		}
		try {
			fu.onTupleArrive(tuple);
		} catch (Exception e) {
			e.printStackTrace();
			sendToExceptionBolt(new Values(this.getClass().getName(), e), collector);
		}
	}

	@Override
	public void cleanup() {
		super.cleanup();
	}

	public void addStatus(String fileName, String line) {
		String key = fileName;
		FileUnit fu = fileMap.get(key);
		if (fu == null) {
			logger.debug("building new FileUnit:" + key);
			try {
				fu = (FileUnit) FileUnitclass.newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			try {
				fu.initSimple(this, key);
			} catch (Exception e) {
				e.printStackTrace();
			}
			fileMap.put(key, fu);
		}
		try {
			fu.addStatus(line);
		} catch (Exception e) {
			e.printStackTrace();
			sendToExceptionBolt(new Values(getClass().getName(), e), collector);
		}
	}


	public MultiFileHDFSBolt withConfigKey(String configKey) {
		this.configKey = configKey;
		return this;
	}

	public MultiFileHDFSBolt withFileNameFormat(CCFileNameFormat fileNameFormat) {
		this.fileNameFormat = fileNameFormat;
		return this;
	}

	public MultiFileHDFSBolt withRecordFormat(RecordFormat format) {
		this.recordFormat = format;
		return this;
	}

	public MultiFileHDFSBolt withSyncPolicy(CCSyncPolicy syncPolicy) {
		this.syncPolicy = syncPolicy;
		return this;
	}

	public MultiFileHDFSBolt withRotationPolicy(CCRotationPolicy rotationPolicy) {
		this.rotationPolicy = rotationPolicy;
		return this;
	}

	public MultiFileHDFSBolt addRotationAction(CCRotationAction action) {
		this.rotationActions.add(action);
		return this;
	}

	@Override
	public Boolean onControlDataArrive(Tuple input) {
		return false;
	}

	@Override
	public void declareCCOutputFields(OutputFieldsDeclarer declarer) {

	}

	public MultiFileHDFSBolt withFileUnit(Class<?> clazz) {
		this.FileUnitclass = clazz;
		return this;
	}

	@Override
	public void onClose() {
		Set<String> keyset = fileMap.keySet();
		for (String key : keyset) {
			FileUnit fu = fileMap.get(key);
			fu.onClose();
		}
		fileMap.clear();
	}

	public void setWriteData(boolean b) {
		this.writeData = b;
	}

	public ThreadPoolExecutor getThreadPool() {
		return threadPool;
	}

}
