package com.chinacache.robin.format.fileunit;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.chinacache.robin.bolt.MultiFileHDFSBolt;
import com.chinacache.robin.format.action.CCRotationAction;
import com.chinacache.robin.format.action.LocalToHDFSMoveFileAction;
import com.chinacache.robin.format.action.SimpleMoveFileAction;
import com.chinacache.robin.format.nameformat.CCFileNameFormat;
import com.chinacache.robin.format.rotation.CCRotationPolicy;
import com.chinacache.robin.util.LocalFileWriterWithBuffer;
import com.chinacache.robin.util.config.Locator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Timer;

public class LocalFileUnit implements FileUnit {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory
			.getLogger(LocalFileUnit.class);

	protected ArrayList<CCRotationAction> rotationActions = new ArrayList<CCRotationAction>();
	private Path oldFile;
	private Path currentFile;

	protected SyncPolicy syncPolicy;
	protected CCRotationPolicy rotationPolicy;
	protected CCFileNameFormat fileNameFormat;
	protected int rotation = 0;
	protected String configKey;
	protected transient Object writeLock;

	private MultiFileHDFSBolt fatherBolt;
	private LocalFileWriterWithBuffer localFileWriter;

	private RecordFormat recordFormat;
	private long offset = 0;

	private FileSystem fileSystem;
	String status = "OK";
	String specialKey;
	String channelId = "none";
	String userId = "none";
	String logType = "UNKNOWN";

	public LocalFileUnit() {

	}

	@Override
	public void init(MultiFileHDFSBolt multiFileHDFSBolt, String key,
			String cid, String uid, String type) {
		fatherBolt = multiFileHDFSBolt;
		this.specialKey = key;
		this.channelId = cid;
		this.userId = uid;
		this.logType = type;
		prepare();
	}

	public void prepare() {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		initConfiguration();
		try {
			initFileSystemRelated();
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (fatherBolt.rotationTimer == null)
			fatherBolt.rotationTimer = new Timer(true);
		rotationPolicy.prepareForStart(fatherBolt.rotationTimer);
	}

	private void initConfiguration() {
		writeLock = new Object();
		syncPolicy = fatherBolt.syncPolicy.copy();
		rotationPolicy = fatherBolt.rotationPolicy.copy();
		fileNameFormat = fatherBolt.fileNameFormat.copy()
				.withSpecialKey(specialKey).withLogType(logType);

		rotationActions = new ArrayList<CCRotationAction>();
		for (CCRotationAction actions : fatherBolt.rotationActions) {
			CCRotationAction newAction = actions.copy();
			newAction.setThreadPool(fatherBolt.threadPool);
			rotationActions.add(newAction);
		}

		recordFormat = fatherBolt.recordFormat;
		// TODO directly use father URL?
		if (syncPolicy == null)
			throw new IllegalStateException("SyncPolicy must be specified.");
		if (rotationPolicy == null)
			throw new IllegalStateException("RotationPolicy must be specified.");
		// prepare for file Format
		fileNameFormat.prepare(fatherBolt.stormConf, fatherBolt.context);
	}

	private void initFileSystemRelated() throws Exception {
		// prepare for hdfs
		try {
			fileSystem = new LocalFileSystem();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
		String baseurl = Locator.getInstance().getLocalDataBaseLocation();
		Path newPath = getNewName(baseurl,
				fileNameFormat.getName(rotation, System.currentTimeMillis()),
				fileSystem);
		logger.info("new path is :" + newPath);

		localFileWriter = new LocalFileWriterWithBuffer();
		try {
			localFileWriter.openFile(newPath.toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			status = FileUnit.diskerro;
		}
		currentFile = newPath;
	}

	/**
	 * 
	 * @throws IOException
	 */
	public void rotateOutputFile() throws IOException {
		for (RotationAction action : rotationActions) {
			if (action instanceof SimpleMoveFileAction) {
				SimpleMoveFileAction action2 = (SimpleMoveFileAction) action;
				action2.setChannelId(channelId);
				action2.setUserName(userId);
				action2.setType(logType);
			}
			if (action instanceof LocalToHDFSMoveFileAction) {
				LocalToHDFSMoveFileAction action2 = (LocalToHDFSMoveFileAction) action;
				action2.setChannelId(channelId);
				action2.setUserName(userId);
				action2.setType(logType);
			}
		}
		long start = System.currentTimeMillis();
		localFileWriter.close();
		oldFile = currentFile;
		try {
			initFileSystemRelated();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		rotation++;
		logger.info("{} Performing {} file rotation actions.", specialKey,
				rotationActions.size());
		for (RotationAction action : rotationActions) {
			try {
				action.execute(fileSystem, oldFile);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e.getLocalizedMessage());
			}
		}

		long time = System.currentTimeMillis() - start;
		logger.info("File rotation took {} ms.", time);
	}

	public void onTupleArrive(Tuple tuple) throws IOException {
		if (status.equals(FileUnit.diskerro))
			return;
		try {
			byte[] bytes = this.recordFormat.format(tuple);
			try {
				localFileWriter.writeStream(bytes);
				offset += bytes.length;
			} catch (Exception e) {
				fatherBolt.sendToExceptionBolt(new Values(this.getClass()
						.getName() + ":" + specialKey, e),
						fatherBolt.collector);
				e.printStackTrace();
			}
			if (rotationPolicy.mark(tuple, offset)) {
				rotateOutputFile(); // synchronized
				offset = 0;
				rotationPolicy.reset();
			}
		} catch (IOException e) {
			e.printStackTrace();
			fatherBolt.sendToExceptionBolt(new Values("FileUnit", e),
					fatherBolt.collector);
			logger.warn("write/sync failed.", e);
		}
	}

	public Path getNewName(String parent, String oldname, FileSystem fs) {
		Path path = new Path(parent, oldname);
		try {
			if (!fs.exists(path)) {
				fs.mkdirs(path.getParent());
				return path;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		Integer i = 0;
		while (true) {
			Path np = new Path(parent, oldname + "." + i);
			try {
				if (fs.exists(np)) {
					i++;
					np = new Path(parent, oldname + "." + i);
				} else {
					return np;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void onClose() {
		try {
			rotateOutputFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		localFileWriter.close();
	}

	@Override
	public void reOpenOutput() throws IOException {

	}

	@Override
	public void addStatus(String status) throws IOException {
	}

	@Override
	public void initSimple(MultiFileHDFSBolt multiFileHDFSBolt, String key)
			throws Exception {
		throw new Exception("Not implemented yet" + "!");
	}
}
