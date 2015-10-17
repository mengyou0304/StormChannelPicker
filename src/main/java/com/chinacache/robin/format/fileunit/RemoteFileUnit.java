package com.chinacache.robin.format.fileunit;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.chinacache.robin.bolt.MultiFileHDFSBolt;
import com.chinacache.robin.format.action.CCRotationAction;
import com.chinacache.robin.format.action.SimpleMoveFileAction;
import com.chinacache.robin.format.nameformat.CCFileNameFormat;
import com.chinacache.robin.format.rotation.CCRotationPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.hdfs.common.security.HdfsSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Map;
import java.util.Timer;

public class RemoteFileUnit implements FileUnit {
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory
			.getLogger(RemoteFileUnit.class);

	String specialKey;
	protected ArrayList<RotationAction> rotationActions = new ArrayList<RotationAction>();
	private Path currentFile;
	protected transient FileSystem fileSystem;
	protected SyncPolicy syncPolicy;
	protected CCRotationPolicy rotationPolicy;
	protected CCFileNameFormat fileNameFormat;

	protected int rotation = 0;
	protected String fsUrl;
	protected String configKey;
	protected transient Object writeLock;

	protected transient Configuration hdfsConfig;

	private transient FSDataOutputStream out;
	private MultiFileHDFSBolt fatherBolt;

	private RecordFormat recordFormat;
	private long offset = 0;

	public RemoteFileUnit() {

	}

	@Override
	public void init(MultiFileHDFSBolt multiFileHDFSBolt, String key,
			String k1, String k2, String k3) {
		fatherBolt = multiFileHDFSBolt;
		specialKey = key;
		prepare();
	}

	@Override
	public void initSimple(MultiFileHDFSBolt multiFileHDFSBolt, String key) {
		fatherBolt = multiFileHDFSBolt;
		specialKey = key;
		prepare();
	}

	public void prepare() {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		initConfiguration();
		initHDFSRelated();
		if (fatherBolt.rotationTimer == null)
			fatherBolt.rotationTimer = new Timer(true);
		rotationPolicy.prepareForStart(fatherBolt.rotationTimer);
	}

	private void initConfiguration() {
		writeLock = new Object();
		syncPolicy = fatherBolt.syncPolicy.copy();
		rotationPolicy = fatherBolt.rotationPolicy.copy();
		fileNameFormat = fatherBolt.fileNameFormat.copy().withSpecialKey(
				specialKey);
		rotationActions = new ArrayList<RotationAction>();
		for (CCRotationAction actions : fatherBolt.rotationActions) {
			rotationActions.add(actions.copy());
		}
		recordFormat = fatherBolt.recordFormat;
		// TODO directly use father URL?
		if (syncPolicy == null)
			throw new IllegalStateException("SyncPolicy must be specified.");
		if (rotationPolicy == null)
			throw new IllegalStateException("RotationPolicy must be specified.");
		if (fsUrl == null) {
			throw new IllegalStateException(
					"File system URL must be specified.");
		}
		// prepare for file Format
		fileNameFormat.prepare(fatherBolt.stormConf, fatherBolt.context);

	}

	private void initHDFSRelated() {
		// prepare for hdfs
		this.hdfsConfig = new Configuration();
		hdfsConfig.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		hdfsConfig.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());

		Map<String, Object> map = (Map<String, Object>) fatherBolt.stormConf
				.get(this.configKey);
		if (map != null) {
			for (String key : map.keySet()) {
				hdfsConfig.set(key, String.valueOf(map.get(key)));
			}
		}
		try {
			HdfsSecurityUtil.login(fatherBolt.stormConf, hdfsConfig);
			fileSystem = FileSystem.get(URI.create(fsUrl), hdfsConfig);
			reOpenOutput();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @throws IOException
	 */
	public void rotateOutputFile() throws IOException {
		long start = System.currentTimeMillis();
		out.close();
		rotation++;
		Path newpath = getNewName(
				fileNameFormat.getPath(),
				fileNameFormat.getName(this.rotation,
						System.currentTimeMillis()), fileSystem);
		out = fileSystem.create(newpath);
		logger.info("{} Performing {} file rotation actions.", this.specialKey,
				rotationActions.size());
		for (RotationAction action : rotationActions) {
			try {
				action.execute(fileSystem, currentFile);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e.getLocalizedMessage());
			}
		}
		currentFile = newpath;
		long time = System.currentTimeMillis() - start;
		logger.info("File rotation took {} ms.", time);
	}

	public void reOpenOutput() throws IOException {
		// this.fs = FileSystem.get(URI.create(this.fsUrl), hdfsConfig);
		Path newpath = getNewName(fileNameFormat.getPath(),
				fileNameFormat.getName(rotation, System.currentTimeMillis()),
				fileSystem);
		logger.info("creating  file: " + newpath.getName());
		out = fileSystem.create(newpath);
		currentFile = newpath;
	}

	public void onTupleArrive(Tuple tuple) throws IOException {
		if (out == null)
			try {
				reOpenOutput();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		try {
			byte[] bytes = recordFormat.format(tuple);
			synchronized (writeLock) {
				try {
					out.write(bytes);
					offset += bytes.length;
				} catch (ClosedChannelException cce) {
					fatherBolt.sendToExceptionBolt(new Values(this.getClass()
							.getName() + ":" + this.specialKey, cce),
							this.fatherBolt.collector);
					reOpenOutput();
				}
				if (syncPolicy.mark(tuple, offset)) {
					String channelid = tuple.getStringByField("channelid");
					String userid = tuple.getStringByField("userid");
					for (RotationAction action : rotationActions) {
						if (action instanceof SimpleMoveFileAction) {
							SimpleMoveFileAction action2 = (SimpleMoveFileAction) action;
							action2.setChannelId(channelid);
							action2.setUserName(userid);
						}
					}
					if (out instanceof HdfsDataOutputStream) {
						((HdfsDataOutputStream) out).hsync(EnumSet
								.of(SyncFlag.UPDATE_LENGTH));
					} else {
						out.hsync();
					}
					syncPolicy.reset();
				}
			}
			if (rotationPolicy.mark(tuple, offset)) {
				synchronized (writeLock) {
					rotateOutputFile(); // synchronized
					offset = 0;
					rotationPolicy.reset();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			fatherBolt.sendToExceptionBolt(new Values("FileUnit", e),
					this.fatherBolt.collector);
			logger.warn("write/sync failed.", e);
		}
	}

	public void addStatus(String status) throws IOException {
		try {
			byte[] bytes = status.getBytes();
			out.write(bytes);
			if (syncPolicy.mark(null, offset)) {
				offset += bytes.length;
				if (out instanceof HdfsDataOutputStream) {
					((HdfsDataOutputStream) out).hsync(EnumSet
							.of(SyncFlag.UPDATE_LENGTH));
				} else {
					this.out.hsync();
				}
				syncPolicy.reset();
				if (rotationPolicy.mark(null, offset)) {
					synchronized (writeLock) {
						rotateOutputFile(); // synchronized
						offset = 0;
						rotationPolicy.reset();
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			logger.warn("write/sync failed.", e);
			throw e;
		}
	}

	public Path getNewName(String parent, String oldname, FileSystem fs)
			throws IOException {
		// Path path = new Path("hdfs://223.202.46.136:8020/","storm");
		Path path = new Path(parent, oldname);
		if (!fs.exists(path)) {
			fs.mkdirs(path.getParent());
			return path;
		}
		Integer i = 0;
		while (true) {
			Path np = new Path(parent, oldname + "." + i);
			if (fs.exists(np)) {
				i++;
				np = new Path(parent, oldname + "." + i);
			} else {
				return np;
			}
		}
	}

	public void onClose() {

	}

}
