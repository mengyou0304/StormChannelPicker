package com.chinacache.robin.format.nameformat;

import backtype.storm.task.TopologyContext;

import java.util.Map;

public class LogFileNameFormat implements CCFileNameFormat {

	private static final long serialVersionUID = 1L;

	private int taskId;
	private String prefix = "";
	private String extension = ".txt";
	private String specialKey = "";
	private String logtype = "FCACCESS";

	/**
	 * Overrides the default prefix.
	 * 
	 * @param prefix
	 * @return
	 */
	@Deprecated
	public LogFileNameFormat withPrefix(String prefix) {
		this.prefix = prefix;
		return this;
	}


	public LogFileNameFormat withSpecialKey(String key) {
		this.specialKey = key;
		return this;
	}

	public LogFileNameFormat withLogType(String logtypo) {
		this.logtype = logtypo;
		return this;
	}

	public LogFileNameFormat withTestTaskId(int taskID) {
		this.taskId = taskID;
		return this;
	}

	/**
	 * Overrides the default file extension.
	 * 
	 * @param extension
	 * @return
	 */
	public LogFileNameFormat withExtension(String extension) {
		this.extension = extension;
		return this;
	}


	@Override
	public void prepare(Map conf, TopologyContext topologyContext) {
	}

	@Override
	public String getName(long rotation, long timeStamp) {
		return logtype + "." + prefix + specialKey + "-" + rotation + "-"
				+ timeStamp + extension;
	}

	@Override @Deprecated
	public String getPath() {
		return null;
	}


	@Override
	public CCFileNameFormat copy() {
		LogFileNameFormat model = new LogFileNameFormat();
		model.taskId = this.taskId;
		model.prefix = this.prefix;
		model.extension = this.extension;
		model.specialKey = this.specialKey;
		model.logtype = this.logtype;
		return model;
	}

	@Override
	public CCFileNameFormat withBoltID(String boltID) {
		return null;
	}
}
