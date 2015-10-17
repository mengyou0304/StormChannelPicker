package com.chinacache.robin.format.nameformat;

import java.util.Map;

import backtype.storm.task.TopologyContext;

public class SimpleFileNameFormat implements CCFileNameFormat {

	private static final long serialVersionUID = 1L;

	private String componentId;
	private int taskId;
	private String path = "/storm";
	private String prefix = "";
	private String extension = ".txt";
	private String specialKey = "";

	/**
	 * Overrides the default prefix.
	 * 
	 * @param prefix
	 * @return
	 */
	public SimpleFileNameFormat withSpecialKey(String key) {
		this.specialKey = key;
		return this;
	}

	/**
	 * Overrides the default file extension.
	 * 
	 * @param extension
	 * @return
	 */
	public SimpleFileNameFormat withExtension(String extension) {
		this.extension = extension;
		return this;
	}

	public SimpleFileNameFormat withPath(String path) {
		this.path = path;
		return this;
	}

	@Override
	public String getName(long rotation, long timeStamp) {
		return specialKey + this.extension;
	}

	public String getPath() {
		return this.path;
	}

	@Override
	public CCFileNameFormat copy() {
		SimpleFileNameFormat model = new SimpleFileNameFormat();
		model.componentId = this.componentId;
		model.taskId = this.taskId;
		model.path = this.path;
		model.prefix = this.prefix;
		model.extension = this.extension;
		model.specialKey = this.specialKey;
		return model;
	}

	@Override
	public CCFileNameFormat withBoltID(String boltID) {
		return null;
	}

	@Override
	public void prepare(Map conf, TopologyContext topologyContext) {

	}

	@Override
	public CCFileNameFormat withLogType(String logtypo) {
		return null;
	}

}
