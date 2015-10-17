package com.chinacache.robin.format.action;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.storm.hdfs.common.rotation.RotationAction;

public interface CCRotationAction extends RotationAction {
	public CCRotationAction copy();

	public void setThreadPool(ThreadPoolExecutor pool);
}
