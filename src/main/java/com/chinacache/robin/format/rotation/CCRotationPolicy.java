package com.chinacache.robin.format.rotation;

import java.util.Timer;

import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;

public interface CCRotationPolicy extends FileRotationPolicy {
	public void prepareForStart(Timer timer);

	public CCRotationPolicy copy();
}
