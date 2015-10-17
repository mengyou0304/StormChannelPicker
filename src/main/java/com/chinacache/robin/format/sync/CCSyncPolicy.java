package com.chinacache.robin.format.sync;

import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

public interface CCSyncPolicy extends SyncPolicy {
	public CCSyncPolicy copy();
}
