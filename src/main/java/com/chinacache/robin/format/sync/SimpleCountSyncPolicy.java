package com.chinacache.robin.format.sync;

import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import backtype.storm.tuple.Tuple;

public class SimpleCountSyncPolicy implements CCSyncPolicy {

	private static final long serialVersionUID = 1L;

	private int count;
	private int executeCount = 0;

	public SimpleCountSyncPolicy(int count) {
		this.count = count;
	}

	@Override
	public boolean mark(Tuple tuple, long offset) {
		executeCount++;
		return executeCount >= count;
	}

	@Override
	public void reset() {
		executeCount = 0;
	}

	@Override
	public CountSyncPolicy clone() {
		CountSyncPolicy csp = new CountSyncPolicy(this.count);
		return csp;
	}

	@Override
	public CCSyncPolicy copy() {
		SimpleCountSyncPolicy sp = new SimpleCountSyncPolicy(this.count);
		return sp;
	}
}
