package com.chinacache.robin.format.rotation;

import java.util.Timer;

import backtype.storm.tuple.Tuple;

public class SimpleFileSizeRotationPolicy implements CCRotationPolicy {

	private static final long serialVersionUID = 1L;

	public static enum Units {

		KB((long) Math.pow(2, 10)), MB((long) Math.pow(2, 20)), GB((long) Math
				.pow(2, 30)), TB((long) Math.pow(2, 40));

		private long byteCount;

		private Units(long byteCount) {
			this.byteCount = byteCount;
		}

		public long getByteCount() {
			return byteCount;
		}
	}

	private long maxBytes;

	private long lastOffset = 0;
	
	private long currentBytesWritten = 0;

	public SimpleFileSizeRotationPolicy(float count, Units units) {
		this.maxBytes = (long) (count * units.getByteCount());
	}

	public SimpleFileSizeRotationPolicy() {
	}

	@Override
	public boolean mark(Tuple tuple, long offset) {
		long diff = offset - lastOffset;
		currentBytesWritten += diff;
		lastOffset = offset;
		return currentBytesWritten >= maxBytes;
	}

	@Override
	public void reset() {
		currentBytesWritten = 0;
		lastOffset = 0;
	}

	@Override
	public CCRotationPolicy copy() {
		SimpleFileSizeRotationPolicy policy = new SimpleFileSizeRotationPolicy();
		policy.maxBytes = maxBytes;
		return policy;
	}

	@Override
	public void prepareForStart(Timer timer) {
	}

}
