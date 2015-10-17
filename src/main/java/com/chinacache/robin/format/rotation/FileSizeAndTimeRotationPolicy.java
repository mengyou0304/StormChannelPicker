/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chinacache.robin.format.rotation;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;

import backtype.storm.tuple.Tuple;

/**
 * File rotation policy that will rotate files when a certain file size is
 * reached.
 * 
 * For example:
 * 
 * <pre>
 * // rotate when files reach 5MB
 * FileSizeRotationPolicy policy = new FileSizeRotationPolicy(5.0, Units.MB);
 * </pre>
 * 
 */
public class FileSizeAndTimeRotationPolicy implements CCRotationPolicy {

	private static final long serialVersionUID = 1L;

	private long maxBytes;

	private long lastOffset = 0;
	private long currentBytesWritten = 0;
	transient boolean canRotat;
	private long interval;

	public FileSizeAndTimeRotationPolicy(int count, int time,
			FileSizeRotationPolicy.Units sizeunits,
			TimedRotationPolicy.TimeUnit timeunit) {
		this.maxBytes = (long) (count * sizeunits.getByteCount());
		this.interval = (long) (time * timeunit.getMilliSeconds());
	}

	public FileSizeAndTimeRotationPolicy() {
	}

	@Override
	public boolean mark(Tuple tuple, long offset) {
		long diff = offset - this.lastOffset;
		this.currentBytesWritten += diff;
		this.lastOffset = offset;
		boolean exceedSize = this.currentBytesWritten >= this.maxBytes;
		if (canRotat || exceedSize) {
			reset();
			return true;
		} else
			return false;
	}

	@Override
	public void reset() {
		this.currentBytesWritten = 0;
		this.lastOffset = 0;
		this.canRotat = false;
	}

	@Override
	public void prepareForStart(Timer timer) {
		TimerTask realtask = new TimerTask() {
			public void run() {
				canRotat = true;
			}
		};
		Long v = interval;
		Random r = new Random();
		int k = r.nextInt(v.intValue() / 1000) * 1000;
		if (k > v)
			k = r.nextInt(2000) * 1000;
		timer.scheduleAtFixedRate(realtask, 60000+k, interval);
	}

	@Override
	public CCRotationPolicy copy() {
		FileSizeAndTimeRotationPolicy fsrp = new FileSizeAndTimeRotationPolicy();
		fsrp.maxBytes = this.maxBytes;
		fsrp.canRotat = false;
		fsrp.interval = this.interval;
		return fsrp;
	}
}
