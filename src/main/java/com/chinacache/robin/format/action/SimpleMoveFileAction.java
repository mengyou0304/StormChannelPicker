/*
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
package com.chinacache.robin.format.action;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ThreadPoolExecutor;

public class SimpleMoveFileAction implements CCRotationAction {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory
			.getLogger(SimpleMoveFileAction.class);

	private String destination = "";

	private String userName = "nu";
	private String channelId = "nc";
	private String type = "nt";

	private static SimpleDateFormat dfs = new SimpleDateFormat("yyyyMMdd/HH");


	@Override
	public void execute(FileSystem fileSystem, Path filePath)
			throws IOException {
		String newDestPath = destination;
		newDestPath += "/BILog/src/" + dfs.format(new Date()) + "/" + userName
				+ "/" + channelId + "/";
		Path destPath = new Path(newDestPath);
		LOG.info("Moving file {} to {}", filePath, destPath);
		fileSystem.mkdirs(destPath);
		fileSystem.rename(filePath, destPath);
	}


	public void setUserName(String userName) {
		this.userName = userName;
	}


	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public static void main(String[] args) {

		String newDestPath = "";
		newDestPath += "/BILog/src/" + dfs.format(new Date()) + "/" + "a" + "/"
				+ "b" + "/";
		System.out.println(newDestPath);
		Date d = new Date();
		System.out.println(dfs.format(d));
	}

	@Override
	public CCRotationAction copy() {
		SimpleMoveFileAction action = new SimpleMoveFileAction();
		action.destination = this.destination;
		action.userName = this.userName;
		action.channelId = this.channelId;
		return action;
	}

	@Override
	public void setThreadPool(ThreadPoolExecutor pool) {
	}
}
