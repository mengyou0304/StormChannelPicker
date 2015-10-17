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

import com.chinacache.robin.util.config.AllConfiguration;
import com.chinacache.robin.service.FileUploadService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ThreadPoolExecutor;

public class LocalToHDFSMoveFileAction implements CCRotationAction {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory
            .getLogger(LocalToHDFSMoveFileAction.class);
    private String destination = "";

    private String userId = "nu";
    private String channelId = "nc";
    private String type = "type";
    private ThreadPoolExecutor pool;

    private SimpleDateFormat dfs = null;

    public LocalToHDFSMoveFileAction() {
        dfs = new SimpleDateFormat("yyyyMMdd/HH", Locale.US);
        dfs.setTimeZone(TimeZone.getTimeZone("GMT+8"));
    }

    public LocalToHDFSMoveFileAction toDestination(String destDir) {
        destination = destDir;
        return this;
    }

    @Override
    public void setThreadPool(ThreadPoolExecutor pool) {
        this.pool = pool;
    }

    @Override
    public void execute(FileSystem remoteFileSystem, final Path filePath)
            throws IOException {
        String newDestPath = AllConfiguration.getHdfsDestPath(
                dfs.format(new Date()), userId, channelId);
        final Path destPath = new Path(newDestPath);
        FileUploadService.getInstance().upload(filePath, destPath);
        LOG.info("Moving file {} to {}", filePath, destPath);
        return;
    }

    public void setUserName(String userName) {
        this.userId = userName;
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

    @Override
    public CCRotationAction copy() {
        LocalToHDFSMoveFileAction action = new LocalToHDFSMoveFileAction();
        action.destination = this.destination;
        action.userId = this.userId;
        action.channelId = this.channelId;
        action.pool = this.pool;
        return action;
    }
}
