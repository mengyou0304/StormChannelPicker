package com.chinacache.robin.util.timely;

import com.chinacache.log.common.service.LogLineService;
import com.chinacache.robin.util.config.ConfigFileManager;

import java.io.IOException;
import java.util.TimerTask;

public class AdmConfUpdater extends TimerTask{
	
	@Override
	public void run() {
		try {
			ConfigFileManager.downloadConf();
			Thread.sleep(10000);
		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}
		LogLineService.reload();
	}
	
	
}
