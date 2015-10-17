package com.chinacache.robin.util.timely;

import com.chinacache.robin.util.config.AllConfiguration;
import com.chinacache.robin.service.FileUploadService;
import com.chinacache.robin.util.config.Locator;
import org.apache.hadoop.fs.Path;
import org.jboss.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class LocalFileDetector extends TimerTask {
	private static final Logger logger = LoggerFactory
			.getLogger(LocalFileDetector.class);

	private static SimpleDateFormat dfs = new SimpleDateFormat("yyyyMMdd/HH");

	class HDFSFile {
		String channlid;
		String userid;
		String type;
		Date gentime;
		File localfile;

		public void parse(String filename, File file) {
			this.localfile = file;

		}

		public String toString() {
			String s = "";
			s += "type:" + type + "\n";
			s += "userid:" + userid + "\n";
			s += "channlid:" + channlid + "\n";
			s += "gentime:" + gentime + "\n";
			s += "localfile:" + localfile + "\n";
			return s;

		}
	}

	ArrayList<String> urllist;
	ArrayList<HDFSFile> filelist;

	public void init() {
		Long ctime = new Date().getTime();
		filelist = new ArrayList<HDFSFile>();
		Locator locator = Locator.getInstance();
		locator.detectFileDirs();
		urllist = locator.getAvailableLocalURLs();
		for (String url : urllist) {
			File dirfile = new File(url);
			File[] files = dirfile.listFiles();
			for (File file : files) {
				if (!file.exists())
					return;
				try {
					String filename = file.getName();
					HDFSFile hfile = parse(filename, file);
					if (hfile == null) {
						boolean delted = file.delete();
						Thread.sleep(10);
						logger.info("["+delted + "]Delete because of can't prase."
								+ file.getAbsolutePath());
						continue;
					}
					if ((ctime - hfile.gentime.getTime()) < 4000l*1000l) {
						logger.info("Jump because of new file."
								+ file.getAbsolutePath());
						continue;
					}
					if (file.length() < 10000) {
						boolean delted = file.delete();
						Thread.sleep(10);
						logger.info("["+delted + "] Delete because of small size."
								+ file.getAbsolutePath());
						continue;
					}
					filelist.add(hfile);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
		}
	}

	public void commitAndDelet() {
		for (HDFSFile hfile : filelist) {
			String newDestPath = AllConfiguration.getHdfsDestPath(
					dfs.format(new Date()), hfile.userid, hfile.channlid);
			final Path destPath = new Path(newDestPath);
			final Path localPath = new Path(hfile.localfile.getAbsolutePath());
			try {
				if (hfile.localfile.exists())
					//HDFSConfUtility.putLocalToHDFS(localPath, destPath);
					FileUploadService.getInstance().upload(localPath, destPath);
				Thread.sleep(10*1000l);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void run() {
		logger.info("Starting detecting old files and commit to HDFS.........");
		System.out.println("Starting detecting old files and commit to HDFS.........");
		init();
		commitAndDelet();
	}

	public HDFSFile parse(String filename, File file) {
		HDFSFile hfile = new HDFSFile();
		String[] ss = StringUtil.split(filename, '_');
		if (ss.length < 3)
			return null;
		String[] type_chanel = StringUtil.split(ss[0], '.');
		hfile.type = type_chanel[0];
		hfile.channlid = type_chanel[1];
		hfile.userid = ss[1];
		String[] ds = StringUtil.split(ss[2], '-');
		if (ds.length < 3)
			return null;
		Long ftimel = Long.parseLong(ds[2].substring(0, ds[2].length() - 4));
		Date filetime = new Date(ftimel);
		hfile.gentime = filetime;
		hfile.localfile = file;
		return hfile;
	}

	public static void main(String[] args) {
		String s1 = "FCACCESS.19861_2025_FCACCESS-0-1425887531028.txt";
		String s2 = "FCACCESS.hdfsbolt-1-69107-6-1425887073353.txt";
		LocalFileDetector lfd = new LocalFileDetector();
		Timer t = new Timer();
		t.schedule(lfd, 0, 10l * 1000);

	}

}
