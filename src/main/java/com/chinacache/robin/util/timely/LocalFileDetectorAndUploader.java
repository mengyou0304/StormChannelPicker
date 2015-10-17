package com.chinacache.robin.util.timely;

import com.chinacache.robin.util.config.AllConfiguration;
import com.chinacache.robin.util.config.Locator;
import com.chinacache.robin.util.hdfs.HDFSUtility;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.jboss.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class LocalFileDetectorAndUploader {
	private static final Logger logger = LoggerFactory
			.getLogger(LocalFileDetector.class);

	private static SimpleDateFormat destDateFormat = new SimpleDateFormat(
			"yyyyMMdd/HH");

	class HDFSFile {
		String channel;
		String user;
		String type;
		Date gentime;
		File localFile;

		public void parse(String filename, File file) {
			this.localFile = file;
		}

		public String toString() {
			String s = "";
			s += "type:" + type + "\n";
			s += "userid:" + user + "\n";
			s += "channelid:" + channel + "\n";
			s += "gentime:" + gentime + "\n";
			s += "localFile:" + localFile + "\n";
			return s;
		}
	}

	enum FileAction {IGNORE, JUMP, DELETE, UPLOAD};
	
	private ArrayList<String> baseDirList;

	private ArrayList<HDFSFile> candidateFileList;

	private long timeLowBound;

	private long timeUpperBound;

	private boolean deleteTooOld = false;

	private Options opts;

	public LocalFileDetectorAndUploader() {
		opts = new Options();
		opts.addOption("format", true, "Specify date format");
		opts.addOption("low", true, "File generate after this time");
		opts.addOption("upper", true, "File generate less than this time");
		opts.addOption("del", true, "Whether delete file older than low time");
		opts.addOption("help", false, "Print usage");
	}

	/**
	 * Helper function to print usage
	 */
	private void printUsage() {
		new HelpFormatter().printHelp("FileUploader", opts);
	}

	/**
	 * Parse command line options
	 * 
	 * @param args
	 * @return
	 * @throws org.apache.commons.cli.ParseException
	 * @throws ParseException
	 */
	public boolean initCommandArgs(String[] args)
			throws org.apache.commons.cli.ParseException, ParseException {
		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}

		String del = cliParser.getOptionValue("del", "false");
		deleteTooOld = Boolean.parseBoolean(del);
		System.out.println(del);

		String format = cliParser.getOptionValue("format");
		if (format == null)
			return false;
		SimpleDateFormat dateFormat = new SimpleDateFormat(format);

		String upper = cliParser.getOptionValue("upper");
		if (upper == null) {
			timeUpperBound = System.currentTimeMillis();
		} else {
			timeUpperBound = dateFormat.parse(upper).getTime();
			System.out.println(upper + "\t" + dateFormat.parse(upper) + "\t"
					+ timeUpperBound);
		}

		String low = cliParser.getOptionValue("low");
		if (low == null) {
			timeLowBound = timeUpperBound - TimeUnit.HOURS.toMillis(1L);
		} else {
			timeLowBound = dateFormat.parse(low).getTime();
			System.out.println(low + "\t" + dateFormat.parse(low) + "\t"
					+ timeLowBound);
		}

		System.out.println("upp : " + dateFormat.format(new Date(timeUpperBound)));
		System.out.println("low : " + dateFormat.format(new Date(timeLowBound)));
		return true;
	}

	/**
	 * Prepare the files to be uploaded. For the files which don't between time
	 * range, either deleted or ignored
	 */
	public void prepareUploadFiles() {
		candidateFileList = new ArrayList<HDFSFile>();

		Locator locator = Locator.getInstance();
		locator.detectFileDirs();
		baseDirList = locator.getAvailableLocalURLs();

		for (String url : baseDirList) {
			File baseDir = new File(url);
			File[] files = baseDir.listFiles();
			for (File file : files) {
				if (!file.exists())
					continue;
				processOneFile(file);
			}
		}
	}

	public void logAction(FileAction action, String reason, Date date, File file) {
		String message = String.format("%s because of %s, File %s (%s)",
				action, reason, file.getAbsolutePath(), date);
		logger.info(message);
	}
	
	/**
	 * For each file, it will be processed as:
	 * <ul>
	 * <li>1. add to upload file list, wait to to upload
	 * <li>2. file generate latter than range, ignored
	 * <li>3. generated than before range, ignored(default), deleted(command set
	 * del argument true)
	 * </ul>
	 * 
	 * @param file
	 */
	public void processOneFile(File file) {
		try {
			boolean deleted = false;
			String fileName = file.getName();
			HDFSFile hfile = null;

			try {
				hfile = parse(fileName, file);
			} catch (Exception e) {
				logger.info("File :" + fileName);
				e.printStackTrace();
			}
			
			if (hfile == null) {
				deleted = file.delete();
				Thread.sleep(10);
				logAction(FileAction.DELETE, "can't parse.", null, file);
				return;
			}

			if (hfile.gentime.getTime() > timeUpperBound) {
				logAction(FileAction.JUMP, "too new", hfile.gentime, file);
				return;
			}

			if (file.length() < 1L) {
				deleted = file.delete();
				Thread.sleep(10);
				logAction(FileAction.DELETE, "empty file", hfile.gentime, file);
				return;
			}

			if (hfile.gentime.getTime() > timeLowBound) {
				candidateFileList.add(hfile);
				logAction(FileAction.UPLOAD, "between time range", hfile.gentime, file);
				return;
			} 
			
			if (deleteTooOld) {
				deleted = file.delete();
				Thread.sleep(10);
				logAction(FileAction.DELETE,
						"too old and command setting delete true",
						hfile.gentime, file);
			} else
				logAction(FileAction.IGNORE, "too old", hfile.gentime, file);
			

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Upload the candidate files to HDFS
	 */
	public void commitAndDelet() {
		int totalNum = candidateFileList.size();
		int curNum = 1;
		logger.info(String .format("There is %d files to be upload", totalNum));
		
		for (HDFSFile hfile : candidateFileList) {
			String newDestPath = AllConfiguration.getHdfsDestPath(
					destDateFormat.format(new Date()), hfile.user,
					hfile.channel);
			final Path destPath = new Path(newDestPath);
			final Path localPath = new Path(hfile.localFile.getAbsolutePath());
			try {
				if (hfile.localFile.exists()) {
					logger.info(String.format(
							"[%d of %d]PutLocalFile %s to hdfs %s", curNum++,
							totalNum, localPath, destPath));
					HDFSUtility.putLocalToHDFS(localPath, destPath);
				}
				Thread.sleep(10 * 1000L);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void run() {
		logger.info("Starting detecting old files and commit to HDFS.........");
		prepareUploadFiles();
		commitAndDelet();
	}

	public HDFSFile parse(String filename, File file) {
		HDFSFile hfile = new HDFSFile();
		String[] ss = StringUtil.split(filename, '_');
		if (ss.length < 3)
			return null;
		String[] type_chanel = StringUtil.split(ss[0], '.');
		hfile.type = type_chanel[0];
		hfile.channel = type_chanel[1];
		hfile.user = ss[1];
		String[] ds = StringUtil.split(ss[2], '-');
		if (ds.length < 3)
			return null;
		int suffixLen = 4;
		if (filename.endsWith("gz")) {
			suffixLen = 7;
		}
		Long ftimel = Long.parseLong(ds[2].substring(0, ds[2].length()
				- suffixLen));
		Date filetime = new Date(ftimel);
		hfile.gentime = filetime;
		hfile.localFile = file;
		return hfile;
	}

	public void test() {
		String fileName = "./data/LIGHTTPD.55649_2152_LIGHTTPD-6-1433495417607.txt";
		processOneFile(new File(fileName));
		logAction(FileAction.UPLOAD, "between time range", null, new File(fileName));
	}

	public static void main(String[] args) {
		try {
			LocalFileDetectorAndUploader lfd = new LocalFileDetectorAndUploader();
			try {
				boolean doRun = lfd.initCommandArgs(args);
				if (!doRun)
					System.exit(0);
			} catch (Exception e) {
				e.printStackTrace();
				lfd.printUsage();
				System.exit(-1);
			}
			lfd.run();
//			 lfd.test();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
