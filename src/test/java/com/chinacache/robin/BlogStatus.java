/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin;

import com.chinacache.robin.util.LocalFileReader;
import com.chinacache.robin.util.LocalFileWriterWithBuffer;
import com.chinacache.robin.util.basic.LineProcess;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;

/**
 * Created by robinmac on 15-10-19.
 */
public class BlogStatus {

	public LocalFileWriterWithBuffer lfb = new LocalFileWriterWithBuffer();

	public void onTitle(String url, String newGenDir) throws FileNotFoundException {
		lfb.openFile(newGenDir);
		LocalFileReader.readM1(url, new LineProcess() {
			@Override
			public void onEachFileLine(String line, Integer lineNum, Long offset) {
				boolean startwithJing = false;
				if (line.trim().length() == 0) {
					lfb.writeStream((line + "\n").getBytes());
					return;
				}

				if (line.trim().charAt(0) == '#')
					startwithJing = true;
				if (!startwithJing) {
					lfb.writeStream((line + "\n").getBytes());
					return;
				}
				boolean goOverJing = false;
				boolean addSpaceFinish = false;
				boolean removeNumFinish = false;
				char[] ts = new char[line.length() + 3];
				int newindex = 0;
				char[] oldts = line.toCharArray();
				for (int i = 0; i < line.length(); i++) {
					if (!addSpaceFinish && goOverJing && line.charAt(i) != '#') {
						ts[newindex] = ' ';
						newindex++;
						addSpaceFinish = true;
					}
					if (line.charAt(i) == '#')
						goOverJing = true;
					if (addSpaceFinish && !removeNumFinish) {
						if ((oldts[i] <= '9' && oldts[i] >= '0') || (oldts[i] == '.') || (oldts[i] == ' '))
							continue;
						else {
							removeNumFinish = true;
						}

					}
					ts[newindex] = oldts[i];
					newindex++;
				}
				String newline = (String.valueOf(ts).trim() + "\n");
				System.out.println(newline);
				lfb.writeStream(newline.getBytes());
			}
		});
		lfb.close();
	}

	public static void addSpaceAndRename(String baseURL) {

		String newGenDir = baseURL + "finishfiles/";
		File dirfile = new File(newGenDir);
		if (!dirfile.exists())
			dirfile.mkdirs();
		BlogStatus bs = new BlogStatus();
		File dir = new File(baseURL);
		File[] files = dir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				if (pathname.getAbsolutePath().endsWith("md") ||
					pathname.getAbsolutePath().endsWith("markdown"))
					return true;
				return false;
			}
		});

		for (File file : files) {
			String filename = file.getName();
			try {
				bs.onTitle(baseURL + "/" + filename, newGenDir + filename);
				System.out.println("*******************\n" + filename);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		String workingURL = "/Users/robinmac/workspace/blogspace/test/";
		addSpaceAndRename(workingURL);
//		deleteAndRenameBack(workingURL);
	}

	private static void deleteAndRenameBack(String workingURL) {


	}

}
