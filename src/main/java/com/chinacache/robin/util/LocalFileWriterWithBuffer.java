/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.util;

import com.chinacache.robin.util.basic.ExceptionUtil;
import com.chinacache.robin.util.basic.LineProcess;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class LocalFileWriterWithBuffer implements Serializable {
	BufferedOutputStream bo;
	FileOutputStream fo;
	File file;
	volatile int lock = 0;

	public void openFile(String url) throws FileNotFoundException {
		lock++;
		try {
			file = new File(url);
			fo = new FileOutputStream(file);
			// bo = new BufferedOutputStream(fo, 2097152);
			bo = new BufferedOutputStream(fo, 8388608);
		} catch (Exception e) {
			String eline = ExceptionUtil.getExcetpionInfo(e, url);
			System.out.println(eline);
		}
		lock--;
	}

	public void writeStream(byte[] bts) {
		try {
			while (lock > 0) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if (bo == null)
				return;
			bo.write(bts);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			bo.flush();
			bo.close();
			fo.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		LocalFileWriterWithBuffer fw = new LocalFileWriterWithBuffer();
		try {
			fw.openFile("/Application/nla/log_pick/conf/mengyou2.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		fw.writeTestStream(fw);
		fw.close();
	}

	private void writeTestStream(final LocalFileWriterWithBuffer fw) {
		FileUtility.processByFile("/Application/nla/log_pick/conf/mengyou.txt",
				new LineProcess() {
					@Override
					public void onEachFileLine(String line,Integer lineNum,Long offset) {
						if (line == null || line.length() == 0)
							return;
						fw.writeStream((line + "\n").getBytes());
					}
				});
	}

}
