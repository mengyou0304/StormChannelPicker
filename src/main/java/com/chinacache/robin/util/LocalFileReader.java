/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.util;

import com.chinacache.robin.util.basic.LineProcess;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by robinmac on 15-8-20.
 */
public class LocalFileReader {

	//using Buffered Reader decrate Reader
	public static void readM1(String url,LineProcess processer) throws FileNotFoundException {
		Integer lineNum=0;
		long offset=0l;
		FileReader fr=new FileReader(url);
		BufferedReader br=new BufferedReader(fr);
		String line="";
		try {
			while((line=br.readLine())!=null) {
				processer.onEachFileLine(line,lineNum,offset);
				offset+=(line+"\n").getBytes().length;
				lineNum++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			fr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Using NIO to read from file
	 * @param url
	 * @param processer
	 * @throws FileNotFoundException
	 */
	public static void readM2(String url,LineProcess processer) throws FileNotFoundException {
		File file=new File(url);
		FileInputStream fis=new FileInputStream(file);
		FileChannel fiIn = fis.getChannel();
//		ByteBuffer buffer = ByteBuffer.allocate(1024);
		ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
		while(true){
			buffer.clear();
			try {
				int r=fiIn.read(buffer);
				if(r==-1)
					break;
				buffer.flip();
				buffer.get();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public static String readLineByOffSet(String url,Long offset){
		RandomAccessFile rafile = null;
		try {
			rafile=new RandomAccessFile(url,"r");
			rafile.seek(offset);
			String line=rafile.readLine();
			return line;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				rafile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public static void main(String[] args) {
		final LocalFileReader fr=new LocalFileReader();
		String url="/Application/nla/log_pick/conf/test/readedfile";
		String url2="/Application/nla/log_pick/conf/test/size.out";
		final String url3="/Application/nla/log_pick/conf/test/testfile";

		try {
			fr.readM1(url3, new LineProcess() {
				@Override
				public void onEachFileLine(String line, Integer lineNum, Long offset) {
					String l2=fr.readLineByOffSet(url3,offset);
					System.out.println("["+lineNum+"]"+line+"\t"+offset);
					System.out.println(l2);
					System.out.println();
				}
			});
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}
