/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by robinmac on 15-9-21.
 */
public class StartSimpleConsumer {
	private static final Logger logger = LoggerFactory
		.getLogger(StartSimpleConsumer.class);

	public static void main(String[] args) {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Date d=new Date(1442975760903l );
		System.out.println(sdf.format(d));
		Date now=new Date();
		System.out.println(sdf.format(now));
		System.out.println(2036394-2336+23320);
	}
}
