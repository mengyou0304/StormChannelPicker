/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.kafka;

/**
 * Created by robinmac on 15-10-8.
 */
public class AckCannotClearException extends RuntimeException{

	public AckCannotClearException(String s) {
		super(s);
	}
}
