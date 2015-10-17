package com.chinacache.robin.service;

/**
 * Interface define Event API
 * 
 * @author zexing.hu
 */
public interface Event<TYPE extends Enum<TYPE>> {
	TYPE getType();

	long getTimestamp();

	String toString();
}
