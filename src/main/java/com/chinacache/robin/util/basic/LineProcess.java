package com.chinacache.robin.util.basic;


public abstract class LineProcess {
	public abstract void onEachFileLine(String line, Integer lineNum, Long offset);
}
