package com.chinacache.robin.util.basic;

public class Entry {
	public Entry(Integer weight, String key) {
		this.weight = weight;
		this.key = key;
	}

	public String toString() {
		return key + ":" + weight;
	};

	String key;
	Integer weight;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Integer getWeight() {
		return weight;
	}

	public void setWeight(Integer weight) {
		this.weight = weight;
	}
}
