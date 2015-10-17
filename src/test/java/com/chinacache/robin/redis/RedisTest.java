/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.redis;

import redis.clients.jedis.Jedis;

/**
 * Created by robinmac on 15-9-28.
 */
public class RedisTest {
	String uri = "redis://127.0.0.1:6379/5";
	String[] base_uris = new String[]{"redis://127.0.0.1:6379/5"};
	Jedis jedisConnection;
	public RedisTest() {

	}


	public Jedis getJedis() {
		if(jedisConnection==null||!jedisConnection.isConnected())
			jedisConnection= new Jedis(uri);
		return jedisConnection;
	}


	public void basicTest() {


	}

	public static void main(String[] args) {

	}
}
