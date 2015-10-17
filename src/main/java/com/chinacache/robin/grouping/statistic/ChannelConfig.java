/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.grouping.statistic;

import java.util.ArrayList;

/**
 * Created by robinmac on 15-9-26.
 */
public class ChannelConfig {
	private  Integer boltNum;
	private  String userID;
	private  String channeID;

	ArrayList<Integer> boltIDs;

	public ChannelConfig(String userid, String channelid, Integer boldNum,ArrayList<Integer> boltIds) {
		this.userID=userid;
		this.channeID=channelid;
		this.boltNum=boldNum;
		this.boltIDs=boltIds;
	}

	@Override
	public String toString(){
		String s="";
		s+="userid: "+userID+"\n";
		s+="channeID: "+channeID+"\n";
		s+="boltNum: "+boltNum+"\n";
		s+="boltIds: "+boltIDs+"\n";
		return s;
	}

}
