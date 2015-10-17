/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.grouping.statistic;

import com.chinacache.robin.util.FileUtility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Created by robinmac on 15-9-26.
 */
public class SatasticGroupingManager {
	HashMap<String,ChannelConfig> channelMap=new HashMap<String,ChannelConfig>();
	int indexer=0;

	public SatasticGroupingManager(String url,List<Integer> boltIds){
		String s=FileUtility.readFromFile(url);
		String[] lines=s.split("\n");
		for(String line:lines){
			String[] ss=line.split(":");
			String [] tt=ss[0].split("-");
			String channelid=tt[1];
			String userid=tt[0];
			Integer  boldNum=Integer.parseInt(ss[1]);
			try {
				ArrayList<Integer> boltList=getAvalibleBoltList(boltIds,boldNum);
				if(boltList==null||boltList.size()==0)
					continue;
				ChannelConfig cconfig = new ChannelConfig(userid, channelid,boldNum,boltList);
				channelMap.put(userid+"_"+channelid,cconfig);
			}catch (Exception e){
				e.printStackTrace();
			}
		}
	}
	private ArrayList<Integer> getAvalibleBoltList(List<Integer> boltIds,Integer boltnum){
		if(indexer>=boltIds.size())
			return new ArrayList<Integer>();
		ArrayList<Integer> reslist=new ArrayList<Integer>();
		while(boltnum>0&&indexer<boltIds.size()){
			reslist.add(boltIds.get(indexer++));
			boltnum--;
		}
		return reslist;
	}
	private void showInfos(){
		Set<String> set=channelMap.keySet();
		for(String key:set){
			ChannelConfig config=channelMap.get(key);
			System.out.println(config.toString());
		}
	}
	public List<Integer> getBolts(String key){
//		System.out.println("key: "+key);
//		System.out.println("keys :"+channelMap.keySet());
		ChannelConfig cconfig=channelMap.get(key);
		if(cconfig==null||cconfig.boltIDs==null||cconfig.boltIDs.size()==0)
			return new ArrayList<Integer>();
		return cconfig.boltIDs;
	}


	public static void main(String[] args) {
		String s="/Users/robinmac/workspace/stormv2/storm_logv2/satastic.channels";
		ArrayList<Integer> list=new ArrayList<Integer>();
		for(int i=0;i<6;i++)
			list.add(i);
		SatasticGroupingManager smanager=new SatasticGroupingManager(s,list);
		smanager.showInfos();
	}

}
