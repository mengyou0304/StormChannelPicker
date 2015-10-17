package com.chinacache.robin.main;

import backtype.storm.topology.TopologyBuilder;

public class StormMain {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		StormComponentGenerator generator = new StormComponentGenerator();
		generator.init(false);
		generator.initSpout(builder);
		generator.initLogicBolt(builder);
		generator.initHFDSBolt(builder);
//		generator.addAdditionalBolt(builder);
		
		generator.startTopology(builder);		
	}
}
