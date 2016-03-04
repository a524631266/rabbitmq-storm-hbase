package com.adamfei.firstStormProject;

import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class VehiclePassStoreBolt extends BaseRichBolt{

	private OutputCollector collector;
	private Jedis jedis;
	private int count = 0;
	
	
	/*
	 * 测试保存一个tuple到redis中，以StormTest+数字为key依次保存
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple tuple) {
		String str = tuple.getString(0);
		
		String key = "StormTest"+count;
		count++;
		
		jedis.set(key.getBytes(), str.getBytes());
	}

	public void prepare(Map configMap, TopologyContext topologyContext, OutputCollector collector) {
		this.collector = collector;
		
		String redisHost = configMap.get("redisHost").toString();
		Integer redisPort = Integer.parseInt(configMap.get("redisPort").toString());
		
		if(jedis == null){
			jedis = new Jedis(redisHost, redisPort);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub		
	}

}
