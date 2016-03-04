package com.adamfei.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import backtype.storm.tuple.Fields;

/**
 * 自定义MQ消息的Schema
 * @author adam
 *
 */
public class MyCustomMessageScheme implements backtype.storm.spout.Scheme {

	
	/**
	 * 把MQ中读取的消息反序列化
	 */
	public List<Object> deserialize(byte[] bytes) {
		List objs = new ArrayList();
		
		//直接反序列化为string
		String str = new String(bytes);
		
		//依次返回UUID，String,Number
		objs.add(UUID.randomUUID().toString());
		objs.add(str);
		String numStr = Math.round(Math.random()*8999+1000)+""; 
		objs.add(numStr);
		
		return objs;
	}

	/**
	 * 定义spout输出的Fileds
	 */
	public Fields getOutputFields() {
		//依次返回UUID，String,Number
		return new Fields("id", "str", "num");
	}

}