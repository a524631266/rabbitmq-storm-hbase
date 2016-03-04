package com.adamfei.hbase.mapper;

import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;

import backtype.storm.tuple.Tuple;

/**
 * 自定义tuple与hbase数据行的映射
 * @author adam
 *
 */
public class MyHBaseMapper implements HBaseMapper {

	public ColumnList columns(Tuple tuple) {
		
		ColumnList cols = new ColumnList();
		cols.addColumn("c1".getBytes(), "str".getBytes(), tuple.getStringByField("str").getBytes());
		cols.addColumn("c2".getBytes(), "num".getBytes(), tuple.getStringByField("num").getBytes());
		
		return cols;
	}

	public byte[] rowKey(Tuple tuple) {
		return tuple.getStringByField("id").getBytes();
	}

}
