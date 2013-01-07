package net.hubs1.bijia.dao;

import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;

public interface Hbase2MysqlInterface {

	public void init();
	public void close();
	public void hbaseSelect(String site);
	public void realHbaseSelect(Map<String,String> map, KeyValue kv);
	public void mysqlInsert(Map<String,String> map);
}
