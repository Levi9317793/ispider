package net.hubs1.spider.core;

import java.util.List;

import net.hubs1.spider.store.KVBean;
import net.hubs1.spider.store.RecordsBean;

public interface DBClient {
	boolean writeData(String tableName,RecordsBean... attributes);
	List<KVBean> scan(String tableName,String family,String rowkeyStart,String rowkeyEnd,String column);
	
	/**
	 * @param tableName
	 * @param family
	 * @param rowkeyStart
	 * @param rowkeyEnd
	 * @param columns 为null时，返回全部字段
	 * @return
	 */
	List<RecordsBean> scan(String tableName,String family,String rowkeyStart,String rowkeyEnd,String[] columns);
	
	RecordsBean get(String tableName,String family,String rowkey,String[] columns);
	boolean delete(String tableName,String rowkey);
	void commit(String tableName);
}
