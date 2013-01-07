package net.hubs1.spider.store;

import net.hubs1.spider.core.DBClient;
import net.hubs1.spider.core.config.SpiderContext;

public class DBFactory {
	private DBFactory(){}
	
	private static FileStoreClient fsc=null;
	
	public synchronized static DBClient getDBClient(){
		return getDBClient(SpiderContext.getConfig().getOutMode());
	}
	
	public synchronized static DBClient getDBClient(String type){
		if("hbase".equalsIgnoreCase(SpiderContext.getConfig().getOutMode())){
			return HbaseClient.getInstance();
		}
		
		if("file".equalsIgnoreCase(SpiderContext.getConfig().getOutMode())){
			if(fsc==null) fsc=new FileStoreClient();
			return fsc;
		}
		return null;
	}
}
