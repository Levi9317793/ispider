package net.hubs1.spider.zk;

import java.io.IOException;
import java.util.Calendar;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;

public class ZKClient {
//	private ZooKeeper zk=null;
	
	private ZKClient() throws Exception{
		
	}
	
	public void createDir(String dir,String isTemp,String annotation){
		
	}
	public static void main(String[] args) throws Exception {
//		ZooKeeper zk=new ZooKeeper("192.168.20.21:9090,192.168.20.22:9090", 1000, new Watcher() {
//			
//			@Override
//			public void process(WatchedEvent event) {
//				
//				
//			}
//		});
//		while(zk.getState() != States.CONNECTED)
//		{
//			Thread.sleep(1000);
//			
//		}
//		
//		zk.setData("/dfax/broker/instance", "bucket_size=10\n192.168.20.141:22200=[-]".getBytes(), -1);
		Calendar c=Calendar.getInstance();
		System.out.println(c.get(Calendar.WEEK_OF_MONTH));
	}
	
}
