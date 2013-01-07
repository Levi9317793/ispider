package net.hubs1.spider.core.config;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import net.hubs1.spider.core.ResourceClose;
import net.hubs1.spider.core.action.TaskMonitor;
import net.hubs1.spider.scheduler.SpiderScheduler;

public final class SpiderContext {
	private static SpiderConfig config=null;
	
	private static Map<String,ResourceClose> rsMap=new HashMap<String,ResourceClose>();
	
	/**
	 * tag的作用主要是用来分类，防止把资源重复放入。如果有多线程，需要资源自身进行释放
	 * @param tag
	 * @param rc
	 */
	public  static void registResource(String tag,ResourceClose rc){
		rsMap.put(tag, rc);
	}
	
	public static void destoryResource(){
		for(ResourceClose rc:rsMap.values()){
			rc.destoryResource();
		}
	}
	
	private SpiderContext(){};
	
	public synchronized static void initSpiderContent(String path) throws MalformedURLException{
		if(config==null){
			config=SpiderConfig.loadConfig(new URL("file://"+path));
			//checkDir(config.getSaveFilePath());
		}
	}
	
	public synchronized static void initSpiderContentByDefaultFile(){
		if(config==null){
			config=SpiderConfig.loadConfig(null);
			//checkDir(config.getSaveFilePath());
		}
	}
	
	public synchronized static void initTaskMonitor(){
		Executors.newSingleThreadExecutor().execute(TaskMonitor.getTaskMonitor());
	}
	
	public synchronized static void init(){
		initSpiderContentByDefaultFile();
		SpiderScheduler.getInstance().init();
		initTaskMonitor();
	}
	
	private static void checkDir(String dir){
		File dirf=new File(dir);
		if(!dirf.exists()){
			dirf.mkdirs();
		}
	}
	public static SpiderConfig getConfig() {
		return config;
	}	
}
