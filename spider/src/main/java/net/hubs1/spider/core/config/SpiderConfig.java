package net.hubs1.spider.core.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;


public final class SpiderConfig {
	private static Logger log=Logger.getLogger(SpiderConfig.class);
	private String mqIP=null;
	private Integer mqPort=null;
	private String publicQueueName=null;
	private int downloadThreadNum=10;
	private String mrJarFilLocalPath=null;
	
	/*输出模式。*/
	private String outMode="hbase";
	
	private String downloadHDFSBasePath=null;
	private String extractHDFSBasePath=null;
	private String customHDFSBasePath=null;
	
	/*以下为hadoop配置*/
	private Configuration hadoopConf=null;
	
	/*以下为hbase配置*/
	private Configuration hbaseConf=null;
	
	private String saveFilePath=null;
	
	private SpiderConfig(){}
	
	public static SpiderConfig loadConfig(URL path){
		URL file=null;
		Properties p=new Properties();
		if(path==null){
			 file=SpiderConfig.class.getClassLoader().getResource("spider_download.conf");
		}else{
			file=path;
		}
		
		if(file!=null){	
			log.info(" add config file "+file.getPath());
			try {
				p.load(file.openStream());
			} catch (InvalidPropertiesFormatException e) {
				log.error(e);
			} catch (FileNotFoundException e) {
				log.error(e);
			} catch (IOException e) {
				log.error(e);
			}
		}
		if(!p.isEmpty()){
			SpiderConfig config=new SpiderConfig();
			config.customHDFSBasePath=p.getProperty("custom.hdfs.basepath","/spider/format/custom");
			config.downloadHDFSBasePath=p.getProperty("download.hdfs.basepath","/spider/format/download");
			config.extractHDFSBasePath=p.getProperty("extract.hdfs.basepath","/spider/format/extract");
			config.downloadThreadNum=Integer.parseInt(p.getProperty("download.thread.num", "10"));
			
			config.mqIP=p.getProperty("mq.ip");
			
			if(config.mqIP==null) throw new RuntimeException("don't set rabbitMQ Server IP! please set");
			
			config.mqPort=Integer.parseInt(p.getProperty("mq.port","5672"));
			config.publicQueueName=p.getProperty("public.queue.name", "spider_public_message");
			
			config.mrJarFilLocalPath=p.getProperty("mr.jar.local.path");
			config.outMode=p.getProperty("out.mode","hbase");
			config.saveFilePath=p.getProperty("save.file.path");
			
//			config.hbaseConf=new Configuration(false);
//			config.hbaseConf.set("fs.default.name", p.getProperty("fs.default.name"));
//			config.hbaseConf.set("hbase.zookeeper.quorum",p.getProperty("hbase.zookeeper.quorum"));
//			config.hbaseConf.set("fs.default.name",p.getProperty("fs.default.name"));
//			config.hbaseConf.set("hbase.zookeeper.property.clientPort",p.getProperty("hbase.zookeeper.property.clientPort"));
//			config.hbaseConf.set("hbase.master",p.getProperty("hbase.master"));
//			config.hbaseConf .set("hbase.rootdir",p.getProperty("hbase.rootdir"));
//											
//			config.hadoopConf=new Configuration(false);
//			config.hadoopConf.set("fs.default.name", p.getProperty("fs.default.name"));
			
			return config;
		}
		
		throw new RuntimeException("don't find or load  config file is fail");
	}
	
	public String getMqIP() {
		return mqIP;
	}
	public Integer getMqPort() {
		return mqPort;
	}
	public String getPublicQueueName() {
		return publicQueueName;
	}
	public int getDownloadThreadNum() {
		return downloadThreadNum;
	}
	public String getDownloadHDFSBasePath() {
		return downloadHDFSBasePath;
	}
	public String getExtractHDFSBasePath() {
		return extractHDFSBasePath;
	}
	public String getCustomHDFSBasePath() {
		return customHDFSBasePath;
	}
	public String getOutMode() {
		return outMode;
	}
	
	public void setOutMode(String om) {
		outMode=om;
	}
	public Configuration getHadoopConf() {
		return hadoopConf;
	}

	public Configuration getHbaseConf() {
		return hbaseConf;
	}

	public String getSaveFilePath() {
		return saveFilePath;
	}

	public void setSaveFilePath(String saveFilePath) {
		this.saveFilePath = saveFilePath;
	}

	public String getMrJarFilLocalPath() {
		return mrJarFilLocalPath;
	}
	
	
}
