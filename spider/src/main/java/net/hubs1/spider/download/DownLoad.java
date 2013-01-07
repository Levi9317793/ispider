package net.hubs1.spider.download;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;


import net.hubs1.spider.core.Command;
import net.hubs1.spider.core.Task;
import net.hubs1.spider.core.action.TaskAction;
import net.hubs1.spider.core.action.TaskInfo;
import net.hubs1.spider.core.config.SpiderContext;
import net.hubs1.spider.rabbit.MQMessage;
import net.hubs1.spider.rabbit.RabbitMQClient;
import net.hubs1.spider.store.KVBean;

public class DownLoad implements Runnable{
	private static Logger log=Logger.getLogger(DownLoad.class);
	private Map<String,List<Command>> commands=null;
	private RabbitMQClient rabbit=RabbitMQClient.getInstance();
	
	private String queueName=null;
	private DownloadStatus dstatus=null;
	private String taskID=null;
	private boolean isMutil=false;
	
	public DownLoad(Map<String,List<Command>> formats,String taskid,boolean mutil,DownloadStatus ds){
		this.commands=formats;
		this.taskID=taskid;
		this.dstatus=ds;
		this.queueName="spider."+taskid;
		this.isMutil=mutil;
	}

	@Override
	public void run() {
		dstatus.reportStatus(taskID,DownloadStatus.Status.START);
	    log.info(" start download task:"+this.taskID);
		
		if(isMutil){
			while(true){
				MQMessage<KVBean> message=rabbit.receiveMessage(this.queueName);
				if(message==null) continue;
				if(message.getStatus()==MQMessage.Status.STOP){
					dstatus.reportStatus(this.taskID, DownloadStatus.Status.STOP);
					log.info(" destory this thread http resource");
					SpiderContext.destoryResource();
					log.info(this.queueName+" download task is finish, exit download thread!");
					break;//退出监控循环。释放线程池资源。
				}
				downloadResource(message.getMessage());
				rabbit.confirmMessage(this.queueName,message.getResponseTag(), false);
			}
		}else{
			downloadResource(null);
			dstatus.reportStatus(this.taskID, DownloadStatus.Status.STOP);
			if(taskID!=null) TaskAction.updateTaskStatus(taskID,TaskInfo.Status.SUCCESS);
			log.info(" destory this thread http resource");
			SpiderContext.destoryResource();
			log.info(" the format download task is finish, exit download thread!");
		}
	}
	
	private void downloadResource(KVBean kv){
		Task t=new Task(commands,kv);
		t.execute();
	}
}
