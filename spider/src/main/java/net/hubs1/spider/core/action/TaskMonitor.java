package net.hubs1.spider.core.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.hubs1.spider.core.DBClient;
import net.hubs1.spider.rabbit.MQMessage;
import net.hubs1.spider.rabbit.RabbitMQClient;
import net.hubs1.spider.scheduler.SpiderScheduler_back;
import net.hubs1.spider.store.DBFactory;
import net.hubs1.spider.store.KVBean;
import net.hubs1.spider.store.RecordsBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;

public class TaskMonitor implements Runnable {
	private static Logger log=Logger.getLogger(TaskMonitor.class);
	private static List<TInfo> info=new ArrayList<TInfo>();
	private JobClient jobClient=null;
	private RabbitMQClient rabbitMqClient=null;
	
	private static TaskMonitor tm=new TaskMonitor();
	
	public TaskMonitor(){
		Configuration cf=new Configuration();
		cf.set("mapred.job.tracker", "spidermaster:50091");
		try {
			jobClient = new JobClient(new org.apache.hadoop.mapred.JobConf(cf));
		} catch (IOException e1) {
			log.error(e1);
		}
	}
	
	public static TaskMonitor getTaskMonitor(){
		return tm;
	}
	
	public   void addTask2Monitor(String taskRowkey,String mrJoibID,String file,String nextRowkey,String type){
		synchronized (info) {
			info.add(new TInfo(taskRowkey, mrJoibID,file,nextRowkey,type));
		}
	}
	
	@Override
	public void run() {
		init();
		log.info("start task monitor thread");
		ExecutorService threadpools=Executors.newFixedThreadPool(15);
		while(true){
			log.info(" start check for a new cycle. the task num is :["+info.size()+"]");
			int index=0;
			
			while(index<info.size()){
				TInfo ti=info.get(index);
				if(ti.isEnd){
					info.remove(index);
					continue;
				}
				if("mr".equalsIgnoreCase(ti.getCtype())){
					mrCheck(ti);
				}else
					if("queue".equalsIgnoreCase(ti.getCtype())){
						threadpools.execute(new MqCheck(ti));
					}else
						if("database".equalsIgnoreCase(ti.getCtype())){
							hbaseCheck(ti);
						}
				++index;
			}
			
			synchronized (this) {
				try {
					wait(1000*60*6);//6分钟检查一次
				} catch (InterruptedException e) {
					log.error(e);
				}
			}
		}
	}
	
	public void init(){
		log.info(" init task monitor ");
		DBClient db=DBFactory.getDBClient();
		rabbitMqClient=RabbitMQClient.getInstance();
		List<RecordsBean> tasks=db.scan("spider_task", "base", null, null, new String[]{});
		for(RecordsBean task:tasks){
			TaskInfo ti=new TaskInfo(task);
			TaskInfo.Status state=ti.getCurrentStatus();
			String type=ti.getType();
			String  nextTask=ti.getNextTask();
			if(state.name().contains("ING")){
				if("url".equalsIgnoreCase(type)){
					if(state.equals(TaskInfo.Status.DOWNLOADING.name())){
						/*这说明抽取任务已经完成*/
						this.addTask2Monitor(task.getRowkeyStr(), null,ti.getFile(), nextTask,"queue");
						TaskAction.sendDownloadComm("url",ti.getTaskID(),ti.getFile(),3);//在初始化发消息通知下载器。
					}else{
						this.addTask2Monitor(task.getRowkeyStr(),ti.getHadoopJobID(),ti.getFile(), nextTask,"mr");
					}
					continue;
				}
				if("extract".equalsIgnoreCase(type)){
					this.addTask2Monitor(task.getRowkeyStr(), ti.getHadoopJobID(),null,nextTask,"mr");
					continue;
				}
				if("format".equalsIgnoreCase(type)){
//					TaskAction.runTask(task.getRowkeyStr());//这个是由于针对模版的下载，如果下载器出现问题则该模版的下载失败。
					this.addTask2Monitor(task.getRowkeyStr(),null,null,nextTask,"database");
					continue;
				}
			}
		}
	}
	
	private void mrCheck(TInfo ti){
		if(ti.getJobID()==null) return ;
		try {
			RunningJob rj=jobClient.getJob(JobID.forName(ti.getJobID()));
			if(rj.isComplete()){
				if(rj.isSuccessful()){
					//更新状态到数据库，并启动下一个任务。
					if(ti.getFile()==null){
						TaskAction.updateTaskStatus(ti.getTaskRowkey(), TaskInfo.Status.SUCCESS);
						ti.setEnd(true);
						ti.setJobID(null);
						if(ti.getNextRowkey()!=null){
							TaskAction.runTask(ti.getNextRowkey());
							log.info(ti.getTaskRowkey()+" 任务成功结束 ，后续任务被启动，任务id是：["+ti.getNextRowkey()+"]");
						}
						SpiderScheduler_back.getInstance().isSuccessEnd(ti.getTaskRowkey());
					}else{
						TaskAction.updateTaskStatus(ti.getTaskRowkey(), TaskInfo.Status.DOWNLOADING);
						TaskAction.sendDownloadComm("url",ti.getTaskRowkey(),ti.getFile(),3);
						ti.setCtype("queue");
						ti.setJobID(null);
						/*外部任务已经运行，而又没有发生异常，则发送下载指令*/
//						TaskAction.sendDownloadComm(3, new KVBean("url",ti.getQueue()));
					}
				}else{
					TaskAction.updateTaskStatus(ti.getTaskRowkey(), TaskInfo.Status.FAIL);
					ti.setEnd(true);
					ti.setJobID(null);
					ti.setNextRowkey(null);//不再继续
					log.error(ti.getTaskRowkey()+" 任务 运行失败 ，后续任务不在进行");
					SpiderScheduler_back.getInstance().isSuccessEnd(ti.getTaskRowkey());
				}
			}
		} catch (IllegalArgumentException e) {
			log.error(e);
		} catch (IOException e) {
			log.error(e);
		}
	}
	
	
	
	class  MqCheck implements Runnable{
		private TInfo tinfo=null;
		public MqCheck(TInfo ti){
			tinfo=ti;
		}
		
		public void run(){
			String queue=tinfo.getQueue();
			if(queue==null) return;
			MQMessage<KVBean> message=rabbitMqClient.receiveMessage(queue);
			if(message!=null){
				try {
					Channel channel=rabbitMqClient.getChannel(queue);
					if(message.getStatus()==MQMessage.Status.STOP){
						tinfo.setEnd(true);
						channel.queueDelete(tinfo.getQueue());
						TaskAction.updateTaskStatus(tinfo.getTaskRowkey(),TaskInfo.Status.SUCCESS);
						log.info(tinfo.getTaskRowkey()+" 任务结束");
						if(tinfo.getNextRowkey()!=null){
							TaskAction.runTask(tinfo.getNextRowkey());
							log.info("后续任务被启动，任务id是：["+tinfo.getNextRowkey()+"]");
						}
						SpiderScheduler_back.getInstance().isSuccessEnd(tinfo.getTaskRowkey());
						
					}
					rabbitMqClient.closeChannel(queue);
				} catch (IOException e) {
					log.error(e);
				}
			}
		}
	}
	
	private void hbaseCheck(TInfo ti){
		RecordsBean rb=DBFactory.getDBClient().get("spider_task", "base", ti.getTaskRowkey(), new String[]{"cstatus"});
		if(rb!=null&&rb.getAttributes().length==1){
			String state=rb.getAttributes()[0].getValue();
			if(state.equals(TaskInfo.Status.SUCCESS.name())){
				ti.setEnd(true);
				log.info(ti.getTaskRowkey()+" 任务成功结束 ");
				if(ti.getNextRowkey()!=null){
					TaskAction.runTask(ti.getNextRowkey());
					log.info("后续任务被启动，任务id是：["+ti.getNextRowkey()+"]");
				}
			}
			if(state.equals(TaskInfo.Status.FAIL.name())){
				ti.setEnd(true);
				log.info(ti.getTaskRowkey()+" 任务执行失败！将忽略后续任务 ");
			}
			
			if(ti.isEnd){
				SpiderScheduler_back.getInstance().isSuccessEnd(ti.getTaskRowkey());
			}
		}
	}
	
	 class TInfo{
		private String taskRowkey=null;
		private String nextRowkey=null;
		private String jobID=null;
		private String file=null;
		private boolean isEnd=false;
		private String queue=null;
		private String ctype=null;
		
		TInfo(String rowkey,String jid,String file,String nRowkey,String currentType){
			this.taskRowkey=rowkey;
			this.jobID=jid;
			this.file=file;
			this.nextRowkey=(nRowkey!=null&&nRowkey.trim().length()>3)?nRowkey:null;
			this.queue="spider."+taskRowkey;
			this.ctype=currentType;
		}

		public String getTaskRowkey() {
			return taskRowkey;
		}

		

		public String getJobID() {
			return jobID;
		}

		

		public void setTaskRowkey(String taskRowkey) {
			this.taskRowkey = taskRowkey;
		}

		public void setJobID(String jobID) {
			this.jobID = jobID;
		}

		public boolean isEnd() {
			return isEnd;
		}

		public void setEnd(boolean isEnd) {
			this.isEnd = isEnd;
		}

		public String getNextRowkey() {
			return nextRowkey;
		}

		public void setNextRowkey(String nextRowkey) {
			this.nextRowkey = (nextRowkey!=null&&nextRowkey.trim().length()>3)?nextRowkey:null;
		}

		public String getFile() {
			return file;
		}

		public void setFile(String file) {
			this.file = file;
		}

		public String getQueue() {
			return queue;
		}

		public String getCtype() {
			return ctype;
		}

		public void setCtype(String ctype) {
			this.ctype = ctype;
		}
	 }
}
