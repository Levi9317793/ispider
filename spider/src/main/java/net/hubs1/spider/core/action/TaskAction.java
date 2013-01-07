package net.hubs1.spider.core.action;


import java.io.BufferedInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import net.hubs1.spider.core.DBClient;
import net.hubs1.spider.rabbit.RabbitMQClient;
import net.hubs1.spider.store.DBFactory;
import net.hubs1.spider.store.KVBean;
import net.hubs1.spider.store.RecordsBean;

public class TaskAction implements Runnable,Job{
	private static Logger log=Logger.getLogger(TaskAction.class);
	private static DBClient dbClient=DBFactory.getDBClient();
	
	//public enum TaskStatus{NEW,EXTRACTING,DOWNLOADING,RUNNING,SUCCESS,FAIL};
	
	
	private String taskID=null;
	
	public TaskAction(String taskid){
		this.taskID=taskid;
	}
	
	public TaskAction(){}
	
	public synchronized static boolean runUrlTask(String taskrowkey,String formatPath,String rowkeyStart,String rowkeyEnd,Long timestamp,String nextTask){
//		String sendmessagePath=SpiderContext.getConfig().getMrJarFilLocalPath();
		String queue="spider."+taskrowkey;
		
		StringBuilder sb=new StringBuilder("hadoop jar ");//command
		sb.append(" /home/hadoop/spider_task.jar ") //jar
		  .append(" net.hubs1.spider.hbase.mapredure.SendMessageMR ")//main class
		  .append(queue==null?"null":queue).append(" ") 
		  .append(rowkeyStart==null?"null":rowkeyStart).append(" ") 
		  .append(rowkeyEnd==null?"null":rowkeyEnd).append(" ");
		if(timestamp!=null)
			sb.append( timestamp);
		
		log.info("  run MR Job ： "+ sb.toString());
		
		
		try {
			Process p=Runtime.getRuntime().exec(sb.toString());
			String jobID=getHadoopJobID(p);
			if(jobID!=null){
				log.info(" MR JOBID is:"+jobID);
				RecordsBean rb=new RecordsBean("base", taskrowkey);
				rb.setAttributes(new KVBean[]{new KVBean("cstatus",TaskInfo.Status.EXTRACTING.name()),new KVBean("jobid",jobID),new KVBean("queue",queue)});
				dbClient.writeData("spider_task", rb);
				TaskMonitor.getTaskMonitor().addTask2Monitor(taskrowkey, jobID, formatPath, nextTask,"mr");
			}else{
				log.error(" don't catch mr jobid");
				log.error(" the hadoop job error info is:"+getHadoopJobError(p));
			}
			p.destroy();
			return true;
		} catch (Exception e) {
			log.error(" the local task is invoke fail!",e);
		}
		return false;
	}
	
	
	public synchronized static boolean runFormatTask(String taskrowkey,String formatFile,String nextTask){
		log.info(" run local command： dowload by format file " );
		//sendDownloadComm(1, new KVBean("format:"+taskrowkey,formatFile));
		sendDownloadComm("format", taskrowkey, formatFile, 1);
		RecordsBean rb=new RecordsBean("base", taskrowkey);
		rb.setAttributes(new KVBean[]{new KVBean("cstatus",TaskInfo.Status.DOWNLOADING.name())});
		dbClient.writeData("spider_task", rb);
		TaskMonitor.getTaskMonitor().addTask2Monitor(taskrowkey,null,null, nextTask,"database");
		return true;
	}
	
	public synchronized static boolean runExtractTask(String taskrowkey,String formatFile,String rowkeyStart,String rowkeyEnd,Long timestamp,String nextTask){
		StringBuilder sb=new StringBuilder("hadoop jar ");//command
		sb.append(" /home/hadoop/spider_task.jar ") //jar
		  .append(" net.hubs1.spider.hbase.mapredure.DataExtractMR  ")//main class
		  .append(formatFile).append(" ")
		  .append(taskrowkey).append(" ")
		  .append(rowkeyStart==null?"null":rowkeyStart).append(" ") 
		  .append(rowkeyEnd==null?"null":rowkeyEnd).append(" ");
		if(timestamp!=null){
			sb.append(timestamp);
		}
		try {
			log.info(" run MR Job ： " +sb.toString());
			Process p=Runtime.getRuntime().exec(sb.toString());
			String jobID=getHadoopJobID(p);
			if(jobID!=null){
				log.info(" MR JOBID is:"+jobID);
				RecordsBean rb=new RecordsBean("base", taskrowkey);
				rb.setAttributes(new KVBean[]{new KVBean("cstatus",TaskInfo.Status.EXTRACTING.name()),new KVBean("jobid",jobID)});
				dbClient.writeData("spider_task", rb);
				TaskMonitor.getTaskMonitor().addTask2Monitor(taskrowkey, jobID, null,nextTask,"mr");
			}else{
				log.error(" don't catch mr jobid");
				log.error(" the hadoop job error info is:"+getHadoopJobError(p));
			}
			
			p.destroy();
			return true;
		} catch (Exception e) {
			log.error(" the local task is invoke fail!",e);
		}
		return false;
	}
	
	public static void sendDownloadComm(String type,String taskID,String data,int num){
		RabbitMQClient rabbit=RabbitMQClient.getInstance();
		KVBean kv=new KVBean(type+":"+taskID, data);
		for(int i=0;i<num;i++){
			rabbit.sendPubMessage(kv);
		}
	}

	
	private static String getHadoopJobID(Process p){
		BufferedInputStream bis=new BufferedInputStream(p.getInputStream());
		
		byte[] info=new byte[256];
		try {
			bis.read(info);
		} catch (IOException e) {
			log.error("catch hadoop job id is error ",e);
		}
		
		
		String jobID=new String(info);
		jobID=jobID.replaceAll("[^-\\w\\p{Punct}]", "");
		if(jobID.startsWith("JobID=")){
			jobID=jobID.substring(jobID.indexOf("=")+1);
			return jobID;
		}
		return null;
	}
	
	
	private static String getHadoopJobError(Process p){
		BufferedInputStream bis=new BufferedInputStream(p.getErrorStream());
		int length=256;
		int realLength=0;
		byte[] bytes=new byte[length];
		
		byte r=-1;
		try {
			while((r=(byte)bis.read())!=-1){
				if(realLength==length){
					length+=256;
					byte[] ls_b=new byte[length];
					System.arraycopy(bytes, 0, ls_b, 0, realLength);
				}
				bytes[realLength++]=r;
			}
		} catch (IOException e) {
			log.error(" don't read hadoop job error info ",e);
		}
		
		String errorMessage=new String(bytes,0,realLength);
		return errorMessage;
	}
	
	
	public static String runTask(String rowkey){
		return runTask(rowkey, true);
	}
	
	/**
	 * @param rowkey
	 * @param icr 是否增量运行
	 * @return
	 */
	public static String runTask(String rowkey,boolean icr){
		if(rowkey==null||rowkey.trim().length()<3) return "非法的任务ID";
		
		DBClient db=DBFactory.getDBClient();
		
		RecordsBean rb=db.get("spider_task", "base", rowkey, null);
		
		Boolean isOK=null;
		String message=null;
		if(rb!=null){
			Map<String,byte[]> attributeMap=rb.attribute2Map();
			String type=new String(attributeMap.get("type"));
			String startRange=attributeMap.get("startrange")!=null?new String(attributeMap.get("startrange")):null;
			String endRange=attributeMap.get("endrange")!=null?new String(attributeMap.get("endrange")):null;
			String file=attributeMap.get("file")!=null?new String(attributeMap.get("file")):null;
			String state=new String(attributeMap.get("cstatus"));
			String preRunTime=attributeMap.get("preruntime")!=null?new String(attributeMap.get("preruntime")):null;
			String runNumStr=new String(attributeMap.get("runnum"));
			String nextRowkey=attributeMap.get("nexttask")!=null?new String(attributeMap.get("nexttask")):null;
			String increment=attributeMap.get("increment")!=null?new String(attributeMap.get("increment")):null;
			
			int runNum=Integer.parseInt(runNumStr);
			SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			Long time=null;
			if(preRunTime!=null){
				try {
					time=sdf.parse(preRunTime).getTime();
				} catch (ParseException e) {
					log.error(e);
				}
			}
			
			if("new".equals(state)||!icr||increment==null||!"yes".equalsIgnoreCase(increment)){
				time=null;
			}
			
			if(!state.contains("ING")){
				if("url".equals(type)) 
					isOK=TaskAction.runUrlTask(rowkey,file,startRange, endRange,time,nextRowkey);
				else if("format".equals(type)) 
					isOK=TaskAction.runFormatTask(rowkey,file,nextRowkey);
				else if("extract".equals(type)) 
					isOK=TaskAction.runExtractTask(rowkey,file, startRange, endRange, time,nextRowkey);
			}else{
				message="任务已在运行！";
			}
		
			if(isOK!=null&&isOK){
				KVBean runtime=null;
				KVBean runn=null;
				if(runNum<1){
					runtime=new KVBean("firstruntime",sdf.format(new Date()));
				}else{
					 runtime=new KVBean("preruntime",sdf.format(new Date()));
				}
				runn=new KVBean("runnum",String.valueOf(runNum+1));
				rb.setAttributes(new KVBean[]{runtime,runn});
				
				db.writeData("spider_task", rb);
			}
		}
		if(message==null){
			message=isOK==null?"任务不存在":(isOK?"任务已提交运行":"任务提交运行失败！");
		}
		log.info(" 任务运行情况，任务ID["+rowkey+"], "+message);
		return message;
	}
	
	public static void updateTaskStatus(String rowkey,TaskInfo.Status status){
		DBClient db=DBFactory.getDBClient();
		RecordsBean rb=new RecordsBean("base",rowkey);
		
		KVBean state=new KVBean("cstatus",status.name());
		rb.setAttributes(new KVBean[]{state});
		db.writeData("spider_task", rb);
	}

	@Override
	public void run() {
		TaskAction.runTask(this.taskID);
	}

	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		String taskid=context.getJobDetail().getJobDataMap().getString("taskID");
		log.info(" ===start task:"+taskid);
		if(taskid==null) return;
		TaskAction.runTask(taskid);
	}

	public String getTaskID() {
		return taskID;
	}

	public void setTaskID(String taskID) {
		this.taskID = taskID;
	}
	
}
