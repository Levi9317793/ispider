package net.hubs1.spider.core.action;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import net.hubs1.spider.scheduler.SpiderScheduler_back;
import net.hubs1.spider.store.RecordsBean;

public class TaskInfo {
	private static Logger log=Logger.getLogger(TaskInfo.class);
	public enum Status{NEW,EXTRACTING,DOWNLOADING,RUNNING,SUCCESS,FAIL};
	public enum Type{EXTRACT,URL,FORMAT};
	
	private String taskID=null;
	private String name = null;
	private String desc = null;
	private String type = null;
	private boolean increment = true;
	private String fixedtime = null;
	private int runedNum = 0;
	private Status currentStatus = null;
	private String preRunResult = null;
	private String preRuntime = null;
	private String nextRuntime = null;
	private String firstRuntime = null;
	private String creatorIP = null;
	private String file=null;
	private String queue=null;
	private String startRange=null;
	private String endRange=null;
	private String nextTask=null;
	private boolean isRunNow=false;
	
	private Date startDate=null;
	private Date endDate=null;
	private String hadoopJobID=null;
	
	public TaskInfo(){}
	
	public TaskInfo(RecordsBean rb){
		Map<String,byte[]> attributeMap=rb.attribute2Map();
		
		this.taskID=rb.getRowkeyStr();
		this.name=getStr(attributeMap.get("name"));
		this.type=getStr(attributeMap.get("type"));
		this.startRange=getStr(attributeMap.get("startrange"));
		this.endRange=getStr(attributeMap.get("endrange"));
		this.file=getStr(attributeMap.get("file"));
		this.queue=getStr(attributeMap.get("queue"));
		this.currentStatus=Status.valueOf(getStr(attributeMap.get("cstatus")));
		this.firstRuntime=getStr(attributeMap.get("firstruntime"));
		this.nextRuntime=getStr(attributeMap.get("nextruntime"));
		this.preRuntime=getStr(attributeMap.get("preruntime"));
		this.runedNum=Integer.parseInt(getNumStr(attributeMap.get("runnum")));
		this.nextTask=getStr(attributeMap.get("nexttask"));
		this.increment="yes".equalsIgnoreCase(getStr(attributeMap.get("increment")));
		this.fixedtime=getStr(attributeMap.get("fixedtime"));
		this.creatorIP=getStr(attributeMap.get("creatorIP"));
		this.isRunNow="true".equalsIgnoreCase(getStr(attributeMap.get("runNow")));
		this.hadoopJobID=getStr(attributeMap.get("jobid"));
		
		String sd=getStr(attributeMap.get("startDate"));
		String ed=getStr(attributeMap.get("endDate"));
		
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		try{
			if(sd!=null){
				startDate=sdf.parse(sd);
			}
			if(ed!=null){
				endDate=sdf.parse(ed);
			}
		}catch(ParseException e){
			log.error(e);
		}
	}
	
	private String getStr(byte[] bytes){
		if(bytes==null||bytes.length==0) return null;
		String str= new String(bytes);
		return str.trim().length()==0?null:str;
	}
	
	private String getNumStr(byte[] bytes){
		if(bytes==null||bytes.length==0) return "-1";
		String str= new String(bytes);
		return str.trim().length()==0?"-1":str;
	}
	public String getTaskID() {
		return taskID;
	}
	public void setTaskID(String taskID) {
		this.taskID = taskID;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public boolean isIncrement() {
		return increment;
	}
	public void setIncrement(boolean increment) {
		this.increment = increment;
	}
	public String getFixedtime() {
		return fixedtime;
	}
	public void setFixedtime(String fixedtime) {
		this.fixedtime = fixedtime;
	}
	public int getRunedNum() {
		return runedNum;
	}
	public void setRunedNum(int runedNum) {
		this.runedNum = runedNum;
	}
	public Status getCurrentStatus() {
		return currentStatus;
	}
	public void setCurrentStatus(Status currentStatus) {
		this.currentStatus = currentStatus;
	}
	public String getPreRunResult() {
		return preRunResult;
	}
	public void setPreRunResult(String preRunResult) {
		this.preRunResult = preRunResult;
	}
	public String getPreRuntime() {
		return preRuntime;
	}
	public void setPreRuntime(String preRuntime) {
		this.preRuntime = preRuntime;
	}
	public String getNextRuntime() {
		return nextRuntime;
	}
	public void setNextRuntime(String nextRuntime) {
		this.nextRuntime = nextRuntime;
	}
	public String getFirstRuntime() {
		return firstRuntime;
	}
	public void setFirstRuntime(String firstRuntime) {
		this.firstRuntime = firstRuntime;
	}
	public String getCreatorIP() {
		return creatorIP;
	}
	public void setCreatorIP(String creatorIP) {
		this.creatorIP = creatorIP;
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
	public void setQueue(String queue) {
		this.queue = queue;
	}
	public String getStartRange() {
		return startRange;
	}
	public void setStartRange(String startRange) {
		this.startRange = startRange;
	}
	public String getEndRange() {
		return endRange;
	}
	public void setEndRange(String endRange) {
		this.endRange = endRange;
	}
	public String getNextTask() {
		return nextTask;
	}
	public void setNextTask(String nextTask) {
		this.nextTask = nextTask;
	}

	public boolean isRunNow() {
		return isRunNow;
	}

	public void setRunNow(boolean isRunNow) {
		this.isRunNow = isRunNow;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public String getHadoopJobID() {
		return hadoopJobID;
	}

	public void setHadoopJobID(String hadoopJobID) {
		this.hadoopJobID = hadoopJobID;
	}
	
}
