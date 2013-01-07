package net.hubs1.spider.scheduler;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.hubs1.spider.core.DBClient;
import net.hubs1.spider.core.action.TaskAction;
import net.hubs1.spider.core.action.TaskInfo;
import net.hubs1.spider.core.config.SpiderContext;
import net.hubs1.spider.rabbit.RabbitMQClient;
import net.hubs1.spider.store.DBFactory;
import net.hubs1.spider.store.KVBean;
import net.hubs1.spider.store.RecordsBean;

import org.apache.log4j.Logger;

public class SpiderScheduler {
	private static Logger log=Logger.getLogger(SpiderScheduler.class);
	private Set<TaskInfo> taskList=new HashSet<TaskInfo>();
	
	private static SpiderScheduler ss=new SpiderScheduler();
	
	private SpiderScheduler(){}
	
	public static SpiderScheduler getInstance(){
		return ss;
	}
	
	public void init(){
		DBClient dbclient=DBFactory.getDBClient();
		List<RecordsBean> rbs=dbclient.scan("spider_task", "base", null, null,new String[0]);
		SimpleDateFormat sd=new SimpleDateFormat("yyyy-MM-dd HH:mm");
		for(RecordsBean rb:rbs){
			if(rb==null) continue;
			
			TaskInfo ti=new TaskInfo(rb);
			String fixedtime=ti.getFixedtime();
			if(fixedtime==null) continue;
			
			if(fixedtime.matches("^\\d+$")){
				if(ti.getNextRuntime()!=null&&!ti.getCurrentStatus().name().contains("ING")){
					try {
						Date nextDate=sd.parse(ti.getNextRuntime());
						if(ti.getEndDate()==null||(nextDate.getTime()-ti.getEndDate().getTime()<60000)){
							QuartzManager.getInstance().addOneJob(ti.getTaskID(), new TaskAction(ti.getTaskID()),nextDate);
						}
					} catch (ParseException e) {
						log.error("task:"+ti.getTaskID()+" is don't start,it's nextRuntime format error");
					}
				}
			}else{
				QuartzManager.getInstance().addCronJob(ti.getTaskID(), new TaskAction(ti.getTaskID()), fixedtime, false);
			}
		}
	}
	
	/**
	 *
	 * @param ti
	 */
	public void addFixedTimeTask(TaskInfo ti){
		if(ti==null) return ;
		String fixedtime=ti.getFixedtime();
		if(fixedtime==null) return;
		
		
		if(fixedtime.matches("^\\d+$")){
			if(ti.isRunNow()){
				QuartzManager.getInstance().addOneJob(ti.getTaskID(), new TaskAction(ti.getTaskID()),null);
			}else{
				int hours=Integer.parseInt(fixedtime);
				Calendar c = Calendar.getInstance();
				if(ti.getStartDate()!=null){
					c.setTime(ti.getStartDate());
				}
				c.set(Calendar.HOUR_OF_DAY, c.get(Calendar.HOUR_OF_DAY) + hours);
				
				QuartzManager.getInstance().addOneJob(ti.getTaskID(), new TaskAction(ti.getTaskID()), c.getTime());
			}
		}else{
			if(ti.getStartDate()!=null||ti.getEndDate()!=null){
				QuartzManager.getInstance().addCronJob(ti.getTaskID(), new TaskAction(ti.getTaskID()),ti.getFixedtime(),ti.getStartDate(),ti.getEndDate());
			}else{
				QuartzManager.getInstance().addCronJob(ti.getTaskID(), new TaskAction(ti.getTaskID()),ti.getFixedtime(),ti.isRunNow());
			}
		}
	}
	
	
	public void addFixedTimeTask(String taskID){
		RecordsBean rb=DBFactory.getDBClient().get("spider_task", "base",taskID, null);
		if(rb==null) return;
		TaskInfo task=new TaskInfo(rb);
		addFixedTimeTask(task);
	}
	
	public void isSuccessEnd(String taskID){
		if(taskID==null||taskID.trim().length()<2) return ;
		
		TaskInfo task=null;
		for(TaskInfo t:taskList){
			if(t.getTaskID().equals(taskID)){
				task=t;
				break;
			}
		}
		
		if(task==null){
			RecordsBean rb=DBFactory.getDBClient().get("spider_task", "base",taskID, null);
			if(rb!=null)
				task=new TaskInfo(rb);
		}
		if(task==null) return;
		
		String fixedtime=task.getFixedtime();
		
		if(fixedtime==null)return ;
		
		if(fixedtime.matches("^\\d+$")){
			int hours=Integer.parseInt(fixedtime);
			Calendar nextDate = Calendar.getInstance();
			nextDate.set(Calendar.HOUR_OF_DAY, nextDate.get(Calendar.HOUR_OF_DAY) + hours);
			
			SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm");
			RecordsBean rb=new RecordsBean("base", task.getTaskID());
			rb.setAttributes(new KVBean[]{new KVBean("nextruntime",sdf.format(nextDate.getTime()))});
			DBFactory.getDBClient().writeData("spider_task", rb);
			
			if(task.getEndDate()==null||(nextDate.getTimeInMillis()-task.getEndDate().getTime()<60000)){
				QuartzManager.getInstance().addOneJob(task.getTaskID(), new TaskAction(task.getTaskID()),nextDate.getTime());
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		SpiderContext.initSpiderContentByDefaultFile();
		RabbitMQClient rbc=RabbitMQClient.getInstance();
//		KVBean kv1=new KVBean("url:1352432906205","/spider/formats/download/qunar/price/qunar_price");
//		KVBean kv2=new KVBean("url:1352432906205","/spider/formats/download/qunar/price/qunar_price");
//		KVBean kv3=new KVBean("url:1352432906205","/spider/formats/download/qunar/price/qunar_price");
		
		KVBean kv1=new KVBean("url:1348474527904","/spider/formats/download/daodao/hotel/hotel_page");
		KVBean kv2=new KVBean("url:1348474527904","/spider/formats/download/daodao/hotel/hotel_page");
		KVBean kv3=new KVBean("url:1348474527904","/spider/formats/download/daodao/hotel/hotel_page");
		
		rbc.sendMessage(kv1, "spider_public_message");
		rbc.sendMessage(kv2, "spider_public_message");
		rbc.sendMessage(kv3, "spider_public_message");
		rbc.close();
	}
}
