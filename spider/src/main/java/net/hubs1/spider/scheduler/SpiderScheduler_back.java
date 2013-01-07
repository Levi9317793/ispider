package net.hubs1.spider.scheduler;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import net.hubs1.spider.core.DBClient;
import net.hubs1.spider.core.action.TaskAction;
import net.hubs1.spider.core.action.TaskInfo;
import net.hubs1.spider.core.config.SpiderContext;
import net.hubs1.spider.rabbit.RabbitMQClient;
import net.hubs1.spider.store.DBFactory;
import net.hubs1.spider.store.KVBean;
import net.hubs1.spider.store.RecordsBean;

public class SpiderScheduler_back {
	private static Logger log=Logger.getLogger(SpiderScheduler_back.class);
	private Set<TaskInfo> taskList=new HashSet<TaskInfo>();
	
	private static SpiderScheduler_back ss=new SpiderScheduler_back();
	private ScheduledExecutorService execute=Executors.newScheduledThreadPool(30);
	
	private SpiderScheduler_back(){}
	
	public static SpiderScheduler_back getInstance(){
		return ss;
	}
	
	public void init(){
		DBClient dbclient=DBFactory.getDBClient();
		List<RecordsBean> rbs=dbclient.scan("spider_task", "base", null, null,new String[0]);
		for(RecordsBean rb:rbs){
			TaskInfo ti=new TaskInfo(rb);
			addFixedTimeTask(ti);
		}
	}
	
	public void addFixedTimeTask(TaskInfo ti){
		if(ti==null) return ;
		String fixedtime=ti.getFixedtime();
		if(fixedtime==null) return;
		
		if(fixedtime.matches("^\\d{2}:\\d{2}$")){
			taskList.add(ti);
			String[] str=fixedtime.split(":");
			
			int hour=Integer.parseInt(str[0].trim());
			int minute=Integer.parseInt(str[1].trim());
			
			Calendar c=Calendar.getInstance();
			c.set(Calendar.HOUR_OF_DAY, hour);
			c.set(Calendar.MINUTE,minute);
			c.set(Calendar.SECOND, 0);
			
			long delay=c.getTimeInMillis()-System.currentTimeMillis();
			if(delay<0){
				Calendar n=Calendar.getInstance();
				n.set(Calendar.DAY_OF_MONTH, n.get(Calendar.DAY_OF_MONTH)+1);
				n.set(Calendar.HOUR_OF_DAY, hour);
				n.set(Calendar.MINUTE,minute);
				n.set(Calendar.SECOND, 0);
				delay= n.getTimeInMillis()-System.currentTimeMillis();
			}
			execute.scheduleAtFixedRate(new TaskAction(ti.getTaskID()), delay,1000*60*60*24, TimeUnit.MILLISECONDS);
		}else{
			if(fixedtime.matches("^\\d+$")&&ti.getNextRuntime()!=null){
				/*此时的下次运行时间可能不正确，需要在任务监控通知任务结束时重新计算*/
				if(ti.getCurrentStatus().name().contains("ING")) return;
				
				taskList.add(ti);
				SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm");
				
				Date date=null;
				try {
					date = sdf.parse(ti.getNextRuntime());
				} catch (ParseException e) {
					log.error(e);
				}
				
				if(date!=null){
					long delay=date.getTime()-System.currentTimeMillis();
					execute.schedule(new TaskAction(ti.getTaskID()), delay<0?0:delay, TimeUnit.MILLISECONDS);
				}
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
			taskList.add(task);
			
			int hour=Integer.parseInt(fixedtime);
			
			SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm");
			Calendar c=Calendar.getInstance();
			c.set(Calendar.HOUR_OF_DAY,c.get(Calendar.HOUR_OF_DAY)+hour);
			c.set(Calendar.SECOND, 0);
			
			long delay=c.getTimeInMillis()-System.currentTimeMillis();
			
			execute.schedule(new TaskAction(task.getTaskID()), delay, TimeUnit.MILLISECONDS);
			RecordsBean rb=new RecordsBean("base", task.getTaskID());
			rb.setAttributes(new KVBean[]{new KVBean("nextruntime",sdf.format(c.getTime()))});
			DBFactory.getDBClient().writeData("spider_task", rb);
		}
	}
	
	public static void main(String[] args) throws IOException {
		//SpiderScheduler ss=new SpiderScheduler();
		SpiderContext.initSpiderContentByDefaultFile();
		RabbitMQClient rabbit=RabbitMQClient.getInstance();
		
		
		KVBean kv=new KVBean();
		kv.setKey("url:");
		kv.setValue("spider.ctrip.hotel");
//		kv.setKey("format");
//		kv.setValue("/spider/formats/booking_com/booking_com.city.url.txt");
//		rabbit.sendPubMessage(kv);
		rabbit.sendPubMessage(kv);
		rabbit.sendPubMessage(kv);
		
		rabbit.close();
	}
	
}