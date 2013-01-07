package net.hubs1.spider.scheduler;

import java.util.Calendar;
import java.util.Date;

import net.hubs1.spider.core.action.TaskAction;

import org.apache.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.CronScheduleBuilder;

/**
 * @author 董明
 * 
 */
public class QuartzManager {
	private static Logger log=Logger.getLogger(QuartzManager.class);
	private  SchedulerFactory sf = null;
	private  Scheduler scheduler=null;
	private static QuartzManager qm=null;
	
	private QuartzManager() throws SchedulerException{
		sf = new StdSchedulerFactory();
		scheduler= sf.getScheduler();
	}
	
	public static synchronized   QuartzManager getInstance(){
		if(qm==null)
			try {
				qm=new QuartzManager();
			} catch (SchedulerException e) {
				log.error("init quartz task is error:",e);
				System.exit(1);
			}
		return qm;
	}
	
	public  void addCronJob(String taskID, TaskAction job, String time,
			boolean isNow) {
		log.info(" add cron job:"+taskID+", cron expression:"+time+" isNow:"+isNow);

		JobDetail jobDetail = JobBuilder.newJob(job.getClass())
										.usingJobData("taskID",job.getTaskID())
										.storeDurably()
										.requestRecovery()
										.withIdentity(taskID, "spider").build();
		// 触发器
		TriggerBuilder<CronTrigger> tb = TriggerBuilder.newTrigger()
				.withIdentity(taskID, "spider")
				.withSchedule(CronScheduleBuilder.cronSchedule(time));

		if (isNow)
			tb.startNow();

		CronTrigger trigger = tb.build();
		try {
			scheduler.scheduleJob(jobDetail, trigger);
			if (!scheduler.isStarted() || scheduler.isShutdown())
				scheduler.start();
		} catch (SchedulerException e) {
			log.error(" add run cron  job  is error:"+taskID+" the crom expression is:"+time,e);
		}
	}

	public  void addCronJob(String taskID, TaskAction job, String time,
			Date startDate, Date endDate) {
		
		log.info(" add run crom job:["+taskID+"],the cron expression is:"+time);
	
		JobDetail jobDetail = JobBuilder.newJob(job.getClass())
				.usingJobData("taskID",job.getTaskID())
				.withIdentity(taskID, "spider").build();
		// 触发器
		TriggerBuilder<CronTrigger> tb = TriggerBuilder.newTrigger()
				.withIdentity(taskID, "spider")
				.withSchedule(CronScheduleBuilder.cronSchedule(time));

		if (startDate != null)
			tb.startAt(startDate);

		if (endDate != null)
			tb.endAt(endDate);

		CronTrigger trigger = tb.build();
		try {
			scheduler.scheduleJob(jobDetail, trigger);
			if (!scheduler.isStarted() || scheduler.isShutdown())
				scheduler.start();
		} catch (SchedulerException e) {
			log.error(" add run cron  job  is error:"+taskID+" the crom expression is:"+time,e);
		}		
	}

	/**
	 * @param taskID
	 * @param job
	 * @param date
	 * @throws Exception
	 */
	public  void addOneJob(String taskID, TaskAction job, Date date){
		
		removeJob(taskID);
		
		log.info(" add run one of job:["+taskID+"]");
		
		JobDetail jobDetail = JobBuilder.newJob(job.getClass())
				.usingJobData("taskID",job.getTaskID())
				.withIdentity(taskID, "spider").build();
		// 触发器
		 TriggerBuilder tb = TriggerBuilder.newTrigger()
				.withIdentity(taskID, "spider");
		 		if(date==null)
		 			tb.startNow();
		 		else
		 			tb.startAt(date);
		Trigger trigger=tb.build();
		
		try {
			scheduler.scheduleJob(jobDetail, trigger);
			if (!scheduler.isStarted() || scheduler.isShutdown())
				scheduler.start();
		} catch (SchedulerException e) {
			log.error(" add run one of job  is error:"+taskID,e);
		}
	}

	/**
	 * 添加一个只运行一次的任务。任务在当前时间之后的指定小时数后开始运行。
	 * @param taskID
	 * @param job
	 * @param hours 在当前时间后的小时数。
	 * @throws Exception
	 */
	public  void addOneJob(String taskID, TaskAction job, int hours){
		
		Calendar c = Calendar.getInstance();
		c.set(Calendar.HOUR_OF_DAY, c.get(Calendar.HOUR_OF_DAY) + hours);

		addOneJob(taskID, job, c.getTime());
	}

	/**
	 * 添加一个定时任务，使用默认的任务组名，触发器名，触发器组名
	 * 
	 * @param jobName
	 *            任务名
	 * @param job
	 *            任务
	 * @param time
	 *            时间设置，参考quartz说明文档
	 * @throws SchedulerException
	 * @throws ParseException
	 */
	public  boolean removeJob(String taskID)  {
		
		JobKey jk=new JobKey(taskID, "spider");
		try {
			if(scheduler.checkExists(jk)){
				log.info(" remove ["+taskID+"] form job list ");
				scheduler.deleteJob(jk);
			}
			return true;
		} catch (SchedulerException e) {
			log.error(" remove job form job list is error:"+taskID,e);
			return false;
		}
	}
	
	public void shutdown(){
		try {
			if(this.scheduler.isStarted()){
				this.scheduler.shutdown();
			}
		} catch (SchedulerException e) {
			log.error(" stop scheduler was fail ",e);
		}
	}
}
