package net.hubs1.bijia.main;

import net.hubs1.bijia.mapReduce.MRinterface;

public class LoadJob {

	public LoadJob setQueueName(String queueName) {
		this.queueName = queueName;
		return this;
	}
	public LoadJob setJob(MRinterface job) {
		this.mrTask = job;
		return this;
	}
	public LoadJob setBeforeInterval(long beforeInterval) {
		this.beforeInterval = beforeInterval;
		return this;
	}
	public LoadJob setAfterInterval(long afterInterval) {
		this.afterInterval = afterInterval;
		return this;
	}
	public LoadJob setSite(String site) {
		this.site = site;
		return this;
	}
	
	public String getSite() {
		return site;
	}
	public String getQueueName() {
		return queueName;
	}
	public MRinterface getJob() {
		return mrTask;
	}
	public long getBeforeInterval() {
		return beforeInterval;
	}
	public long getAfterInterval() {
		return afterInterval;
	}
	private String queueName;
	private MRinterface mrTask;
	private long beforeInterval;
	private long afterInterval;
	private String site;
	private LoadJob nextJob;
	
	public LoadJob getNextJob() {
		return nextJob;
	}
	public void setNextJob(LoadJob nextJob) {
		this.nextJob = nextJob;
	}
	public void execute(){
		try {
				Thread.sleep(beforeInterval);
				if(mrTask.mapStart(site)){
					Thread.sleep(afterInterval);
					if(nextJob != null)
						nextJob.execute();
				}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
