package net.hubs1.spider.download;
public interface DownloadStatus {
public static enum Status{STOP,RECEIVE,START};
	
	void reportStatus(String taskID,Status status); 
}
