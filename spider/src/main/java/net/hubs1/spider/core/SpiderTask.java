package net.hubs1.spider.core;

public interface SpiderTask {
	void init();
	void run(String url,String rowkey);
	void destory();
}
