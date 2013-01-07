package net.hubs1.spider.core;

import java.util.Map;

public interface Command{
	//String CURRENT="current";
	void execute(Object... params);
	
}
