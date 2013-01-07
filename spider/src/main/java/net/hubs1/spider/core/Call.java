package net.hubs1.spider.core;

import java.util.List;
import java.util.Map;

public interface Call extends Command{
	Map<String,List<Command>> commandMap();
}
