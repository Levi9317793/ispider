package net.hubs1.spider.web;

import java.io.IOException;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;

import net.hubs1.spider.core.config.SpiderContext;
import net.hubs1.spider.rabbit.RabbitMQClient;
import net.hubs1.spider.scheduler.QuartzManager;

public class SpiderContextListener  implements ServletContextListener {
	private static Logger log=Logger.getLogger(SpiderContextListener.class);
	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		try {
			RabbitMQClient.getInstance().close();
		} catch (IOException e) {
			log.error(e);
		}
		QuartzManager.getInstance().shutdown();
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		SpiderContext.init();
	}
}
