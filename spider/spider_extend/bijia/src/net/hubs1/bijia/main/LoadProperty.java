package net.hubs1.bijia.main;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

public class LoadProperty {

	public String jdbc;
	public String user;
	public String passwd;
	public String driver;
	public String queue;
	public String bsgQueue;
	public String qunarQueue;
	public LoadProperty(){
		Properties prop = new Properties();
		InputStream in = LoadProperty.class.getClassLoader().getResourceAsStream("conf.properties");
		try {
			prop.load(in);
			jdbc = prop.getProperty("jdbc");
			user = prop.getProperty("user");
			passwd = prop.getProperty("passwd");
			driver = prop.getProperty("driver");
			queue = prop.getProperty("queue");
			bsgQueue = prop.getProperty("bsg_queue");
			qunarQueue = prop.getProperty("qunar_queue");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
