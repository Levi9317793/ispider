package net.hubs1.bijia.main;


import net.hubs1.bijia.dao.Hbase2MysqlCtripPrice;
import net.hubs1.bijia.mapReduce.MRExtractCtripPrice;
import net.hubs1.bijia.mapReduce.MRExtractQunarBijia;
import net.hubs1.bijia.mapReduce.MRImportCtripMedal;
import net.hubs1.bijia.mapReduce.MRImportCtripPrice;
import net.hubs1.bijia.mapReduce.MRImportQunarPrice;
import net.hubs1.bijia.mapReduce.MRinterface;
import net.hubs1.bijia.mapReduce.MRExtractQunarPrice;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import Test.HbaseClient;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;

public class CtripMain {
	
	private static final long loopInterval = 1000 * 60 * 5;//5分钟
	private static final Log logger = LogFactory.getLog(CtripMain.class);
	private static boolean flag = false;
	/**
	 * Monitor download page queue ,if null ,start job
	 * @throws Exception
	 */
	public static void loop(LoadJob job) {
		// TODO Auto-generated method stub
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("192.168.20.21");
	    while (true) {
	    	try{
		    	logger.info("loop "+job.getQueueName()+"......");
			    Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();
				channel.queueDeclare(job.getQueueName(), true, false, false, null);
				channel.basicQos(1);
				QueueingConsumer consumer = new QueueingConsumer(channel);
			    channel.basicConsume(job.getQueueName(), false, consumer);
			    QueueingConsumer.Delivery delivery = consumer.nextDelivery(loopInterval);
	        	if(delivery!=null){
	        		
	        		channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);
	        		channel.close();
	        		connection.close();
	        		Thread.sleep(loopInterval);
	        		flag = true;
	        	}
	        	else{
	        		channel.close();
	        		connection.close();
	        		if(flag){
		        		job.execute();
				        flag = false;
	        		}	
	        	}
	    	}catch(Exception e){e.printStackTrace();}

        }
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LoadProperty property = new LoadProperty();
		
		LoadJob qunarImportPrice = new LoadJob();
		qunarImportPrice.setJob(new MRImportQunarPrice())
			.setSite("qunar_all");
		
		LoadJob qunarExtractPrice = new LoadJob();
		qunarExtractPrice.setQueueName(property.qunarQueue)
			.setJob(new MRExtractQunarPrice())
			.setSite("qunar_all")
			.setAfterInterval(1000 * 60 *5)
			.setNextJob(qunarImportPrice);
		loop(qunarExtractPrice);
	} 

}
