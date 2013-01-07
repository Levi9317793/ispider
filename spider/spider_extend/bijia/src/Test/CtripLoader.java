package Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.hubs1.bijia.main.CtripMain;
import net.hubs1.bijia.mapReduce.MRExtractCtripPrice;
import net.hubs1.bijia.mapReduce.MRImportCtripPrice;
import net.hubs1.bijia.mapReduce.MRinterface;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class CtripLoader {
	private static final Log logger = LogFactory.getLog(CtripLoader.class);

	public static void main(String[] args) throws Exception {
		// try {
		// Hbase2HbaseExtract.getInstance().select();
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("192.168.20.21");
		String queueName = "spider.ctrip.bsg";
		long loopInterval = 1000 * 60 * 5 ;
		while (true) {
			logger.info("loop " + queueName + "......");
			Connection connection = factory.newConnection();
			Channel channel = connection.createChannel();
			channel.queueDeclare(queueName, true, false, false, null);
			channel.basicQos(1);
			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, false, consumer);
			QueueingConsumer.Delivery delivery = consumer
					.nextDelivery(loopInterval);
			if (delivery != null) {

				channel.basicReject(delivery.getEnvelope().getDeliveryTag(),
						true);
				// close rabbitMq channel
				channel.close();
				connection.close();
				Thread.sleep(loopInterval);
			} else {
				// delete queue,close rabbitMq channel
				channel.close();
				connection.close();
				Thread.sleep(loopInterval);
			}

		}
	}

}
