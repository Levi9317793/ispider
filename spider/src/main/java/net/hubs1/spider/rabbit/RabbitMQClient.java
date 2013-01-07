package net.hubs1.spider.rabbit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

import net.hubs1.spider.core.config.SpiderContext;
import net.hubs1.spider.message.KVCode;
import net.hubs1.spider.store.KVBean;

import com.google.protobuf.ByteString;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;


public class RabbitMQClient {
	private static Logger log=Logger.getLogger(RabbitMQClient.class);
	
	private String host =null;
	private int port =-1;
	private Connection connection=null;
	private ThreadLocal<Map<String,Channel>> rabbitChannels=null ;
	private ThreadLocal<Map<String,QueueingConsumer>> consumers=null ;
	private  static RabbitMQClient rabbit=null;
	private String pubQueue=null;;
	private static final long timeout=1000*60*5;//5分钟;
	
	public synchronized  static RabbitMQClient getInstance(){
		if(rabbit==null) rabbit=new RabbitMQClient();
		return rabbit;
	}
	
	public static RabbitMQClient newInstance(){
		return new RabbitMQClient();
	}
	
	private RabbitMQClient(){
		this.host=SpiderContext.getConfig().getMqIP();
		this.port=SpiderContext.getConfig().getMqPort();
		this.pubQueue=SpiderContext.getConfig().getPublicQueueName();
		init();
	}
	
	private synchronized void init(){
		if(this.connection!=null&&this.connection.isOpen()) return;
		try{
			  ConnectionFactory factory = new ConnectionFactory();
		      factory.setHost(host);
		      factory.setPort(port);
		      connection = factory.newConnection();
		      rabbitChannels=new ThreadLocal<Map<String,Channel>>();
		      consumers=new ThreadLocal<Map<String,QueueingConsumer>>();
			}catch(Exception e){
				log.error("init is error",e);
			}
	}
	
	public Channel getChannel(String queue){
		Map<String,Channel> cmap=rabbitChannels.get();
		Channel channel=null;
		if(cmap==null){
			cmap=new HashMap<String,Channel>();
		    rabbitChannels.set(cmap);
		}
		
		channel=cmap.get(queue);
		if(channel==null){
			try{
				channel = connection.createChannel();
				channel.basicQos(1);
				channel.queueDeclare(queue, true, false, false, null);
			    cmap.put(queue, channel);
			}catch(IOException e){
				log.error("get rabbit mq channel is fail:",e);
			}
		 }
		return channel;
	}
	
	public boolean sendMessage(KVBean kv,String queue,int tryNum) {
		KVCode.KVBean.Builder kvbean=KVCode.KVBean.newBuilder();
		kvbean.setKey(kv.getKey());
		kvbean.setValue( ByteString.copyFrom(kv.getValueByte()));
		
		try{
			getChannel(queue).basicPublish(
							"", 
							queue, 
				            MessageProperties.PERSISTENT_TEXT_PLAIN,
				            kvbean.build().toByteArray());
		}catch(ShutdownSignalException  e){
			log.error("connection is close,now,if the trynum is create 0, try open the connection,if success,and try send message,try num is: "+tryNum,e);
			if(tryNum>0){
				init();
				sendMessage(kv, queue,--tryNum);
			}
		}catch(IOException ioe){
			log.error(" send message to queue ["+queue+"] is fail:"+kv,ioe);
			return false;
		}
		return true;
	}
	
	
	public boolean sendMessage(KVBean kv,String queue) {
		return sendMessage(kv, queue, 1);
	}
	
	public boolean sendPubMessage(KVBean message){
		return sendMessage(message, pubQueue);
	}
	
	public QueueingConsumer getConsumer(String queue){
		Map<String,QueueingConsumer> map=this.consumers.get();
		QueueingConsumer consumer=null;
		if(map==null){
			map=new HashMap<String,QueueingConsumer>();
			this.consumers.set(map);
		}
		consumer=map.get(queue);
		if(consumer==null){
			Channel rabbitChannel = getChannel(queue);
		    consumer = new QueueingConsumer(rabbitChannel);
		    try {
				rabbitChannel.basicConsume(queue, false, consumer);
			} catch (IOException e) {
				log.error(" don't create QueueingConsumer",e);
			}
		    map.put(queue, consumer);
		}
		return consumer;
	}
	
	public  MQMessage<KVBean> receivePubMessage(){
		return receiveMessage(pubQueue, 1000, false,1);
	}
	
	public synchronized void closeChannel(String queue){
		Channel channel=getChannel(queue);
		try {
			channel.close();
		} catch (IOException e) {
			log.error("close channel:["+queue+"] is fail!",e);
		}
		this.rabbitChannels.get().put(queue, null);
		this.consumers.get().put(queue, null);
	}
	
	private  MQMessage<KVBean> receiveMessage(String queue,long timeout,boolean isClose,int tryNum){
		try{
			
			QueueingConsumer consumer=getConsumer(queue);
			QueueingConsumer.Delivery delivery = consumer.nextDelivery(timeout);
			
			if(delivery==null){
				if(isClose){
					closeChannel(queue);
					MQMessage<KVBean> m=new MQMessage<KVBean>();
					m.setStatus(MQMessage.Status.STOP);
					return m;
				}
				return null;
			}
			
			KVCode.KVBean.Builder kvbean=KVCode.KVBean.newBuilder();
			kvbean.mergeFrom(delivery.getBody());
			
			KVBean kv=new KVBean(kvbean.getKey(), kvbean.getValue().toByteArray());
			
			log.info("receive message from ["+queue+"]:"+kv.getKey()+"="+kv.getValue());
			return new MQMessage<KVBean>(delivery.getEnvelope().getDeliveryTag(), kv);
		}catch(ShutdownSignalException e){
			log.error("connection is close,now,if the trynum is create 0, try open the connection,if success,and try send message,try num is: "+tryNum,e);
			if(tryNum>0){
				init();
				receiveMessage(queue,timeout,isClose,--tryNum);
			}
		}catch(ConsumerCancelledException e){
			closeChannel(queue);
		}catch(Exception e){
			log.error("receive message is fail:",e);
		}
        return null;
	}
	
	/**
	 * 拒绝消息
	 * @param deliveryTag
	 * @param queue
	 * @param requeue
	 */
	public void rejectMessage(long deliveryTag,String queue,boolean requeue){
		try {
			this.getChannel(queue).basicReject(deliveryTag, requeue);
		} catch (IOException e) {
			log.error(e);
		}
	}
	
	public MQMessage<KVBean> receiveMessage(String queue){
		return receiveMessage(queue, this.timeout, true,1);
	}
	
	/**
	 * 确认消息
	 * @param queue
	 * @param tag
	 * @param multi
	 * @return
	 */
	public boolean confirmMessage(String queue,long tag,boolean multi){
		Channel rabbitChannel=getChannel(queue);
        try {
			rabbitChannel.basicAck(tag, false);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 关闭到rabbitMQ的连接
	 * @throws IOException
	 */
	public synchronized void close() throws IOException {
		if(connection.isOpen())
			connection.close();
	}
	
	public static void main(String[] args) throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {
//		SpiderContext.initSpiderContentByDefaultFile();
//		RabbitMQClient rabbitMqClient=new RabbitMQClient();
//		
//		//rabbitMqClient.sendMessage(new KVBean("test","tst"), "spider_test_qqq");
//		
//		MQMessage<KVBean> message=rabbitMqClient.receiveMessage("spider_test_qqq");
//		if(message!=null){
//			try {
//				Channel channel=rabbitMqClient.getChannel("spider_test_qqq");
//				if(message.getStatus()==MQMessage.Status.STOP) 
//					channel.queueDelete("spider_test_qqq");
//				channel.close();
//			} catch (IOException e) {
//				log.error(e);
//			}
//		}
//		rabbitMqClient.close();
		
		
//		job_201208061759_0110
		
		Configuration cf=new Configuration();
		cf.set("mapred.job.tracker", "spidermaster:50091");
		JobClient jc=null;
		try {
			jc = new JobClient(new org.apache.hadoop.mapred.JobConf(cf));
			JobID ji=JobID.forName("job_201208061759_0110");
			RunningJob rj=jc.getJob(ji);
			System.out.println(rj.getJobState());
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
//		KVBean kv=new KVBean();
//		kv.setKey("test");
//		kv.setValue("1234567890中文值");
//		send.sendKVMessage(kv,"spider_test_123");
//		send.close();
//		send.receiveMessage("spider.agoda.image");
										 
//		RabbitClient receive=new RabbitClient();
//		KVBean kvb=receive.receiveMessage();
//		System.out.println(kvb.getKey()+"==="+kvb.getValue());
	}
}
