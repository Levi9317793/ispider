package net.hubs1.spider.hbase.mapredure;

import java.io.IOException;

import net.hubs1.spider.core.config.SpiderContext;
import net.hubs1.spider.rabbit.RabbitMQClient;
import net.hubs1.spider.store.KVBean;
import net.hubs1.spider.util.SpiderUtil;

import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;

public class SendMessageMR {
	 private static Logger log=Logger.getLogger(SendMessageMR.class);
//	 protected static Set<String> queueNameSet=new HashSet<String>();
	 
	 public static class SendMapper extends TableMapper<ImmutableBytesWritable, Put> {
		 private RabbitMQClient mq=null;
		 private String queueName=null;
		 public void setup(Context context) {
			 queueName=context.getConfiguration().get("send.message.to.queue");
			 SpiderContext.initSpiderContentByDefaultFile();
			 mq=RabbitMQClient.newInstance();
		 }
		 /* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			 	String rowkey=new String(row.get());
			 	
			 	byte[] url=value.getValue("base".getBytes(), "url".getBytes());
			 	if(url!=null&&url.length>0){
				 	KVBean kv=new KVBean(rowkey,url);
				 	mq.sendMessage(kv,queueName);
			 	}
		 }
		
		public void cleanup(Context context) {
	  		if(mq!=null)
				try {
					mq.close();
				} catch (IOException e) {
					log.error(" don't close mq ");
				}
	  	}
	}

	public static void main(String[] args) throws Exception {
		if( !(args.length==3 ||args.length==4)){
			log.error(" send message params number is error ,only 2 or 3.the format is  [start rowkey,end rowkey,timestamp]");
			log.error(" the params length  is:"+args.length+" data is"+SpiderUtil.array2Str(args));
			//throw new RuntimeException(" send message params number is error ,only 2 or 3.the format is  [start rowkey,end rowkey,timestamp], the real params length  is:"+args.length+" ,data is"+SpiderUtil.array2Str(args));
			System.err.println(" send message params number is error ,only 2 or 3.the format is  [start rowkey,end rowkey,timestamp]");
			System.err.println(" the params length  is:"+args.length+" data is"+SpiderUtil.array2Str(args));
			return ;
		}
		
		try{
			SpiderContext.initSpiderContentByDefaultFile();
			
			String queue="null".equals(args[0])?null:args[0];
			String startRowkey="null".equals(args[1])?null:args[1];
			String endRowkey="null".equals(args[2])?null:args[2];
			
			if(queue==null||(startRowkey==null&&endRowkey==null)){
				throw new RuntimeException(" the queue is must set,strat rowkey or end rowkey must set one" );
			}
			
			String timestamp=args.length>3?args[3]:null;
			
			long minTimestamp=-1;
			if(timestamp!=null){
				minTimestamp=Long.parseLong(timestamp);
			}
			
			Configuration config = HBaseConfiguration.create();
			config.set("send.message.to.queue", queue);
			Job job = new Job(config,"send_url_to["+queue+"]");
			job.setJarByClass(SendMessageMR.class);    
					
			job.setMapSpeculativeExecution(false); ////禁止推测
	
			Scan scan = new Scan();
			
			if(minTimestamp!=-1){
				//实现增量下载
				scan.setTimeRange(minTimestamp,System.currentTimeMillis());
			}
			
			scan.addColumn("base".getBytes(),"url".getBytes());
			scan.setCaching(50);
			scan.setCacheBlocks(false);  
			if(startRowkey!=null) scan.setStartRow(startRowkey.getBytes());
			if(endRowkey!=null) scan.setStopRow(endRowkey.getBytes());
				        
			TableMapReduceUtil.initTableMapperJob(
				"download_url",      // input table
				scan,	          // Scan instance to control CF and attribute selection
				SendMapper.class,   // mapper class
				null,	          // mapper output key
				null,	          // mapper output value
				job);
			job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper
			job.setNumReduceTasks(0);
			job.submit();
			System.out.println("JobID="+job.getJobID());
		}catch(Exception e){
			e.printStackTrace(System.err);
		}catch(Error e){
			e.printStackTrace(System.err);
		}
	}
}
