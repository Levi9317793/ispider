package net.hubs1.spider.hbase.mapredure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.hubs1.spider.core.config.SpiderContext;
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
import org.apache.log4j.Logger;

public class DataExtractMR {
	private static Logger log=Logger.getLogger(DataExtractMR.class);
	 public static class Extract extends TableMapper<ImmutableBytesWritable, Put> {
		 private static DataExtract de=null;
		 private static boolean isExtractList=false;
		 private static String family=null;
		 public void setup(Context context) {
			 SpiderContext.initSpiderContentByDefaultFile();
			 String file=context.getConfiguration().get("extract.format.file.path");
			 de=new DataExtract(file,false);
			 Map<String,Object> config=de.getConfig();
			 isExtractList="true".equalsIgnoreCase((String)config.get("isExtractList"));
			
			 family=(String)config.get("targetfamily");
		 }
		 
		 public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			    List<Put> puts=resultToPut(row,value);
		   		if(!puts.isEmpty()){
		   			for(Put put:puts){
		   				if(put!=null) context.write(row, put);
		   			}
		   		}
		  }
		        
	  	private static List<Put> resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
	  		 List<Put> al=new ArrayList<Put>();
	  		 
	  		byte[] htmlByts=result.getValue("base".getBytes(), "page".getBytes());
	  		if(htmlByts==null || htmlByts.length<20) return al;
	  		
	  		String htmlStr=new String(htmlByts);
	  		
	  		 Map<String,Object> mp=de.extract(htmlStr,new String(key.get()));
	  		 
	  		 if(mp==null||mp.isEmpty()){
	  			 log.info(" attribute is null,the rowkey is ["+new String(key.get())+"] document lenth is "+htmlStr.length());
	  			 return al;
	  		 }
	  		 
	  		if(!isExtractList){
	  			String rowkey=(String)mp.remove("rowkey");//优先使用模版中设置的rowkey
	  			if(rowkey==null){
	  				 rowkey=new String(key.get());
	  		  		 if(rowkey.contains(".en.")){
	  		  			 rowkey=rowkey.replace(".en.", ".");
	  		  		 }
	  		  		 if(rowkey.contains(".cn.")){
	  		  			 rowkey=rowkey.replace(".cn.", ".");
	  		  		 }
	  			}
		  		Put put = new Put(rowkey.getBytes());
		 		for (Map.Entry<String,Object> entry:mp.entrySet()) {
					put.add(family.getBytes(),entry.getKey().getBytes(), (entry.getValue()!=null?entry.getValue().toString().getBytes():null));
				}
		 		al.add(put);
	  		}else{
	  			for (Map.Entry<String,Object> entry:mp.entrySet()) {
	  				if(entry.getValue() instanceof Collection){
	  					Collection<Map<String,Object>> maps=null;
	  					try{
	  						maps=(Collection<Map<String,Object>>)entry.getValue();
	  					}catch(ClassCastException e){
	  						log.error(e);
	  					}
	  					
	  					for(Map<String,Object> map:maps){
	  						String rk=(String)map.remove("rowkey");
	  						Put put = new Put(rk.getBytes());
	  						for(Map.Entry<String, Object> attribute:map.entrySet()){
	  							if(!(attribute.getValue() instanceof String)){
	  								log.error("ERROR:the value isn't String:"+attribute.getKey()+"="+attribute.getValue()+" it's type is "+attribute.getKey().getClass());
	  								continue;
	  							}
	  							byte[] v=attribute.getValue()!=null?((String)attribute.getValue()).getBytes():null;
	  							put.add(family.getBytes(),attribute.getKey().getBytes(),v);
	  						}
	  						al.add(put);
	  					}
	  				}
	  			}
	  		}
	  		return al;
	   	}
	}

	public static void main(String[] args) throws Exception {
		
		/*参数检查*/
		if( !(args.length==2 || args.length==4 || args.length==5)){
			log.error(" extra data params number is error ,only  or 4 or 5.the format is  [format file path,task ID,start rowkey,end rowkey,timestamp]");
			log.error(" the params length  is:"+args.length+" data is"+SpiderUtil.array2Str(args));
			System.err.println(" extra data params number is error ,only 2 or 4 or 5.the format is [format file path,task ID,start rowkey,end rowkey,timestamp]");
			System.err.println(" the real params length  is:"+args.length+",data is"+SpiderUtil.array2Str(args));
			return;
		}
		
		/*运行hadoop任务*/
		try{
			SpiderContext.initSpiderContentByDefaultFile();
			
			String formatFilePath=args[0];
			log.info(" the file path is :"+formatFilePath);
			DataExtract de=new DataExtract(formatFilePath,false);
			if(!de.isInitOK()){
				log.error("the format file is not found");
				return ;
			}
			
			Map<String,Object> formatConfig=de.getConfig();
			
			String sourceTableName=formatConfig.get("sourceTable").toString();
			String targetTableName=formatConfig.get("targetTable").toString();
			String startRowkey=formatConfig.get("startRowkey")!=null?formatConfig.get("startRowkey").toString():null;
			String endRowkey=formatConfig.get("endRowkey")!=null?formatConfig.get("endRowkey").toString():null;
			String scanCach=formatConfig.get("scanCach")!=null?formatConfig.get("scanCach").toString():"10";
			String jobName="Extract";
			
			/*以下配置用于任务中定义的范围来替换模版文件中的范围*/
			if(args.length>1&&!"null".equals(args[1])) jobName=jobName+"["+args[1].trim()+"]";
			if(args.length>2&&!"null".equals(args[2])) startRowkey=args[2].trim();
			if(args.length>3&&!"null".equals(args[3])) endRowkey=args[3].trim();
			String timestamp=args.length==5&&args[4].matches("\\d+")?args[4]:null;
			
			long minTimestamp=-1;
			if(timestamp!=null){
				minTimestamp=Long.parseLong(timestamp);
			}
			//GenericOptionsParser.
			
			log.info("====sourceTableName="+sourceTableName+", targetTableName="+targetTableName);
			log.info("====startRowkey="+startRowkey+", endRowkey="+endRowkey);
			
			Configuration config = HBaseConfiguration.create();
			config.set("extract.format.file.path",formatFilePath );
			Job job = new Job(config,jobName);
			job.setJarByClass(DataExtractMR.class);    
			
			Scan scan = new Scan();
			log.info("====startTimestamp="+minTimestamp);
			if(minTimestamp!=-1){
				//实现增量抽取
				scan.setTimeRange(minTimestamp,System.currentTimeMillis());
			}
			
			scan.addColumn("base".getBytes(),"page".getBytes());
			
			
			scan.setCaching(Integer.parseInt(scanCach));        
			scan.setCacheBlocks(false);  
			if(startRowkey!=null) scan.setStartRow(startRowkey.getBytes());
			if(endRowkey!=null)scan.setStopRow(endRowkey.getBytes());
			
			job.setMapSpeculativeExecution(false);//禁止推测
			
			TableMapReduceUtil.initTableMapperJob(
				sourceTableName,      // input table
				scan,	          // Scan instance to control CF and attribute selection
				Extract.class,   // mapper class
				null,	          // mapper output key
				null,	          // mapper output value
				job);
			TableMapReduceUtil.initTableReducerJob(
				targetTableName,      // output table
				null,             // reducer class
				job);
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
