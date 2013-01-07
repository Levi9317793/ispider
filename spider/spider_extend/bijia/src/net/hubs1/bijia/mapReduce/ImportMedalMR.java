package net.hubs1.bijia.mapReduce;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.soap.Text;


import net.hubs1.bijia.dao.Hbase2MysqlMedal;
import net.hubs1.bijia.dao.Hbase2MysqlPrice;
import net.hubs1.bijia.extract.CtripPriceExtract;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.json.JSONArray;


public class ImportMedalMR implements JobMR{

	private static String sourceTable = "spider_price";
	private static final Log logger = LogFactory.getLog(ImportMedalMR.class);


	static class MyMapper extends TableMapper<ImmutableBytesWritable,Put> {
		
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Hbase2MysqlMedal.init();
		}
		@Override
		protected void cleanup(Context context){
			Hbase2MysqlMedal.close();
		}
		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws InterruptedException, IOException {
			Map<String,String> map = new HashMap<String,String>();
			for(KeyValue kv :values.list()){
				Hbase2MysqlMedal.subHbaseSelect(map,kv);
			
			}
			Hbase2MysqlMedal.mysqlInsert(map);		
		}
	}

	public boolean mapStart() {
		boolean result = false;
		try{
			Configuration config = HBaseConfiguration.create();
			Job job = new Job(config, "CtripMedalImport");
			job.setJarByClass(ImportMedalMR.class);
	
			Scan scan = new Scan();
			scan.setCaching(500); 							// MapReduce jobs
			scan.setCacheBlocks(false); // don't set to true for MR jobs
			// set other scan attrs
			scan.setStartRow("ctrip.hotel.a".getBytes());
			scan.setStopRow("ctrip.hotel.zzzzzzzzzzzz".getBytes());
			TableMapReduceUtil.initTableMapperJob(sourceTable, // input table
					scan, // Scan instance to control CF and attribute selection
					MyMapper.class, // mapper class
					ImmutableBytesWritable.class, // mapper output key
					Put.class, // mapper output value
					job);
			TableMapReduceUtil.initTableReducerJob("ex_review", // output table
					null, // reducer class
					job);
			job.setMapSpeculativeExecution(false);
			job.setNumReduceTasks(0);
			result = job.waitForCompletion(true);
		}catch(Exception e){
			logger.error("import medal job error",e);
		}
		return result;
	}

}
