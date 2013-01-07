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
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.json.JSONArray;


public class ImportPriceMR implements JobMR{

	private static final Log logger = LogFactory.getLog(ImportPriceMR.class);

	static class MyMapper extends TableMapper<ImmutableBytesWritable,Put> {
		public static Hbase2MysqlPrice hbase2MysqlPrice = Hbase2MysqlPrice.getInstance();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			hbase2MysqlPrice.init();
		}
		@Override
		protected void cleanup(Context context){
			hbase2MysqlPrice.close();
		}
		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws InterruptedException, IOException {
			Map<String,String> map = new HashMap<String,String>();
			for(KeyValue kv :values.list()){
				hbase2MysqlPrice.subHbaseSelect(map,kv);
			
			}
			hbase2MysqlPrice.mysqlInsert(map);		
		}
	}

	public boolean mapStart(){
		boolean result = false;
		
		String sourceTable = "spider_price";
		try{
			Hbase2MysqlPrice hbase2MysqlStatus = Hbase2MysqlPrice.getInstance();
    		hbase2MysqlStatus.init();
    		hbase2MysqlStatus.statusStart();
    		
			Configuration config = HBaseConfiguration.create();
			Job job = new Job(config, "CtripPriceImport");
			job.setJarByClass(ImportPriceMR.class);
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
			job.setMapSpeculativeExecution(false);
			job.setOutputFormatClass(NullOutputFormat.class);
			job.setNumReduceTasks(0);
			result = job.waitForCompletion(true);

    		hbase2MysqlStatus.statusEnd();
			hbase2MysqlStatus.close();
		}catch(Exception e){
			logger.error("import price job error",e);
		}
		return result;
	}

}
