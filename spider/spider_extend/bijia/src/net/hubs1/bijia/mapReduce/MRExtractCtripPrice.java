package net.hubs1.bijia.mapReduce;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import net.hubs1.bijia.extract.ExtractCtripPrice;

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
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;


public class MRExtractCtripPrice implements MRinterface{

	
	private static final Log logger = LogFactory.getLog(MRExtractCtripPrice.class);

	static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws InterruptedException, IOException {

			String url = new String(row.get());
			for (KeyValue kv : values.list()) {
				if ("base".equals(Bytes.toString(kv.getFamily()))
						&& "page".equals(Bytes.toString(kv.getQualifier()))) {
					List<Map<String, String>> list = null;			
					try {
						list = ExtractCtripPrice.extractPrice(new String(kv.getValue()), url);
					} catch (Exception e) {
						e.printStackTrace();
					}
					for (Map<String, String> map : list) {
						String rowKey = map.get("source_url") + "."
								+ map.get("planCode");
						Put put = new Put(rowKey.getBytes());
						put.setWriteToWAL(false);
						Iterator<String> it = map.keySet().iterator();
						while (it.hasNext()) {
							String key = (String) it.next();
							put.add("base".getBytes(), key.getBytes(),
									map.get(key).getBytes());
						}
						context.write(null, put);
					}
				}
			}
		}
	}

	// static class MyReducer extends TableReducer<Text, Text, Put> {
	// public void reduce(ImmutableBytesWritable key, Iterator<Text> values,
	// Context context) throws IOException, InterruptedException {
	// Text value = null;
	// if(values.hasNext()){
	// value = values.next();
	// }
	// Put put = new Put(key.get());
	// put.add(Bytes.toBytes("base"), Bytes.toBytes("price"),
	// Bytes.toBytes(value.toString()));
	//
	// context.write(null, put);
	// }
	// }
	public boolean mapStart(String sourceSite){
		String sourceTable = "download_page";
		String targetTable = "spider_price";
		boolean result = false;
		try{
			Configuration config = HBaseConfiguration.create();
			Job job = new Job(config, "CtripPriceExtract");
			job.setJarByClass(MRExtractCtripPrice.class); // class that contains mapper
	
			Scan scan = new Scan();
			scan.setCaching(50); // 1 is the default in Scan, which will be bad for
									// MapReduce jobs
			scan.setCacheBlocks(false); // don't set to true for MR jobs
			// set other scan attrs
			scan.setStartRow("ctrip.hotel.a".getBytes());
			scan.setStopRow("ctrip.hotel.zzzzzzzzzzzz".getBytes());
			TableMapReduceUtil.initTableMapperJob(sourceTable, // input table
					scan, // Scan instance to control CF and attribute selection
					MyMapper.class, // mapper class
					ImmutableBytesWritable.class, // mapper output key
					Text.class, // mapper output value
					job);
			TableMapReduceUtil.initTableReducerJob(targetTable, // output table
					null, // reducer class
					job);
			job.setMapSpeculativeExecution(false);
			job.setNumReduceTasks(0);
			result = job.waitForCompletion(true);
		}catch(Exception e){
			logger.error("extract price job error",e);
		}
		return result;
	}
}
