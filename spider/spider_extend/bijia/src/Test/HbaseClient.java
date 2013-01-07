package Test;


import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


import net.hubs1.bijia.extract.ExtractCtripPrice;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseClient {
	private static HTablePool pool;
	private static HBaseAdmin admin;
	private static String familyName = "base";
	private static boolean autoFlush = false;
	private static int count = 1000;
	private static final Log logger = LogFactory.getLog(HbaseClient.class);
	static {
		Configuration hbaseConfiguration = HBaseConfiguration.create();
		try {
			admin = new HBaseAdmin(hbaseConfiguration);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		pool = new HTablePool(hbaseConfiguration, 100);
	}
	public static void delete(String tableName) {
		HTable table = (HTable) pool.getTable(tableName);
		table.setAutoFlush(false);
		Scan s = new Scan();
		s.setCaching(100);
		try {
			ResultScanner ss = table.getScanner(s);
			int i=0;
			for(Result r:ss){
				//if("quna.hotel.price.detail".equals(new String(r.getRow()))){
					Delete d = new Delete(r.getRow());
					table.delete(d);
					i++;
				//}
			}
			System.out.println(i);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String get() {
		String result =null;
		HTable table = (HTable) pool.getTable("download_page");
		String row = "ctrip.hotel.hotel.108470";
		Get get = new Get(row.getBytes());
		Result r = null;
		try {
			r = table.get(get);
			result = new String(r.getValue("base".getBytes(), "page".getBytes()));
			System.out.println(result);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result; 
	}
	public static void insertRoom(List<Map<String, String>> list,HTable writeTable) throws IOException{
		//rowKey = url2RowKey(rowKey,"quna.hotel",false);
		for(Map<String, String> map:list){
			String rowKey = "ctrip:"+map.get("source_url");
			Put put = new Put(rowKey.getBytes());
			put.setWriteToWAL(false);
	        Iterator<String> it = map.keySet().iterator();
	        while(it.hasNext()) {
	            String key = (String) it.next();
				put.add("roominfo".getBytes(), key.getBytes(), map.get(key).getBytes());
				try {
					writeTable.put(put);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
		}
	}
	public static void createTable(String tableName) {
		HTableDescriptor td = new HTableDescriptor(tableName);
		HColumnDescriptor hc = new HColumnDescriptor(familyName);
		hc.setMaxVersions(5);
		td.addFamily(hc);
		
		try {
			admin.createTable(td);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void insert(String tableName ,String rowKey,String value) {
		//rowKey = url2RowKey(rowKey,"quna.hotel",false);
		HTable table = (HTable) pool.getTable(tableName);
		//HTable table=null;
//		try {
//			table = new HTable(tableName);
//		} catch (IOException e2) {
//			e2.printStackTrace();
//		}
 		table.setAutoFlush(false);
 		System.out.println(table.isAutoFlush());
		try {
			table.setWriteBufferSize(100000);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		Put put = new Put(rowKey.getBytes());
		put.setWriteToWAL(false);
		put.add(familyName.getBytes(), "url".getBytes(), value.getBytes());
		try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static String url2RowKey(String url,String rowkeyPrefix,boolean deleteUrlParam){
		int i=-1;
		if((i=url.indexOf("//"))!=-1){
			url=url.substring(i+2);
		}
		
		if(deleteUrlParam){
			int index=url.indexOf("?");
			if(index>0) url=url.substring(0,index);
		}
		
		String rowkey=rowkeyPrefix;
		if(!url.startsWith("/")){
			url=url.substring(url.indexOf("/"));
		}
		
		int haveExt=url.lastIndexOf(".");
		if(haveExt>0) 
			rowkey=rowkey+url.substring(0,haveExt);
		else
			rowkey=rowkey+url;
		
		rowkey=rowkey.replaceAll("/", ".");
		return rowkey;
	}
	public static void extractTest(){
		HTable table = (HTable) pool.getTable("download_page");
		Scan s = new Scan();
		s.setCaching(10000);
		s.setStartRow("quna.hotel.a".getBytes());
		s.setStopRow("quna.hotel.zzzzzzzzzzzz".getBytes());
		try {
			ResultScanner ss = table.getScanner(s);
			int i=0;
			for(Result r:ss){
					String hotelUrl = new String(r.getRow());
					String result = new String(r.getValue("base".getBytes(), "page".getBytes()));
					//QunaTest.extraPrice(hotelUrl,result);
					System.out.println(hotelUrl);
					//System.out.println(result);
			}
			System.out.println(i);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void select(){
		HTable table = (HTable) pool.getTable("download_page");
		Scan s = new Scan();
		s.setCaching(30);
		s.setStartRow("ctrip.hotel.hotel.20065".getBytes());
		s.setStopRow("ctrip.hotel.hotel.20065000".getBytes());
		//s.setStartRow("ctrip.hotel.hotel.55731".getBytes());
		//s.setStopRow("ctrip.hotel.hotel.55731000".getBytes());
		s.setMaxVersions(5);
		try {
			ResultScanner ss = table.getScanner(s);
			int i=0;
			
			for(Result r:ss){
				System.out.println(new String(r.getRow()));
					for(KeyValue kv :r.raw()){
						logger.info(new String
								(kv.getQualifier())+"=="+new String(kv.getValue()));
						SimpleDateFormat formatter = 
								new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						logger.info(formatter.format(kv.getTimestamp()));
					}
			}
			System.out.println(i);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void guangzhouTest() throws Exception{
		HTable table = (HTable) pool.getTable("download_url");
		Scan s = new Scan();
		s.setCaching(30);
		s.setStartRow("spider.ctrip.12745.a".getBytes());
		s.setStopRow("spider.ctrip.12745.zzzz".getBytes());
		
		String jdbc = "jdbc:mysql://192.168.20.119:3306/bijia";
		String driver = "com.mysql.jdbc.Driver";		
		String user = "root";
		String passwd = "thayer";
		String sql1 = " select planId from ctripPlan where ctripId =?";
		String sql2 = " select date from ctripRate where pId =? where date > '2012-10-14'";
		Class.forName(driver);
		Connection conn = DriverManager.getConnection(jdbc, user, passwd);
		conn.setAutoCommit(false);
		
		PreparedStatement prestPlanSelect = conn.prepareStatement(sql1);
		PreparedStatement prestRateSelect = conn.prepareStatement(sql2);
		//s.setStartRow("ctrip.hotel.hotel.55731".getBytes());
		//s.setStopRow("ctrip.hotel.hotel.55731000".getBytes());
		try {
			ResultScanner ss = table.getScanner(s);
			int i=0;
			
			for(Result r:ss){
				System.out.println(new String(r.getRow()));
				i++;
					for(KeyValue kv :r.raw()){
						if("url".equals(new String
								(kv.getQualifier()))){
							String url = new String(kv.getValue());
							prestPlanSelect.setString(0, url);
							ResultSet rs = prestPlanSelect.executeQuery(); 
							if(rs.next()) {
								prestRateSelect.setString(0, rs.getString("planId"));
								ResultSet rsRate = prestRateSelect.executeQuery();
								while(rsRate.next()){
									System.out.println(rsRate.getString("date"));
								}
							}
							
						}
					}
			}
			System.out.println(i);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void guangzhouSelect(){
		HTable table = (HTable) pool.getTable("download_page");
		Scan s = new Scan();
		s.setCaching(30);
		s.setStartRow("ctrip.hotel.hotel.67464".getBytes());
		s.setStopRow("ctrip.hotel.hotel.67464000".getBytes());
		//s.setStartRow("ctrip.hotel.hotel.55731".getBytes());
		//s.setStopRow("ctrip.hotel.hotel.55731000".getBytes());
		try {
			ResultScanner ss = table.getScanner(s);
			int i=0;
			
			for(Result r:ss){
				String url = new String(r.getRow());
				System.out.println(url);
				for(KeyValue kv :r.raw()){
						if ("base".equals(Bytes.toString(kv.getFamily()))
								&& "page".equals(Bytes.toString(kv.getQualifier()))) {
							List<Map<String, String>> list = null;			
							try {
								list = ExtractCtripPrice.extractPrice(new String(kv.getValue()), url);
								System.out.println(list);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
				}					
			}
			System.out.println(i);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws Exception{
		//createTable();
		//select();
		//delete("spider_price");
		//	insert("download_url" ,"quna.hotel.city.beijing_city.dt-18842","http://hotel.qunar.com/city/beijing_city/dt-18842");
		//extractTest();
		//get();
		//guangzhouTest();
		guangzhouSelect();
	}
}
