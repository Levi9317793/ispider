package net.hubs1.bijia.dao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;



import net.hubs1.bijia.entity.MedalEntity;
import net.hubs1.bijia.main.LoadProperty;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.json.JSONArray;
import org.json.JSONObject;

import Test.CtripLoader;


public class Hbase2MysqlMedal {

	private static Connection conn ;
	private static MedalEntity insertMedalEntity = MedalEntity.getInsertInstance();
	private static MedalEntity updateMedalEntity = MedalEntity.getUpdateInstance();
	private static PreparedStatement prestMedalInsert;
	private static PreparedStatement prestMedalUpdate;
	private static int count ;
	private static long time = System.currentTimeMillis();
	private static final Log logger = LogFactory.getLog(CtripLoader.class);
	private static String idPrefix = "ctrip:http://hotels.ctrip.com/hotel/";
	private static String idPostfix = ".html";
	public  static void init(){
		LoadProperty property = new LoadProperty();	
		String driver = property.driver;
		String url = property.jdbc;
		String user = property.user;
		String passwd = property.passwd;
		try {

			Class.forName(driver);
			if(conn == null)
				conn = DriverManager.getConnection(url, user,passwd);
				conn.setAutoCommit(false);
				prestMedalInsert = conn.prepareStatement(insertMedalEntity.sql);
				prestMedalUpdate = conn.prepareStatement(updateMedalEntity.sql);
			}
		catch(Exception e){
			logger.error("Hbase2MysqlMedal init error",e);
		}
	}
	public static void close(){
		try {
			conn.commit();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Hbase2MysqlMedal close error",e);
		}
	}
	
	public static void mysqlMedalUpdate(Map<String,String> map) throws Exception{
		
		prestMedalUpdate.setShort(updateMedalEntity.goldLevel, Short.valueOf(map.get("medal")));
		prestMedalUpdate.setString(updateMedalEntity.ctripId, idPrefix+map.get("propId")+idPostfix); 
		if(prestMedalUpdate.executeUpdate()==0){
			
			prestMedalInsert.setString(insertMedalEntity.ctripId, idPrefix+map.get("propId")+idPostfix);
			prestMedalInsert.setShort(insertMedalEntity.goldLevel, Short.valueOf(map.get("medal")));
			prestMedalInsert.executeUpdate();
		}
		

	}
	
	public static void mysqlInsert(Map<String,String> map) throws InterruptedException{
		try {
			mysqlMedalUpdate(map);
			
			count++;
			if(count%2000==0){
				logger.info("ctripPlan count:"+count);
				long temp = System.currentTimeMillis();
				logger.info("ctripPlan time per 2000:"+(temp-time)/1000);
				time =temp;
				conn.commit();
			}
			
		}catch(Exception e){
			logger.error("Hbase2MysqlMedal mysqlInsert error",e);
			Thread.sleep(5000);
		} 
	}
	
	public static void hbaseSelect(){
		try {
			HTable readTable = new HTable("spider_price");
			Scan s = new Scan();
			s.setStartRow("ctrip.hotel.a".getBytes());
			s.setStopRow("ctrip.hotel.zzzzzzzzzzzz".getBytes());
			s.setCaching(300);
			ResultScanner ss = readTable.getScanner(s);
			int count =0;
			for(Result r:ss){
				String hotelUrl = new String(r.getRow());
				Map<String,String> map = new HashMap<String,String>();
				for(KeyValue kv :r.raw()){
					subHbaseSelect(map,kv);
				}
				if(count++>2000)break; 
				mysqlInsert(map);
			}
			readTable.close();
		}catch(Exception e){
			logger.error("Hbase2MysqlMedal hbaseSelect error",e);
		}
	}
	
	public static void subHbaseSelect(Map<String,String> map, KeyValue kv){
		String column = new String(kv.getQualifier());
		String value = new String(kv.getValue());
		if("medal".equals(column)){
			map.put("medal",value);				
		}
		if("propId".equals(column)){
			map.put("propId",value);	
		}
	}
	public static void start(){
		init();
		hbaseSelect();
		close();
	}
	
	public static void main(String[] args){
		init();
		hbaseSelect();
		close();
	}
}
