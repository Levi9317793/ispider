package net.hubs1.bijia.dao;


import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.hubs1.bijia.entity.MedalEntity;
import net.hubs1.bijia.entity.PlanEntity;
import net.hubs1.bijia.entity.QunarHotelEntity;
import net.hubs1.bijia.entity.RateEntity;
import net.hubs1.bijia.main.LoadProperty;
import net.hubs1.bijia.util.QunaParser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import Test.CtripLoader;



public class Hbase2MysqlQunarHotel implements Hbase2MysqlInterface {
	private Connection conn ;
	private int count ;
	private long time = System.currentTimeMillis();
	private QunarHotelEntity hotelInsertEntity = QunarHotelEntity.getInsertInstance();
	private QunarHotelEntity hotelSelectEntity = QunarHotelEntity.getSelectInstance();
	private Map<String,String> cityMap;
	private PreparedStatement hotelInsertState ;
	private PreparedStatement hotelSelectState ;
	private final Log logger = LogFactory.getLog(CtripLoader.class);
	
	private Hbase2MysqlQunarHotel(){}
	public static Hbase2MysqlQunarHotel getInstance(){
		Hbase2MysqlQunarHotel hbase2MysqlPrice = new Hbase2MysqlQunarHotel();
		return hbase2MysqlPrice;
	}
	public void init(){
		LoadProperty property = new LoadProperty();	
		String driver = property.driver;
		String url = property.jdbc;
		String user = property.user;
		String passwd = property.passwd;
		try {

			Class.forName(driver);
			if(conn == null)
				conn = DriverManager.getConnection(url, user, passwd);
				conn.setAutoCommit(false);
				hotelInsertState = conn.prepareStatement(hotelInsertEntity.sql);
				hotelSelectState = conn.prepareStatement(hotelSelectEntity.sql);
			}
		catch(Exception e){
			logger.error("Hbase2MysqlPrice init error",e);
		}
	}
	/**
	 * close connection
	 */
	public void close(){
		try {
			conn.commit();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Hbase2MysqlPrice close error",e);
		}
	}


	/**
	 * ctripRate table update or insert
	 * @param map
	 * @param breakfast
	 * @throws Exception 
	 */
	public void mysqlHotelInsert(Map<String,String> map) throws Exception{

		hotelSelectState.setString(1, map.get("pdcId"));
		ResultSet rs = hotelSelectState.executeQuery(); 
		if(!rs.next()) { 
			hotelInsertState.setString(hotelInsertEntity.pdcId, map.get("pdcId"));  
			hotelInsertState.setString(hotelInsertEntity.province, map.get("province")); 
			hotelInsertState.setString(hotelInsertEntity.city, map.get("city"));
			hotelInsertState.setString(hotelInsertEntity.name, map.get("name")); 
			hotelInsertState.setString(hotelInsertEntity.star, map.get("star")); 
			hotelInsertState.executeUpdate();
		}
		

	}
	/**
	 * ctripPlan and price import
	 * @param map
	 * @throws InterruptedException
	 */
	public void mysqlInsert(Map<String,String> map){
		try {
			mysqlHotelInsert(map);
			
			count++;
			if(count%200==0){
				logger.info("ctripPlan count:"+count);
				long temp = System.currentTimeMillis();
				logger.info("ctripPlan time per 200:"+(temp-time)/1000);
				time =temp;
				conn.commit();
			}
			
		}catch(Exception e){
			logger.error("Hbase2MysqlPrice mysqlInsert error",e);
			try {Thread.sleep(5000);}catch(Exception threadE){threadE.printStackTrace();}
		} 
	}
	/**
	 * hotel select from hbase
	 */
	public void hbaseSelect(String site){
		try {
			cityMap = QunaParser.fileRead("province.txt");
			HTable readTable = new HTable("spider_prop");
			Scan s = new Scan();
			s.setStartRow("qunar:a".getBytes());
			s.setStopRow("qunar:zzzzzzzzzzzz".getBytes());
			s.setCaching(300);
			ResultScanner ss = readTable.getScanner(s);
			for(Result r:ss){
				String hotelUrl = new String(r.getRow());
				Map<String,String> map = new HashMap<String,String>();
				for(KeyValue kv :r.raw()){
					realHbaseSelect(map,kv);
				}
				mysqlInsert(map);
			}
		}catch(Exception e){
			logger.error("Hbase2MysqlPrice hbaseSelect error",e);
		}
	}
	
	public void realHbaseSelect(Map<String,String> map, KeyValue kv){
		String column = new String(kv.getQualifier());
		String value = new String(kv.getValue());
		if("city".equals(column)){
			map.put("city",value);
			Iterator<String> it = cityMap.keySet().iterator();
			while (it.hasNext()) {
				String key = it.next();
				String citys = cityMap.get(key);
				if(citys!=null && citys.contains(value)){
					map.put("province",key);
					break;
				}
			}
		}
		if("name".equals(column)){
			map.put("name",value);	
			
		}
		if("source_url".equals(column)){
			map.put("pdcId","qunar:"+value);	
		}
		if("star_rating".equals(column)){
			map.put("star",value);	
		}

	}
	public static void main(String[] args) throws Exception {
		Hbase2MysqlQunarHotel client = new Hbase2MysqlQunarHotel();
		client.init();
		client.hbaseSelect("");
		client.close();
	}
}
