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
import java.util.Map;



import net.hubs1.bijia.entity.MedalEntity;
import net.hubs1.bijia.entity.PlanEntity;
import net.hubs1.bijia.entity.RateEntity;
import net.hubs1.bijia.main.LoadProperty;

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



public class Hbase2MysqlQunarPrice implements Hbase2MysqlInterface {
	private Connection conn ;
	private int count ;
	private long time = System.currentTimeMillis();
	private PlanEntity planInsertEntity = PlanEntity.getQunarInsertInstance();
	private PlanEntity planUpdateEntity = PlanEntity.getQunarUpdateInstance();
	private PlanEntity planSelectEntity = PlanEntity.getSelectInstance();
	
	private RateEntity rateInsertEntity = RateEntity.getQunarInsertInstance();
	private RateEntity rateUpdateEntity = RateEntity.getQunarUpdateInstance();
	
	private PreparedStatement prestRateInsert ;
	private PreparedStatement prestPlanInsert ;
	private PreparedStatement prestRateUpdate ;
	private PreparedStatement prestPlanUpdate ;
	private PreparedStatement prestPlanSelect ;
	private String dateString;
	private String idPrefix = "ctrip:http://hotels.ctrip.com/hotel/";
	private String idPostfix = ".html";
	private String sourceSite = "qunar_bijia";
	
	private final Log logger = LogFactory.getLog(CtripLoader.class);
	

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
				prestRateInsert = conn.prepareStatement(rateInsertEntity.sql);
				prestRateUpdate = conn.prepareStatement(rateUpdateEntity.sql);
				
				prestPlanInsert = conn.prepareStatement(planInsertEntity.sql);
				prestPlanUpdate = conn.prepareStatement(planUpdateEntity.sql);
				prestPlanSelect = conn.prepareStatement(planSelectEntity.sql);
			}
		catch(Exception e){
			logger.error("Hbase2MysqlPrice init error",e);
		}
		System.out.println(url);
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
	 * Qunar Plan table update or insert
	 */
	public void mysqlPlanUpdate(Map<String,String> map)throws Exception{
	
			prestPlanSelect.setString(1, map.get("pId"));
			ResultSet rs = prestPlanSelect.executeQuery(); 
			if(rs.next()) {  
				
					prestPlanUpdate.setString(planUpdateEntity.payType, map.get("payType")); 
					prestPlanUpdate.setString(planUpdateEntity.status, map.get("status")); 
					prestPlanUpdate.setString(planUpdateEntity.planId, map.get("pId")); 
				
				prestPlanUpdate.execute();
			}
			else{
				prestPlanInsert.setString(planInsertEntity.planId, map.get("pId"));  
				prestPlanInsert.setString(planInsertEntity.ctripID, map.get("pdcID")); 
				prestPlanInsert.setString(planInsertEntity.rid, map.get("roomCode"));
				prestPlanInsert.setString(planInsertEntity.planName, map.get("saleItem")); 
				prestPlanInsert.setString(planInsertEntity.payType, map.get("payType")); 
				prestPlanInsert.setString(planInsertEntity.status, map.get("status")); 
				prestPlanInsert.setString(planInsertEntity.site, sourceSite); 
				prestPlanInsert.setString(planInsertEntity.provider, map.get("provider")); 
				prestPlanInsert.execute();
			}
			rs.close();
	}
	/**
	 * Qunar Rate table update or insert
	 * @param map
	 * @param breakfast
	 * @throws Exception 
	 */
	public void mysqlRateUpdate(Map<String,String> map) throws Exception{
		String breakfast=null;
		if(map.get("price")==null)
			System.out.println(map);
		String price = map.get("price");

			BigDecimal rate = new BigDecimal(0);
			if(price.replaceAll("\\d", "").equals("")){
				rate = new BigDecimal(price);
			}
			prestRateUpdate.setBigDecimal(rateUpdateEntity.svcRate, rate);
			prestRateUpdate.setString(rateUpdateEntity.status, map.get("status"));
			prestRateUpdate.setString(rateUpdateEntity.pId, map.get("pId"));
			prestRateUpdate.setString(rateUpdateEntity.date, map.get("date"));
			if(prestRateUpdate.executeUpdate()==0)
			{			
				prestRateInsert.setString(rateInsertEntity.pId, map.get("pId"));  
				prestRateInsert.setString(rateInsertEntity.date, map.get("date")); 
				prestRateInsert.setBigDecimal(rateInsertEntity.svcRate, rate);
				prestRateInsert.setString(rateInsertEntity.status, map.get("status")); 
				prestRateInsert.setString(rateInsertEntity.site, sourceSite); 
				prestRateInsert.setString(rateInsertEntity.provider, map.get("provider")); 
				prestRateInsert.executeUpdate();
			}
		

	}
	/**
	 * Qunar Plan and price import
	 * @param map
	 * @throws InterruptedException
	 */
	public void mysqlInsert(Map<String,String> map) {
		try {
			mysqlRateUpdate(map);
			mysqlPlanUpdate(map);
			
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
	 * price select from hbase
	 */
	public void hbaseSelect(String site){
		sourceSite = site;
		try {
			HTable readTable = new HTable("spider_price");
			Scan s = new Scan();
			if(sourceSite.equals("qunar_all")){	
				s.setStartRow("qunar.price.a".getBytes());
				s.setStopRow("qunar.price.zzzzzzzzzzzz".getBytes());
			}
			else {
				s.setStartRow("qunar.bijia.a".getBytes());
				s.setStopRow("qunar.bijia.zzzzzzzzzzzz".getBytes());
				System.out.println("bijia");
			}
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
		if("roomPrice".equals(column)){
			map.put("price",value);				
		}
		if("reservation".equals(column)){
			map.put("status",value);	
			
		}
		if("payType".equals(column)){
			map.put("payType",value);	
			
		}
		if("planCode".equals(column)){
			map.put("pId",value);	
		}
		if("source_url".equals(column)){
			map.put("source_url",value);	
		}
		if("roomCode".equals(column)){
			map.put("roomCode",value);	
		}
		if("pdcID".equals(column)){
			map.put("pdcID",value);	
		}

		if("saleItem".equals(column)){
			map.put("saleItem",value);	
		}
		if("date".equals(column)){
			map.put("date",value);	
		}
		if("provider".equals(column)){
			map.put("provider",value);	
		}
	}
	public static void main(String[] args) throws Exception {
		Hbase2MysqlQunarPrice client = new Hbase2MysqlQunarPrice();
		client.init();
		//client.hbaseSelect("qunar_all");
		client.hbaseSelect("qunar_bijia");
		client.close();
	}
}
