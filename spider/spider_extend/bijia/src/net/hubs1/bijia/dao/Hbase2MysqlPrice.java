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



public class Hbase2MysqlPrice {
	private Connection conn ;
	private int count ;
	private long time = System.currentTimeMillis();
	private PlanEntity planInsertEntity = PlanEntity.getInsertInstance();
	private PlanEntity planUpdateEntity = PlanEntity.getUpdateInstance();
	private PlanEntity planSelectEntity = PlanEntity.getSelectInstance();
	
	private RateEntity rateInsertEntity = RateEntity.getInsertInstance();
	private RateEntity rateUpdateEntity = RateEntity.getUpdateInstance();
	
	private PreparedStatement prestRateInsert ;
	private PreparedStatement prestPlanInsert ;
	private PreparedStatement prestRateUpdate ;
	private PreparedStatement prestPlanUpdate ;
	private PreparedStatement prestPlanSelect ;
	private String dateString;
	private String idPrefix = "ctrip:http://hotels.ctrip.com/hotel/";
	private String idPostfix = ".html";
	private final Log logger = LogFactory.getLog(CtripLoader.class);
	
	private Hbase2MysqlPrice(){}
	public static Hbase2MysqlPrice getInstance(){
		Hbase2MysqlPrice hbase2MysqlPrice = new Hbase2MysqlPrice();
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
				prestRateInsert = conn.prepareStatement(rateInsertEntity.sql);
				prestRateUpdate = conn.prepareStatement(rateUpdateEntity.sql);
				
				prestPlanInsert = conn.prepareStatement(planInsertEntity.sql);
				prestPlanUpdate = conn.prepareStatement(planUpdateEntity.sql);
				prestPlanSelect = conn.prepareStatement(planSelectEntity.sql);
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
	 * start import,insert status table
	 */
	public void statusStart(){
		try {
			Date date = new Date();
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			dateString = formatter.format(date);
			String sqlStatus = "insert into status (date,hubs1Status,ctripStatus) " +
					"values(?,?,?)";
			System.out.println(dateString);		
			PreparedStatement prestStatusStart = conn.prepareStatement(sqlStatus);
			prestStatusStart.setString(1, dateString);
			prestStatusStart.setInt(2, 0);
			prestStatusStart.setInt(3, 0);
			prestStatusStart.execute();
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Hbase2MysqlPrice statusStart error",e);
		}
	}
	/**
	 * end import, update status table
	 */
	public void statusEnd(){
		try {
			String sqlStatus = "update status set ctripStatus=1 where date=?";
					
			PreparedStatement prestStatusEnd = conn.prepareStatement(sqlStatus);
			prestStatusEnd.setString(1, dateString);
			prestStatusEnd.execute();
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Hbase2MysqlPrice statusEnd error",e);
		}
	}
	/**
	 * ctripPlan table update or insert
	 */
	public void mysqlPlanUpdate(Map<String,String> map)throws Exception{
	
			prestPlanSelect.setString(1, map.get("pId"));
			ResultSet rs = prestPlanSelect.executeQuery(); 
			if(rs.next()) {  
				if((rs.getString("ctripNumId")!=null) && map.get("ctripNumId")==null){
					prestPlanUpdate.setString(planUpdateEntity.breakfastNum, map.get("breakfast")); 
					prestPlanUpdate.setString(planUpdateEntity.broadBand, map.get("broadband")); 
					prestPlanUpdate.setString(planUpdateEntity.payType, map.get("payType")); 
					prestPlanUpdate.setString(planUpdateEntity.status, map.get("status")); 
					prestPlanUpdate.setString(planUpdateEntity.planId, map.get("pId")); 
				}
				else{
					prestPlanUpdate.setString(planUpdateEntity.breakfastNum, map.get("breakfast")); 
					prestPlanUpdate.setString(planUpdateEntity.broadBand, map.get("broadband")); 
					prestPlanUpdate.setString(planUpdateEntity.payType, map.get("payType")); 
					prestPlanUpdate.setString(planUpdateEntity.status, map.get("status")); 
					prestPlanUpdate.setString(planUpdateEntity.ctripNumId, map.get("ctripNumId")); 
					prestPlanUpdate.setString(planUpdateEntity.planId, map.get("pId")); 
				}
				prestPlanUpdate.execute();
			}
			else{
				prestPlanInsert.setString(planInsertEntity.planId, map.get("pId"));  
				prestPlanInsert.setString(planInsertEntity.ctripID, idPrefix+map.get("propId")+idPostfix); 
				prestPlanInsert.setString(planInsertEntity.rid, map.get("roomCode"));
				prestPlanInsert.setString(planInsertEntity.planName, map.get("saleItem"));
				prestPlanInsert.setString(planInsertEntity.breakfastNum, map.get("breakfast")); 
				prestPlanInsert.setString(planInsertEntity.broadBand, map.get("broadband")); 
				prestPlanInsert.setString(planInsertEntity.payType, map.get("payType")); 
				prestPlanInsert.setString(planInsertEntity.ctripNumId, map.get("ctripNumId"));
				prestPlanInsert.setString(planInsertEntity.status, map.get("status")); 
				prestPlanInsert.execute();
			}
			rs.close();
	}
	/**
	 * ctripRate table update or insert
	 * @param map
	 * @param breakfast
	 * @throws Exception 
	 */
	public void mysqlRateUpdate(Map<String,String> map) throws Exception{
		String breakfast=null;
		if(map.get("price")==null)
			System.out.println(map);
		JSONArray jsonArray = new JSONArray(map.get("price"));
		for(int i=0;i<jsonArray.length();i++){
			JSONObject jsonObject = jsonArray.getJSONObject(i);
			String price = jsonObject.get("price").toString();
			String breakfastItem = jsonObject.get("breakfast").toString();
			if(breakfast == null){
				breakfast = jsonObject.get("breakfast").toString();
				map.put("breakfast",breakfast);
			}
			BigDecimal rate = new BigDecimal(0);
			if(price.replaceAll("\\d", "").equals("")){
				rate = new BigDecimal(price);
			}
			prestRateUpdate.setBigDecimal(rateUpdateEntity.svcRate, rate);
			prestRateUpdate.setString(rateUpdateEntity.breakfast, breakfastItem); 
			prestRateUpdate.setString(rateUpdateEntity.status, map.get("status"));
			prestRateUpdate.setString(rateUpdateEntity.currency, map.get("currency"));
			prestRateUpdate.setString(rateUpdateEntity.pId, map.get("pId"));
			prestRateUpdate.setString(rateUpdateEntity.date, jsonObject.get("date").toString());
			if(prestRateUpdate.executeUpdate()==0){
				
				prestRateInsert.setString(rateInsertEntity.pId, map.get("pId"));  
				prestRateInsert.setString(rateInsertEntity.date, jsonObject.get("date").toString()); 
				prestRateInsert.setBigDecimal(rateInsertEntity.svcRate, rate);
				prestRateInsert.setString(rateInsertEntity.breakfast, breakfastItem); 
				prestRateInsert.setString(rateInsertEntity.status, map.get("status")); 
				prestRateInsert.setString(rateInsertEntity.currency, map.get("currency")); 
				prestRateInsert.executeUpdate();
			}
		}

	}
	/**
	 * ctripPlan and price import
	 * @param map
	 * @throws InterruptedException
	 */
	public void mysqlInsert(Map<String,String> map) throws InterruptedException{
		try {
			mysqlRateUpdate(map);
			mysqlPlanUpdate(map);
			
			count++;
			if(count%2000==0){
				logger.info("ctripPlan count:"+count);
				long temp = System.currentTimeMillis();
				logger.info("ctripPlan time per 2000:"+(temp-time)/1000);
				time =temp;
				conn.commit();
			}
			
		}catch(Exception e){
			logger.error("Hbase2MysqlPrice mysqlInsert error",e);
			Thread.sleep(5000);
		} 
	}
	/**
	 * price select from hbase
	 */
	public void hbaseSelect(){
		try {
			HTable readTable = new HTable("spider_price");
			Scan s = new Scan();
			s.setStartRow("ctrip.hotel.a".getBytes());
			s.setStopRow("ctrip.hotel.zzzzzzzzzzzz".getBytes());
			s.setCaching(300);
			ResultScanner ss = readTable.getScanner(s);
			for(Result r:ss){
				String hotelUrl = new String(r.getRow());
				Map<String,String> map = new HashMap<String,String>();
				for(KeyValue kv :r.raw()){
					
					subHbaseSelect(map,kv);
				}
				mysqlInsert(map);
			}
		}catch(Exception e){
			logger.error("Hbase2MysqlPrice hbaseSelect error",e);
		}
	}
	
	public void subHbaseSelect(Map<String,String> map, KeyValue kv){
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
		if("propId".equals(column)){
			map.put("propId",value);	
		}
		if("ctripNumId".equals(column)){
			map.put("ctripNumId",value);	
		}
		if("broadband".equals(column)){
			map.put("broadband",value);	
		}
		
		if("saleItem".equals(column)){
			map.put("saleItem",value);	
		}
		if("currency".equals(column)){
			map.put("currency",value);	
		}
	 
	}
}
