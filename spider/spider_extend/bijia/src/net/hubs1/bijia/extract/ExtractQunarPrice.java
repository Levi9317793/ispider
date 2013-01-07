package net.hubs1.bijia.extract;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import net.hubs1.bijia.util.QunaJoin;
import net.hubs1.bijia.util.SaveUrlHelper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

public class ExtractQunarPrice {
	
	private static final Log logger = LogFactory.getLog(ExtractQunarPrice.class);

	public static List<Map<String, String>> extractPrice(String input,String url,String rowKeyPrefix) {
		
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
    	Pattern starPattern = Pattern.compile("\\(\\{.+\\}\\)",Pattern.DOTALL); 
        Matcher starMatcher = starPattern.matcher(input);
        if(starMatcher.find())
        {
        	input = starMatcher.group();
        	input = input.substring(input.indexOf("({")+1,input.lastIndexOf("})")+1);
        	try {
        		JSONObject json = new JSONObject(input);
        		String cssString = json.get("roomtypecss").toString();
        		JSONObject jsonResult = new JSONObject(json.get("result").toString());
				@SuppressWarnings("unchecked")
				Iterator<String> iterator = jsonResult.keys();

				while(iterator.hasNext()){
					JSONArray jsonArray=new JSONArray(jsonResult.get(iterator.next()).toString());

					String saleItem = QunaJoin.join(jsonArray.get(2).toString(), cssString);
										
					String roomName = jsonArray.getString(3);
					String roomCode = ToolCommand.pinyin(roomName, null);
					String status =null;
					Map<String,String> map = new HashMap<String,String>();
					if(jsonArray.getInt(9)>0){
						status ="预订";
					}
					else if(jsonArray.getInt(9)==-2)
						status ="部分日期可订";
					else 	
						status ="订完";
					if(jsonArray.getInt(14)==1){
						String payType ="预付";
						map.put("payType", payType);
					}
					String provider = jsonArray.getString(5);
					String dateString = jsonArray.getString(4);
					int dateStartIndex = dateString.indexOf("required1=");
					int dateEndIndex = dateString.indexOf("&required2=");
					if(dateEndIndex > dateStartIndex && dateStartIndex>0){
						dateString = dateString.substring(dateStartIndex+10,dateEndIndex);
						SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd");
						Date date=simpleDateFormat.parse(dateString);
						map.put("date", simpleDateFormat.format(date));
					}
					map.put("provider", provider);
					map.put("roomPrice", jsonArray.getString(0));
					map.put("saleItem", saleItem);
					map.put("roomCode",roomCode);
					map.put("source_url", url);
					String pdcID = SaveUrlHelper.rowkey2url(url, rowKeyPrefix, "http://hotel.qunar.com");
					map.put("pdcID", "qunar:"+pdcID);
					map.put("reservation", status);
					map.put("planCode", Until.encrypt(url+"#"+provider+"#"+ToolCommand.pinyin(saleItem, null)));
					list.add(map);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("extra price error:"+e);
				e.printStackTrace();
			}
        }
        return list;
    }
	
	public static void saveBijia(){
    	int count =0;
    	try {
	    	HTable table = new HTable("download_page");
			Scan s = new Scan();
			s.setStartRow("qunar.bijia.a".getBytes());
			s.setStopRow("qunar.bijia.zzzz".getBytes());
			s.setCaching(100);
			ResultScanner ss = table.getScanner(s);
			
			HTable writeTable = new HTable("spider_price");
			writeTable.setAutoFlush(false);
			writeTable.setWriteBufferSize(2097152);
			
			for(Result r:ss){
				count++;
				for (KeyValue kv : r.list()) {
					String family = Bytes.toString(kv.getFamily());
					String qualifier = Bytes.toString(kv.getQualifier());
					if ("base".equals(family)
							&&( "page".equals(qualifier)
							||"page1".equals(qualifier)
							||"page2".equals(qualifier) )) {	
						String hotelUrl = new String(r.getRow());
						String result = new String(kv.getValue());
						
			    		List<Map<String, String>> list =  ExtractQunarPrice.extractPrice(result,hotelUrl,"qunar.bijia");
			    		
			    		//System.out.println(list.size());
			    		for(Map<String, String> map :list){ 
			    			String priceRowKey = map.get("source_url") + "."
			    					+ map.get("planCode")+"."+qualifier;
			    			//System.out.println(priceRowKey);
			    			Put put = new Put(priceRowKey.getBytes());
			    			put.setWriteToWAL(false);
			    			Iterator<String> it = map.keySet().iterator();
			    			while (it.hasNext()) {
			    				String key = (String) it.next();
			    				put.add("base".getBytes(), key.getBytes(),
			    						map.get(key).getBytes());
			    			}
			    			writeTable.put(put);
			    		}

			    	}
			    }
				if(count%200 == 0)
					System.out.println(count);
				if(count == 100)break;
	    	}
			writeTable.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	    System.out.println("Hotel total count:"+count);
  
	}
	
	public static void saveAll(){
    	int count =0;
    	try {
	    	HTable table = new HTable("download_page");
			Scan s = new Scan();
			s.setStartRow("qunar.price.a".getBytes());
			s.setStopRow("qunar.price.zzzz".getBytes());
			s.setCaching(300);
			ResultScanner ss = table.getScanner(s);
			
			HTable writeTable = new HTable("spider_price");
			writeTable.setAutoFlush(false);
			writeTable.setWriteBufferSize(2097152);

			for(Result r:ss){
				count++;
				for (KeyValue kv : r.list()) {
					String family = Bytes.toString(kv.getFamily());
					String qualifier = Bytes.toString(kv.getQualifier());
					if ("base".equals(family)
							&& "page".equals(qualifier)) {	
						String hotelUrl = new String(r.getRow());
						String result = new String(kv.getValue());
			    		List<Map<String, String>> list =  ExtractQunarPrice.extractPrice(result,hotelUrl,"qunar.price");
			    		System.out.println(list);
			    		for(Map<String, String> map :list){ 
			    			String priceRowKey = map.get("source_url") + "."
			    					+ map.get("planCode");
			    			//System.out.println(priceRowKey);
			    			Put put = new Put(priceRowKey.getBytes());
			    			put.setWriteToWAL(false);
			    			Iterator<String> it = map.keySet().iterator();
			    			while (it.hasNext()) {
			    				String key = (String) it.next();
			    				put.add("base".getBytes(), key.getBytes(),
			    						map.get(key).getBytes());
			    			}
			    			//writeTable.put(put);
			    		}
			    	}
			    }
				if(count%1000 == 0)
					System.out.println(count);
				break;
	    	}
			writeTable.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	    System.out.println("Hotel total count:"+count);
  
	}
    public static void main(String[] args) {
    	//saveBijia();
    	saveAll();
    }
    
}
