package net.hubs1.bijia.extract;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class ExtractCtripPrice { 
	
	public static List<Map<String, String>> extractPrice(String input,String url) throws Exception{
    	Document htmlDoc = Jsoup.parse(input);
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		String name ="";
		String dateString ="";
		String medalString ="";
        int urlEndIndex = url.lastIndexOf(".");
        Element medalElement = htmlDoc.select("h3#ctl00_MainContentPlaceHolder_h3id").first();
		if(medalElement!=null){
			medalString = medalElement.attr("class");
			if(medalString.contains("gold"))
				medalString ="1";
			else if(medalString.contains("silver"))
				medalString ="2";
			else if(medalString.contains("blue"))
				medalString ="5";
			else
				medalString ="0";
		}
		Element dateElement = htmlDoc.select("input#change_txtCheckIn").first();
		if(dateElement!=null){
			dateString = dateElement.attr("value");
			dateString = dateString.replaceAll("[^-0-9]", "");
		}
		for(Element element :htmlDoc.select("table.hotel_datelist tr")){
			
			Map<String,String> map = new HashMap<String,String>();
			map.put("medal", medalString);
			map.put("source_url", url);
			map.put("propId", url.substring(urlEndIndex+1));
			if(element.attr("class").equals("t")){
				Element nameElement =element.select("a.hotel_room_name").first();
				if(nameElement!=null){
					name = nameElement.attr("title");
					String roomCode = ToolCommand.pinyin(name, null);
					
					map.put("roomName", name);
					map.put("roomCode",roomCode);
					map.put("planCode", Until.encrypt(url+"#"+roomCode));
					// real extract
					subExtractPrice(element, map, list, dateString, url);
					
				}
			}
			else if(element.attr("class").equals("n unexpanded")){
				Element saleElement = element.select("span.hotel_room_style").first();
				if(saleElement!=null){
					String saleItem = saleElement.text().replaceAll("[()]", "");					
					String roomCode = ToolCommand.pinyin(name, null);
					map.put("saleItem", saleItem);
					map.put("roomName", name);
					map.put("roomCode",roomCode);
					map.put("planCode", Until.encrypt(url+"#"+roomCode+"#"+saleItem));
					
					subExtractPrice(element, map, list, dateString, url);
				}
			}
		}
		return list;
	}
	/**
	 * split bed type example "da/shuang"
	 * @param element
	 * @param map
	 * @throws Exception 
	 */
	public static void subExtractPrice(Element element ,Map<String, String> map,
			List<Map<String, String>> list,String dateString,String url) throws Exception{
		
		Element payElement = element.select("span.icon_prepay").first();
		if(payElement!=null){
			map.put("payType","预付");
		}
		Element reserveElement = element.select("input.btn_buy").first();
		if(reserveElement!=null){
			map.put("reservation",reserveElement.attr("value"));
			String ctripNumId = reserveElement.attr("onclick");
			String[] array = ctripNumId.split(",");
			if(array.length>2){
				ctripNumId = ctripNumId.split(",")[1];
				map.put("ctripNumId",ctripNumId.replaceAll("\\D", ""));
			}
		}
		else{
			reserveElement = element.select("input.hotel_btn_none").first();
			if(reserveElement!=null){
				map.put("reservation",reserveElement.attr("value"));
			}
		}
		Element bedTdElement = element.select("td.hotel_room").first();
		Element bedTdNextElement = bedTdElement.nextElementSibling();
		if(bedTdNextElement!=null ){
			String bedType = bedTdNextElement.text().trim();
			if(bedType.contains("/")){
				String[] beds = bedType.split("/");
				map.put("bedItem1",beds[0]);
				map.put("bedItem2",beds[1]);
			}
			else{
				String roomCode = map.get("roomCode")+"_"+ToolCommand.pinyin(bedType, null);
				map.put("roomCode",roomCode);
				String planCode = null;
				if(map.containsKey("saleItem"))
					planCode = Until.encrypt(map.get("source_url")+"#"+roomCode+"#"+map.get("saleItem"));
				else
					planCode = Until.encrypt(map.get("source_url")+"#"+roomCode);
				map.put("planCode", planCode);
			}
			Element nextElement = bedTdNextElement.nextElementSibling();
			if(nextElement!=null ){
				if(nextElement.text().contains("费")){
					map.put("broadband",nextElement.text());
				}
				else{
					nextElement = nextElement.nextElementSibling();
					if(nextElement!=null && nextElement.text().contains("费")){
						map.put("broadband",nextElement.text());
					}
				}
			}
		}
		Element priceElement = element.select("span.base_txtdiv").first();
		if(priceElement!=null){
			//map.put("price",priceElement.attr("mod_jmpinfo_content"));
			JSONArray jsonArray = priceSplit(priceElement.attr("data-params"),dateString);
			map.put("roomPrice",jsonArray.toString());
			
			Element currencyElement = priceElement.select("dfn").first();
			if(currencyElement!=null){
				map.put("currency", currencyElement.text());
					
			}
			if(map.containsKey("bedItem1")){
				List<Map<String, String>> mapList = splitMap(map,url);
				for(Map<String,String> tempMap:mapList){
					list.add(tempMap);
				}
			}
			else{
				list.add(map);
			}
		}	
	}
	/**
	 * split map example "da/shuang"
	 * @param roomMap
	 * @param url
	 * @return
	 * @throws Exception 
	 */
    public static List<Map<String, String>> splitMap(Map<String,String> roomMap,String url) throws Exception{
    	List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Iterator<String> it = roomMap.keySet().iterator();
		Map<String,String> copyMap1 = new HashMap<String,String>();
		Map<String,String> copyMap2 = new HashMap<String,String>();
        while(it.hasNext()) {
            String key = (String) it.next();
            if( key.equals("planCode")||key.equals("bedItem1")||key.equals("bedItem2")){
            	
            }
            else if(key.equals("roomCode")){
            	String bedType = roomMap.get("bedItem1");
            	
            	String roomCode = roomMap.get("roomCode")+"_"+ToolCommand.pinyin(bedType, null);
            	String planCode = null;
    			if(roomMap.containsKey("saleItem"))
    				planCode = Until.encrypt(roomMap.get("source_url")+"#"+roomCode+"#"+roomMap.get("saleItem"));
    			else
    				planCode = Until.encrypt(roomMap.get("source_url")+"#"+roomCode);
    			copyMap1.put("roomCode",roomCode);
    			copyMap1.put("planCode",planCode);
    			
    			bedType = roomMap.get("bedItem2");
    			
    			roomCode = roomMap.get("roomCode")+"_"+ToolCommand.pinyin(bedType, null);
    			if(roomMap.containsKey("saleItem"))
    				planCode = Until.encrypt(roomMap.get("source_url")+"#"+roomCode+"#"+roomMap.get("saleItem"));
    			else
    				planCode = Until.encrypt(roomMap.get("source_url")+"#"+roomCode);
    			copyMap2.put("roomCode",roomCode);
    			copyMap2.put("planCode",planCode);
            	
            }
            else {
            	copyMap1.put(key, roomMap.get(key));
            	copyMap2.put(key, roomMap.get(key));
            }
        }
        list.add(copyMap1);
        list.add(copyMap2);
        return list;
    }
    /**
     * split prices to jsonArray 
     * @param price
     * @param dateString
     * @return
     */
	public static JSONArray priceSplit(String price,String dateString){
		int  priceStart =price.indexOf("'info':");
		int priceEnd = price.indexOf(",'reduce'");
		JSONArray jsonArray=new JSONArray();
		if(priceStart > 0 && priceEnd > priceStart){
			price = price.substring(priceStart,priceEnd);
			Calendar calendar=null;
			try{
				SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd");
				Date date=simpleDateFormat.parse(dateString);
				calendar = new GregorianCalendar();
				calendar.setTime(date);
				calendar.add(Calendar.DATE,-1);
				JSONObject json =new JSONObject("{"+price+"}");
				//数据最多不超过1个月
				for(int dateGroup=1;dateGroup<5;dateGroup++){
					if(json.getJSONObject("info").has(Integer.toString(dateGroup))){					
						JSONArray jsonPriceArray = json.getJSONObject("info").getJSONArray(Integer.toString(dateGroup));
						for(int i=0;i<jsonPriceArray.length();i++){
							calendar.add(Calendar.DATE,1);
			    			String tempDate = simpleDateFormat.format(calendar.getTime());
			    			
							JSONObject tempJson =jsonPriceArray.getJSONObject(i);
							String dataString = tempJson.get("data").toString().replaceAll("[\"\\[\\]]","");
							String[] stringArray = dataString.split(",");
							if(stringArray.length>1 && stringArray[0].length()>0){
								tempJson = new JSONObject();
								tempJson.put("date", tempDate);
								tempJson.put("price", stringArray[0]);
								tempJson.put("breakfast", stringArray[1]);
				    			jsonArray.put(tempJson);
							}
						}
					}
				}
	
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		return jsonArray;
	}
	public static void main(String[] args){
	}
}
