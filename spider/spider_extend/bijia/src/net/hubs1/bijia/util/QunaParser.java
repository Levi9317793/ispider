package net.hubs1.bijia.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class QunaParser {
	private static final Log logger = LogFactory.getLog(QunaParser.class);
	public static List<String> urlParser(String result){
		List<String> list=new ArrayList<String>();
		try {
			JSONObject json = new JSONObject(result);
			JSONArray jsonArray=(JSONArray)json.get("hotels");
			String url=null;
			for(int i=0;i<jsonArray.length();i++){
				String id = ((JSONObject)jsonArray.get(i)).getString("id");
				url = generateUrl(id);
				list.add(url);
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			logger.error("url parser error",e);
		}
		
		return list;
	}
	public static String generateUrl(String id){
		String url ="http://hotel.qunar.com/city/";
		int index = id.lastIndexOf("_");
		if(index>0){
			String city=id.substring(0,index);
			String cityId=id.substring(index+1,id.length());
			url=url+city+"/dt-"+cityId+"/";
		}
		else
			url = "error";
		return url;
	}
	public static Map<String,String> fileRead(String filePath){
		Map<String,String> map = new HashMap<String,String>();
		try {
			InputStreamReader reader = new InputStreamReader(QunaParser.class.getResourceAsStream(filePath));
			BufferedReader br = new BufferedReader(reader);
			String s = null;  
	        while((s = br.readLine()) != null && s.length()>0) { 

	               map.put(s.substring(0, s.indexOf(":")), s.substring(s.indexOf(":")+1));
	         }
		}
        catch (Exception e) {
			// TODO Auto-generated catch block
        	logger.error("fiel read error",e);
		}
		return map;
	}
	public static List<String> cityRead(String cityPath){
		Map<String,String> map  = fileRead(cityPath);
		List<String> cityList = new ArrayList<String>();
		Iterator<String> it = map.keySet().iterator();
		while (it.hasNext()) {
			String key = it.next();
			String[] cityArray = map.get(key).split("、");
			for(String temp :cityArray){
				cityList.add(temp);
			}
		}
		return cityList;
	}
	public static void main(String[] args) {
		System.out.println(cityRead("province.txt").size());
	}
}
