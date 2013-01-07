package net.hubs1.bijia.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.json.JSONException;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class QunaJoin {
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static Map<String, String> cssParser(String cssString) throws Exception{
		//String input = ".x span{width:26px;left:0px;z-index:2}span.x28{width:52px;left:0px;z-index:2}span.x16{width:13px;left:39px;z-index:1}span.x14{width:13px;left:52px;z-index:2}span.x7{width:26px;left:39px;z-index:1}span.x2{width:26px;left:26px;z-index:2}span.x9{width:13px;left:0px;z-index:2}span.x1{width:39px;left:0px;z-index:2}span.x3{width:13px;left:26px;z-index:2}span.x27{width:13px;left:65px;z-index:2}span.x15{width:26px;left:0px;z-index:1}span.x6{width:13px;left:26px;z-index:1}span.x24{width:26px;left:26px;z-index:1}span.x17{width:39px;left:26px;z-index:1}span.x13{width:13px;left:0px;z-index:1}span.x18{width:26px;left:52px;z-index:1}span.x12{width:13px;left:13px;z-index:2}span.x26{width:13px;left:78px;z-index:1}span.x0{width:26px;left:39px;z-index:2}span.x31{width:52px;left:0px;z-index:1}span.x23{width:39px;left:13px;z-index:2}span.x19{width:13px;left:39px;z-index:2}span.x21{width:13px;left:52px;z-index:1}span.x8{width:26px;left:13px;z-index:2}";
		String[] stringArray = cssString.split("span.x");
		Map<String, String> map= new HashMap<String, String>();
		for(String temp : stringArray){
			int index = temp.indexOf("{");
			if(index>0){
				String num = temp.substring(0,index);
				if(num.contains("span"))
					num="span";
				String jsonString = temp.substring(index);
				JSONObject json = new JSONObject(jsonString);
				String width = json.get("width").toString().replaceAll("\\D", "");
				String left = json.get("left").toString().replaceAll("\\D", "");
				String zIndex = json.get("z-index").toString().replaceAll("\\D", "");
				map.put(num, left+","+width+","+zIndex);
			}
		}
			
		//System.out.println(map);
		return map;
	}
	/**
	 * 
	 * @param input
	 * @param cssString
	 * @return
	 */
	public static String join(String input,String cssString)throws Exception{
		String result = "";
		long start = System.currentTimeMillis();
		Document doc = Jsoup.parse(input);
		
		Map<String, String> cssMap = cssParser(cssString);
		Map<Integer, Map<String, String>> treeMap = new TreeMap<Integer, Map<String, String>>();
		for(Element element:doc.select("span")){
			String key = element.attr("class");
			if(key.startsWith("x")){
				key = key.replaceAll("\\D", "");
				String text = element.text().trim();
				//System.out.println(element.outerHtml());
				String[] value = null;
				if(!cssMap.containsKey(key))
					value = cssMap.get("span").split(",");
				else
					value = cssMap.get(key).split(",");
				int left = (Integer.parseInt(value[0]))/13;
				int width = (Integer.parseInt(value[1]))/13;
		        Set<Integer> keySet = treeMap.keySet();
		        Iterator<Integer> it = keySet.iterator();
		        boolean findZIndexflag=false;
		        for (; it.hasNext();) {
		        	Map<String, String> map = treeMap.get(it.next());
		        	// the same zIndex  
		            if(map.get("zIndex").equals(value[2])){
		            	findZIndexflag=true;
		            	StringBuilder sb = new StringBuilder();
						for(int i=0;i<left;i++){
							sb.append("0");
						}
						sb.append(text.substring(0,width));
						String mapCharArray= map.get("text");
						String charArray= sb.toString();
						sb = new StringBuilder();
						for(int i=0,j=0;i<mapCharArray.length()&&j<charArray.length();i++,j++){
							if(charArray.charAt(i)!='0'){
								sb.append(charArray.charAt(i));
							}
							else 
								sb.append(mapCharArray.charAt(i));
								
						}
						if(mapCharArray.length() > charArray.length()){
							sb.append(mapCharArray.substring(charArray.length()));
						}
						else
							sb.append(charArray.substring(mapCharArray.length()));
						map.put("text", sb.toString());
						break;
		            }
		            	
		        }
		        if(!findZIndexflag){
		        	Map<String, String> map = new HashMap<String, String>();
					map.put("zIndex", value[2]);
					StringBuilder sb = new StringBuilder();
					for(int i=0;i<left;i++){
						sb.append("0");
					}
					sb.append(text.substring(0,width));
					map.put("text", sb.toString());		
					treeMap.put(Integer.parseInt(value[2]), map);
		        }
			}
			if(key.equals("enc1")){
				result = element.text().trim();
			}
		}
		String tempString = new String();
		Set<Integer> keySet = treeMap.keySet();
        Iterator<Integer> it = keySet.iterator();
        for (; it.hasNext();) {
        	Map<String, String> map = treeMap.get(it.next());
        	String charArray = map.get("text");
        	StringBuilder sb = new StringBuilder();
        	for(int i=0,j=0;i<tempString.length()&&j<charArray.length();i++,j++){
				if(charArray.charAt(i)!='0'){
					sb.append(charArray.charAt(i));
				}
				else 
					sb.append(tempString.charAt(i));
        	}
			if(tempString.length() > charArray.length()){
				sb.append(tempString.substring(charArray.length()));
			}
			else
				sb.append(charArray.substring(tempString.length()));
						
			tempString = sb.toString();
        	
        }
        result = tempString + result;
		//System.out.println(result);
		return result;
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//Document doc = Jsoup.parse("<span class='x4'>准</span>");
		long start = System.currentTimeMillis();
		String cssString = ".x span{width:13px;left:0px;z-index:2}span.x8{width:26px;left:0px;z-index:1}span.x0{width:13px;left:26px;z-index:1}span.x4{width:13px;left:13px;z-index:1}span.x7{width:13px;left:0px;z-index:1}span.x6{width:13px;left:13px;z-index:2}span.x1{width:26px;left:0px;z-index:2}span.x3{width:13px;left:26px;z-index:2}";
		String input = "<em class='sort x' style='display:inline-block;width:39px'><span class='x4'>准</span><span class='x7'>标</span><span class='x3'>间</span></em><span class='enc1' style='display: block; text-indent: 39px;'>-含双早</span>";

		join(input,cssString);
		System.out.println((System.currentTimeMillis()-start));
//		long start = System.currentTimeMillis();
//		join(input,cssString);
//		System.out.println((System.currentTimeMillis()-start));
	}

}
