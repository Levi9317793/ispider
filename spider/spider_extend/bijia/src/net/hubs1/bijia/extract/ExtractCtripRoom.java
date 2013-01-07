package net.hubs1.bijia.extract;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class ExtractCtripRoom {
	private static final Log logger = LogFactory.getLog(ExtractCtripRoom.class);

    public static List extraRoom(String input,String url ){
    	List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        Document doc = Jsoup.parse(input);
        int urlStartIndex = url.lastIndexOf("/");
        int urlEndIndex = url.lastIndexOf(".");
		//房型
		Element tableRoomElement = doc.select("table.hotel_datelist").first();
		if(tableRoomElement!=null){
			tableRoomElement = tableRoomElement.select("tbody").first();
			if(tableRoomElement!=null){
				int i=1;
				for(Element roomElement:tableRoomElement.children()){
					if(roomElement.attr("class").equals("t")){
						Map<String,String> roomMap = new HashMap<String,String>();
						i++;
						roomMap.put("source", "ctrip");
						roomMap.put("source_url", url);
						roomMap.put("pdcid", "ctrip:"+url);
						//酒店Id
						if(urlStartIndex>=0 && urlEndIndex>urlStartIndex){
							roomMap.put("propid", url.substring(urlStartIndex+1,urlEndIndex));
							//真正抽取
							subExtraRoom(url,roomElement,roomMap);
							//含有两种床型
							if(roomMap.containsKey("bedItem1")){
								List<Map<String, String>> mapList = splitMap(roomMap,url);
								for(Map<String,String> tempMap:mapList){
									list.add(tempMap);
								}
							}
							else{
								list.add(roomMap);
							}
						}
					} 
				}
			}
		}
        return list;
    }
    /**
     * Real extract room
     * @param url
     * @param roomElement
     * @param roomMap
     */
	public static void subExtraRoom(String url ,Element roomElement,Map<String,String> roomMap){
		//房间名称
        Element nameRoomElement = roomElement.select("a.hotel_room_name").first();
		if(nameRoomElement!=null){
			String name = nameRoomElement.attr("title");
			roomMap.put("name_zh",name);
			String roomCode = ToolCommand.pinyin(name, null);
			roomMap.put("room_code",roomCode);
		}
		//床型 class=unexpanded
		Element roomUnElement = roomElement.nextElementSibling();
		if(roomUnElement!=null && roomUnElement.attr("class").contains("unexpanded")){
			Element bedTdElement = roomUnElement.select("td.hotel_room").first();
			Element bedTdNextElement = bedTdElement.nextElementSibling();
			if(bedTdNextElement!=null 
					&& (bedTdNextElement.text().contains("床")||bedTdNextElement.text().contains("/"))){
				String bedType = bedTdNextElement.text().trim();
				
				roomMap.put("bed_type", bedType);

				
				if(bedType.contains("/")){
					String[] beds = bedType.split("/");
					roomMap.put("bedItem1",beds[0]);
					roomMap.put("bedItem2",beds[1]);
				}
				else{
					roomMap.put("room_code",roomMap.get("room_code")+"_"+ToolCommand.pinyin(bedType, null));
				}
			}
			
		}
		//床型 class=t
		else{
			Element bedTdElement = roomElement.select("td.hotel_room").first();
			Element bedTdNextElement = bedTdElement.nextElementSibling();
			if(bedTdNextElement!=null
					&& (bedTdNextElement.text().contains("床")||bedTdNextElement.text().contains("/"))){
				String bedType = bedTdNextElement.text().trim();
				
				roomMap.put("bed_type", bedType);

				if(bedTdNextElement.text().contains("/")){
					String[] beds = bedTdNextElement.text().trim().split("/");
					roomMap.put("bedItem1",beds[0]);
					roomMap.put("bedItem2",beds[1]);
				}
				else{
					roomMap.put("room_code",roomMap.get("room_code")+"_"+ToolCommand.pinyin(bedType, null));
				}
			}
			
		}
		//房间描述
		while(roomElement!=null){
			roomElement = roomElement.nextElementSibling();
			if(roomElement.attr("class").equals("clicked hidden")){
				Element ulElement = roomElement.select("ul.searchresult_caplist").first();
				for(Element liElement : ulElement.children()){
					if(liElement.attr("title").trim().startsWith("建筑面积")){
						String area = liElement.text();
						int areaIndex = area.indexOf("：");
						if(areaIndex!=-1){
							roomMap.put("area", area.substring(areaIndex+1));
						}
					}
					else if(liElement.attr("title").trim().startsWith("楼层")){
						String floor = liElement.text();
						int floorIndex = floor.indexOf("：");
						if(floorIndex!=-1){
							roomMap.put("floor", floor.substring(floorIndex+1));
						}
					}
					else if(liElement.attr("title").trim().startsWith("床宽")){
						String bed = liElement.text();
						int bedIndex = bed.indexOf("：");
						if(bedIndex!=-1){
							roomMap.put("bed_desc", bed.substring(bedIndex+1));
						}
					}
					else if(liElement.attr("title").trim().contains("可入住人数"))
						roomMap.put("maxoccupancy", liElement.text().replaceAll("\\D", ""));
					else if(liElement.attr("title").trim().startsWith("不可加床"))
						roomMap.put("is_addbed", "0");
					else if(liElement.attr("title").trim().startsWith("可加床"))
						roomMap.put("is_addbed", "1");
					else if(liElement.attr("title").trim().startsWith("无烟房"))
						roomMap.put("is_nosmoke", "1");
					else if(liElement.attr("title").trim().startsWith("无窗"))
						roomMap.put("is_window", "0");
					else if(liElement.attr("title").trim().startsWith("宽带")){
						if(liElement.attr("title").contains("免费")){
							roomMap.put("is_broadband_network", "1");
						}
						else
							roomMap.put("is_broadband_network", "0");
						
					}
				}
				//房间图片
				Element imgRoomElement = roomElement.select("div.searchresult_caption").first();
				JSONArray jsonRoomArray=new JSONArray();
				JSONObject jsonRoom = null;
				String imgRoomUrl = null;
				try{
					if(imgRoomElement!=null){
						for(Element aRoomElement : imgRoomElement.children()){
							imgRoomUrl = aRoomElement.attr("style");
							int roomStartIndex = imgRoomUrl.lastIndexOf("(");
							int roomEndIndex = imgRoomUrl.lastIndexOf(")");
							if(roomStartIndex>=0 && roomEndIndex>roomStartIndex){
								jsonRoom = new JSONObject();
								imgRoomUrl = imgRoomUrl.substring(roomStartIndex+1, roomEndIndex);
								jsonRoom.put("url", imgRoomUrl);
								jsonRoom.put("type", imgRoomUrl.substring(imgRoomUrl.lastIndexOf(".")+1,imgRoomUrl.length()));
								jsonRoomArray.put(jsonRoom);
								}
							}
						}
					roomMap.put("images", jsonRoomArray.toString());
					
				}
				catch(Exception e){
					e.printStackTrace();
				}
				break;
				
			}
		}
	}
	/**
	 * bedType split
	 * @param roomMap
	 * @param url
	 * @return
	 */
    public static List<Map<String, String>> splitMap(Map<String,String> roomMap,String url){
    	List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		Iterator<String> it = roomMap.keySet().iterator();
		Map<String,String> copyMap1 = new HashMap<String,String>();
		Map<String,String> copyMap2 = new HashMap<String,String>();
        while(it.hasNext()) {
            String key = (String) it.next();
            if(key.equals("bedItem1")||key.equals("bedItem2")
            		|| key.equals("room_code")){
            	
            }
            else if(key.equals("bed_type")){
            	String bedType = roomMap.get("bedItem1");
            	copyMap1.put("bed_type",bedType);
            	copyMap1.put("room_code",roomMap.get("room_code")+"_"+ToolCommand.pinyin(bedType, null));
            	bedType = roomMap.get("bedItem2");
            	copyMap2.put("bed_type",bedType);
            	copyMap2.put("room_code",roomMap.get("room_code")+"_"+ToolCommand.pinyin(bedType, null));
            	
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
    
    public static String reverseUrl(String url ,String domain ,String prefix){
    	url = url.replace(".", "/");
    	url = url.substring(prefix.length());
    	url = domain + url+".html";
    	return url;
    }
    public static boolean validate (Map<String, String> map ,String url){

    		if(map.containsKey("source_id")&&
    				map.containsKey("country")&&
    				map.containsKey("city")&&
    				map.containsKey("name")&&
    				map.containsKey("address")){
    			return true;
    		}
    		else{
    			logger.error("=============="+url);
    			return false;
    		}
    	
    }
}
